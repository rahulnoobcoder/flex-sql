#include <list>
#include <list>
#include "engine/executor.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <atomic>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "parser/parser.h"
#include "storage/table.h"
#include "util/core_utils.h"

namespace flexql::engine {

template <typename K, typename V>
class LRUCache {
public:
    LRUCache(size_t capacity) : capacity_(capacity), is_empty_(true) {}

    bool empty() const { return is_empty_.load(std::memory_order_relaxed); }

    bool get(const K& key, V& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) return false;
        list_.splice(list_.begin(), list_, it->second);
        value = it->second->second;
        return true;
    }

    void put(const K& key, const V& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            list_.splice(list_.begin(), list_, it->second);
            it->second->second = value;
            return;
        }
        if (map_.size() >= capacity_) {
            auto last = list_.end();
            last--;
            map_.erase(last->first);
            list_.pop_back();
        }
        list_.emplace_front(key, value);
        map_[key] = list_.begin();
        is_empty_.store(false, std::memory_order_relaxed);
    }

    void clear() {
        if (empty()) return;
        std::lock_guard<std::mutex> lock(mutex_);
        if (map_.empty()) return;
        map_.clear();
        list_.clear();
        is_empty_.store(true, std::memory_order_relaxed);
    }

private:
    size_t capacity_;
    std::atomic<bool> is_empty_;
    std::list<std::pair<K, V>> list_;
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> map_;
    std::mutex mutex_;
};

namespace {

constexpr size_t kMaxNumericLiteralLen = 4096;
constexpr uint32_t kWalMagic = 0x4C415746;  // FWAL
constexpr uint16_t kWalVersion = 1;
constexpr uint16_t kWalOpInsertRows = 1;
constexpr uint16_t kWalOpTruncate = 2;
constexpr uint16_t kWalOpInsertSqlValues = 3;
constexpr size_t kWalFlushThresholdBytes = 4U * 1024U * 1024U;

uint32_t wal_checksum_xor32(const char* data, size_t len, uint16_t op, uint32_t payload_size) {
    uint32_t checksum = 0x9E3779B9U ^ static_cast<uint32_t>(op) ^ payload_size;
    size_t i = 0;
    while (i + sizeof(uint32_t) <= len) {
        uint32_t chunk = 0;
        std::memcpy(&chunk, data + i, sizeof(uint32_t));
        checksum ^= chunk;
        checksum = (checksum << 5U) | (checksum >> 27U);
        i += sizeof(uint32_t);
    }
    uint32_t tail = 0;
    size_t shift = 0;
    while (i < len) {
        tail |= (static_cast<uint32_t>(static_cast<unsigned char>(data[i])) << shift);
        shift += 8;
        ++i;
    }
    checksum ^= tail;
    return checksum;
}

struct ActiveDatabase {
    std::string name;
    std::unordered_map<std::string, std::shared_ptr<storage::Table>> tables;
    mutable std::shared_mutex mutex;
};

struct ResolvedColumn {
    int table_id = 0;
    size_t column_index = 0;
};

bool parse_int64(std::string_view s, int64_t& out) {
    if (s.empty() || s.size() > kMaxNumericLiteralLen) {
        return false;
    }
    auto begin = s.data();
    auto end = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(begin, end, out);
    return ec == std::errc() && ptr == end;
}

bool parse_double(std::string_view s, double& out) {
    if (s.empty() || s.size() > kMaxNumericLiteralLen) {
        return false;
    }
    auto begin = s.data();
    auto end = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(begin, end, out);
    return ec == std::errc() && ptr == end && std::isfinite(out);
}

bool compare_numeric(double lhs, parser::Operator op, double rhs) {
    switch (op) {
        case parser::Operator::Eq:
            return std::fabs(lhs - rhs) < 1e-9;
        case parser::Operator::Gt:
            return lhs > rhs;
        case parser::Operator::Lt:
            return lhs < rhs;
        case parser::Operator::Ge:
            return lhs >= rhs;
        case parser::Operator::Le:
            return lhs <= rhs;
    }
    return false;
}

bool compare_string(const std::string& lhs, parser::Operator op, std::string_view rhs) {
    switch (op) {
        case parser::Operator::Eq:
            return lhs == rhs;
        case parser::Operator::Gt:
            return lhs > rhs;
        case parser::Operator::Lt:
            return lhs < rhs;
        case parser::Operator::Ge:
            return lhs >= rhs;
        case parser::Operator::Le:
            return lhs <= rhs;
    }
    return false;
}

std::string column_type_to_string(parser::ColumnType type) {
    switch (type) {
        case parser::ColumnType::Int:
            return "INT";
        case parser::ColumnType::Decimal:
            return "DECIMAL";
        case parser::ColumnType::Varchar:
            return "VARCHAR";
        case parser::ColumnType::Datetime:
            return "DATETIME";
    }
    return "UNKNOWN";
}

}  // namespace

struct Executor::Impl {
    parser::SQLParser parser;
    ActiveDatabase active_db;
    std::string data_root_dir = "data";
    std::string default_database_name = "TEST";
    LRUCache<std::string, ExecutionResult> cache{100};
    std::unordered_map<std::string, std::vector<char>> wal_buffers;
    mutable std::mutex wal_mutex;

    Impl() {
        std::error_code ec;
        std::filesystem::create_directories(data_root_dir, ec);
        activate_default_database_if_needed();
    }

    ~Impl() {
        flush_all_tables();
    }

    bool has_active_database() const {
        return !active_db.name.empty();
    }

    bool activate_database(const std::string& db_name, std::string& error) {
        const std::string db_dir = database_dir_path(db_name);
        std::error_code ec;
        if (!std::filesystem::exists(db_dir, ec)) {
            error = "database not found";
            return false;
        }

        if (has_active_database() && active_db.name == db_name) {
            return true;
        }

        flush_all_tables();
        {
            std::unique_lock<std::shared_mutex> lock(active_db.mutex);
            active_db.name = db_name;
        }
        load_tables_from_disk(db_name);
        return true;
    }

    bool activate_default_database_if_needed() {
        if (has_active_database()) {
            return true;
        }
        std::error_code ec;
        std::filesystem::create_directories(database_dir_path(default_database_name), ec);
        if (ec) {
            return false;
        }
        std::string error;
        return activate_database(default_database_name, error);
    }

    std::string database_dir_path(const std::string& db_name) const {
        return data_root_dir + "/" + db_name;
    }

    std::string table_file_path(const std::string& table_name) const {
        return database_dir_path(active_db.name) + "/" + table_name + ".tbl";
    }

    std::string schema_file_path(const std::string& table_name) const {
        return database_dir_path(active_db.name) + "/" + table_name + ".schema";
    }

    std::string wal_file_path(const std::string& table_name) const {
        return database_dir_path(active_db.name) + "/" + table_name + ".wal";
    }

    bool ensure_database_selected(ExecutionResult& out) const {
        if (has_active_database()) {
            return true;
        }
        auto* self = const_cast<Impl*>(this);
        if (self->activate_default_database_if_needed()) {
            return true;
        }
        out = {false, "no database selected (failed to activate default TEST db)", {}};
        return false;
    }

    bool persist_table(const std::shared_ptr<storage::Table>& table, std::string& error) {
        return table->persist_to_file(table_file_path(table->name()), error);
    }

    bool persist_schema(const std::shared_ptr<storage::Table>& table, std::string& error) {
        std::ofstream out(schema_file_path(table->name()), std::ios::trunc);
        if (!out) {
            error = "unable to open schema file for write";
            return false;
        }

        out << "TABLE " << table->name() << "\n";
        for (const auto& col : table->columns()) {
            out << col.name << ' ' << column_type_to_string(col.type) << "\n";
        }

        if (!out) {
            error = "failed writing schema file";
            return false;
        }

        return true;
    }

    bool flush_wal_buffer(const std::string& table_name, std::string& error) {
        std::vector<char> chunk;
        {
            std::lock_guard<std::mutex> lock(wal_mutex);
            auto it = wal_buffers.find(table_name);
            if (it == wal_buffers.end() || it->second.empty()) {
                return true;
            }
            chunk.swap(it->second);
        }

        std::ofstream out(wal_file_path(table_name), std::ios::binary | std::ios::app);
        if (!out) {
            error = "unable to open WAL file for append";
            return false;
        }

        out.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        if (!out) {
            error = "failed writing WAL bytes";
            return false;
        }

        return true;
    }

    bool append_wal_frame(
        const std::string& table_name,
        uint16_t op,
        std::string_view payload,
        bool force_flush,
        std::string& error) {
        if (payload.size() > std::numeric_limits<uint32_t>::max()) {
            error = "WAL payload too large";
            return false;
        }

        const uint32_t payload_size = static_cast<uint32_t>(payload.size());
        const uint32_t checksum = wal_checksum_xor32(payload.data(), payload.size(), op, payload_size);

        const size_t frame_size = sizeof(kWalMagic) + sizeof(kWalVersion) + sizeof(op) + sizeof(payload_size) +
                                  sizeof(checksum) + payload.size();

        if (payload.size() >= (kWalFlushThresholdBytes / 2U) && !force_flush) {
            std::ofstream out(wal_file_path(table_name), std::ios::binary | std::ios::app);
            if (!out) {
                error = "unable to open WAL file for direct append";
                return false;
            }

            out.write(reinterpret_cast<const char*>(&kWalMagic), sizeof(kWalMagic));
            out.write(reinterpret_cast<const char*>(&kWalVersion), sizeof(kWalVersion));
            out.write(reinterpret_cast<const char*>(&op), sizeof(op));
            out.write(reinterpret_cast<const char*>(&payload_size), sizeof(payload_size));
            out.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
            if (!payload.empty()) {
                out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
            }
            if (!out) {
                error = "failed writing direct WAL frame";
                return false;
            }
            return true;
        }

        bool should_flush = force_flush;
        {
            std::lock_guard<std::mutex> lock(wal_mutex);
            auto& buf = wal_buffers[table_name];
            buf.reserve(buf.size() + frame_size);

            auto append_raw = [&buf](const void* ptr, size_t len) {
                const auto* p = static_cast<const char*>(ptr);
                buf.insert(buf.end(), p, p + len);
            };

            append_raw(&kWalMagic, sizeof(kWalMagic));
            append_raw(&kWalVersion, sizeof(kWalVersion));
            append_raw(&op, sizeof(op));
            append_raw(&payload_size, sizeof(payload_size));
            append_raw(&checksum, sizeof(checksum));
            if (!payload.empty()) {
                append_raw(payload.data(), payload.size());
            }

            if (buf.size() >= kWalFlushThresholdBytes) {
                should_flush = true;
            }
        }

        if (should_flush) {
            return flush_wal_buffer(table_name, error);
        }
        return true;
    }

    bool truncate_wal_file(const std::string& table_name, std::string& error) {
        {
            std::lock_guard<std::mutex> lock(wal_mutex);
            wal_buffers.erase(table_name);
        }

        std::ofstream out(wal_file_path(table_name), std::ios::binary | std::ios::trunc);
        if (!out) {
            error = "unable to truncate WAL file";
            return false;
        }
        return true;
    }

    bool checkpoint_table(const std::shared_ptr<storage::Table>& table, std::string& error) {
        std::string flush_error;
        if (!flush_wal_buffer(table->name(), flush_error)) {
            error = "wal flush failed: " + flush_error;
            return false;
        }
        if (!persist_table(table, error)) {
            return false;
        }
        if (!persist_schema(table, error)) {
            return false;
        }
        if (!truncate_wal_file(table->name(), error)) {
            return false;
        }
        return true;
    }

    bool replay_wal_for_table(const std::shared_ptr<storage::Table>& table, std::string& error) {
        const std::string path = wal_file_path(table->name());
        std::error_code ec;
        if (!std::filesystem::exists(path, ec)) {
            return true;
        }

        std::ifstream in(path, std::ios::binary);
        if (!in) {
            error = "unable to open WAL file for replay";
            return false;
        }

        while (true) {
            uint32_t magic = 0;
            in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
            if (!in) {
                if (in.eof()) {
                    return true;
                }
                error = "failed reading WAL frame magic";
                return false;
            }

            uint16_t version = 0;
            uint16_t op = 0;
            uint32_t payload_size = 0;
            uint32_t checksum = 0;

            in.read(reinterpret_cast<char*>(&version), sizeof(version));
            in.read(reinterpret_cast<char*>(&op), sizeof(op));
            in.read(reinterpret_cast<char*>(&payload_size), sizeof(payload_size));
            in.read(reinterpret_cast<char*>(&checksum), sizeof(checksum));
            if (!in) {
                util::log_error("WAL truncated at frame header for table=" + table->name());
                return true;
            }

            if (magic != kWalMagic || version != kWalVersion) {
                util::log_error("WAL frame invalid header for table=" + table->name() + ", stopping replay");
                return true;
            }

            std::vector<char> payload(payload_size);
            if (payload_size > 0) {
                in.read(payload.data(), static_cast<std::streamsize>(payload.size()));
                if (!in) {
                    util::log_error("WAL truncated at payload for table=" + table->name() + ", stopping replay");
                    return true;
                }
            }

            const uint32_t expected = wal_checksum_xor32(payload.data(), payload.size(), op, payload_size);
            if (expected != checksum) {
                util::log_error("WAL checksum mismatch for table=" + table->name() + ", stopping replay");
                return true;
            }

            if (op == kWalOpInsertRows) {
                size_t applied_rows = 0;
                if (!table->apply_wal_payload(std::string_view(payload.data(), payload.size()), applied_rows, error)) {
                    error = "failed applying WAL rows: " + error;
                    return false;
                }
            } else if (op == kWalOpInsertSqlValues) {
                size_t inserted_rows = 0;
                if (!table->insert_values_sql(
                        std::string_view(payload.data(), payload.size()),
                        inserted_rows,
                        error,
                        nullptr)) {
                    error = "failed replaying WAL SQL values: " + error;
                    return false;
                }
            } else if (op == kWalOpTruncate) {
                table->clear();
            } else {
                util::log_error("WAL unknown op for table=" + table->name() + ", stopping replay");
                return true;
            }
        }
    }

    void flush_all_tables() {
        if (!has_active_database()) {
            return;
        }

        std::vector<std::shared_ptr<storage::Table>> tables;
        {
            std::shared_lock<std::shared_mutex> lock(active_db.mutex);
            tables.reserve(active_db.tables.size());
            for (const auto& kv : active_db.tables) {
                tables.push_back(kv.second);
            }
        }

        for (const auto& table : tables) {
            std::string checkpoint_error;
            if (!checkpoint_table(table, checkpoint_error)) {
                util::log_error("final checkpoint failed table=" + table->name() + " error=" + checkpoint_error);
            }
        }
    }

    void load_tables_from_disk(const std::string& db_name) {
        {
            std::unique_lock<std::shared_mutex> lock(active_db.mutex);
            active_db.tables.clear();
        }
        {
            std::lock_guard<std::mutex> lock(wal_mutex);
            wal_buffers.clear();
        }

        const std::string db_dir = database_dir_path(db_name);
        std::error_code ec;
        if (!std::filesystem::exists(db_dir, ec)) {
            return;
        }

        for (const auto& entry : std::filesystem::directory_iterator(db_dir, ec)) {
            if (ec) {
                util::log_error("failed to iterate database directory " + db_dir);
                break;
            }
            if (!entry.is_regular_file()) {
                continue;
            }
            if (entry.path().extension() != ".tbl") {
                continue;
            }

            std::string error;
            auto table = storage::Table::load_from_file(entry.path().string(), error);
            if (!table) {
                util::log_error("failed loading table file " + entry.path().string() + " error=" + error);
                continue;
            }

            std::string replay_error;
            if (!replay_wal_for_table(table, replay_error)) {
                util::log_error("failed replaying WAL for table " + table->name() + " error=" + replay_error);
                continue;
            }

            std::unique_lock<std::shared_mutex> lock(active_db.mutex);
            active_db.tables[table->name()] = table;
            lock.unlock();

            std::string schema_error;
            if (!persist_schema(table, schema_error)) {
                util::log_error("failed writing schema file for " + table->name() + " error=" + schema_error);
            }
            util::log_info("loaded table from disk db=" + db_name + " table=" + table->name());
        }
    }

    std::shared_ptr<storage::Table> find_table(const std::string& table_name) const {
        if (!has_active_database()) {
            return nullptr;
        }
        std::shared_lock<std::shared_mutex> lock(active_db.mutex);
        auto it = active_db.tables.find(table_name);
        if (it == active_db.tables.end()) {
            return nullptr;
        }
        return it->second;
    }

    ExecutionResult execute_create_database(const parser::CreateDatabaseStatement& stmt) {
        const std::string db_dir = database_dir_path(stmt.database);
        std::error_code ec;
        if (std::filesystem::exists(db_dir, ec)) {
            return {false, "database already exists", {}};
        }
        std::filesystem::create_directories(db_dir, ec);
        if (ec) {
            return {false, "failed to create database", {}};
        }
        util::log_info("CREATE DATABASE " + stmt.database);
        return {true, {}, {}};
    }

    ExecutionResult execute_drop_database(const parser::DropDatabaseStatement& stmt) {
        const std::string db_dir = database_dir_path(stmt.database);
        std::error_code ec;
        if (!std::filesystem::exists(db_dir, ec)) {
            return {false, "database not found", {}};
        }

        if (has_active_database() && active_db.name == stmt.database) {
            flush_all_tables();
            {
                std::unique_lock<std::shared_mutex> lock(active_db.mutex);
                active_db.tables.clear();
                active_db.name.clear();
            }
            {
                std::lock_guard<std::mutex> lock(wal_mutex);
                wal_buffers.clear();
            }
        }

        std::filesystem::remove_all(db_dir, ec);
        if (ec) {
            return {false, "failed to drop database", {}};
        }

        if (!has_active_database()) {
            (void)activate_default_database_if_needed();
        }

        util::log_info("DROP DATABASE " + stmt.database);
        return {true, {}, {}};
    }

    ExecutionResult execute_use_database(const parser::UseDatabaseStatement& stmt) {
        std::string error;
        if (!activate_database(stmt.database, error)) {
            return {false, error, {}};
        }

        util::log_info("USE " + stmt.database);
        return {true, {}, {}};
    }

    ExecutionResult execute_drop_table(const parser::DropTableStatement& stmt) {
        ExecutionResult gate;
        if (!ensure_database_selected(gate)) {
            return gate;
        }

        std::shared_ptr<storage::Table> table;
        {
            std::unique_lock<std::shared_mutex> lock(active_db.mutex);
            auto it = active_db.tables.find(stmt.table);
            if (it == active_db.tables.end()) {
                return {false, "table not found", {}};
            }
            table = it->second;
            active_db.tables.erase(it);
        }

        std::error_code ec;
        std::filesystem::remove(table_file_path(stmt.table), ec);
        ec.clear();
        std::filesystem::remove(schema_file_path(stmt.table), ec);
        ec.clear();
        std::filesystem::remove(wal_file_path(stmt.table), ec);
        {
            std::lock_guard<std::mutex> lock(wal_mutex);
            wal_buffers.erase(stmt.table);
        }

        util::log_info("DROP TABLE " + stmt.table + " db=" + active_db.name);
        return {true, {}, {}};
    }

    ExecutionResult execute_create(const parser::CreateTableStatement& stmt) {
        ExecutionResult gate;
        if (!ensure_database_selected(gate)) {
            return gate;
        }

        std::unique_lock<std::shared_mutex> lock(active_db.mutex);
        auto it = active_db.tables.find(stmt.table);
        if (it != active_db.tables.end()) {
            if (stmt.if_not_exists) {
                return {true, {}, {}};
            }

            const auto& existing_cols = it->second->columns();
            bool same_schema = existing_cols.size() == stmt.columns.size();
            if (same_schema) {
                for (size_t i = 0; i < existing_cols.size(); ++i) {
                    if (existing_cols[i].name != stmt.columns[i].name || existing_cols[i].type != stmt.columns[i].type) {
                        same_schema = false;
                        break;
                    }
                }
            }

            if (!same_schema) {
                return {false, "table already exists", {}};
            }

            auto table = it->second;
            lock.unlock();

            table->clear();

            std::string persist_error;
            if (!persist_table(table, persist_error)) {
                return {false, "persist failed: " + persist_error, {}};
            }

            std::string schema_error;
            if (!persist_schema(table, schema_error)) {
                return {false, "schema persist failed: " + schema_error, {}};
            }
            std::string wal_reset_error;
            if (!truncate_wal_file(stmt.table, wal_reset_error)) {
                return {false, "wal reset failed: " + wal_reset_error, {}};
            }

            util::log_info("CREATE TABLE reset existing " + stmt.table);
            return {true, {}, {}};
        }

        auto table = std::make_shared<storage::Table>(stmt.table, stmt.columns);
        active_db.tables.emplace(stmt.table, table);
        lock.unlock();

        std::string persist_error;
        if (!persist_table(table, persist_error)) {
            return {false, "persist failed: " + persist_error, {}};
        }

        std::string schema_error;
        if (!persist_schema(table, schema_error)) {
            return {false, "schema persist failed: " + schema_error, {}};
        }

        std::string wal_reset_error;
        if (!truncate_wal_file(stmt.table, wal_reset_error)) {
            return {false, "wal reset failed: " + wal_reset_error, {}};
        }

        util::log_info("CREATE TABLE " + stmt.table + " db=" + active_db.name);
        return {true, {}, {}};
    }

    ExecutionResult execute_delete(const parser::DeleteStatement& stmt) {
        ExecutionResult gate;
        if (!ensure_database_selected(gate)) {
            return gate;
        }

        auto table = find_table(stmt.table);
        if (!table) {
            return {false, "table not found", {}};
        }
        table->clear();
        std::string checkpoint_error;
        if (!checkpoint_table(table, checkpoint_error)) {
            return {false, "checkpoint failed: " + checkpoint_error, {}};
        }

        util::log_info("DELETE FROM " + stmt.table + " db=" + active_db.name);
        return {true, {}, {}};
    }

    ExecutionResult execute_insert(const parser::InsertStatement& stmt) {
        ExecutionResult gate;
        if (!ensure_database_selected(gate)) {
            return gate;
        }

        auto table = find_table(stmt.table);
        if (!table) {
            return {false, "table not found", {}};
        }

        std::vector<char> wal_payload;
        std::string error;
        if (!table->insert_batch(stmt.rows, error, &wal_payload)) {
            return {false, error, {}};
        }

        std::string wal_error;
        if (!append_wal_frame(stmt.table, kWalOpInsertRows, std::string_view(wal_payload.data(), wal_payload.size()), false, wal_error)) {
            return {false, "wal append failed: " + wal_error, {}};
        }

        util::log_info("INSERT INTO " + stmt.table + " rows=" + std::to_string(stmt.rows.size()) + " db=" + active_db.name);
        return {true, {}, {}};
    }

    ExecutionResult execute_insert_rows(const std::string& table_name, const std::vector<std::vector<parser::ValueToken>>& rows) {
        ExecutionResult gate;
        if (!ensure_database_selected(gate)) {
            return gate;
        }

        auto table = find_table(table_name);
        if (!table) {
            return {false, "table not found", {}};
        }

        std::vector<char> wal_payload;
        std::string error;
        if (!table->insert_batch(rows, error, &wal_payload)) {
            return {false, error, {}};
        }

        std::string wal_error;
        if (!append_wal_frame(
                table_name,
                kWalOpInsertRows,
                std::string_view(wal_payload.data(), wal_payload.size()),
                false,
                wal_error)) {
            return {false, "wal append failed: " + wal_error, {}};
        }

        return {true, {}, {}};
    }

    static void skip_spaces(const std::string& sql, size_t& pos) {
        while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) {
            ++pos;
        }
    }

    static bool starts_with_insert(std::string_view sql) {
        size_t i = 0;
        while (i < sql.size() && std::isspace(static_cast<unsigned char>(sql[i]))) {
            ++i;
        }
        if (i + 6 > sql.size()) {
            return false;
        }
        return util::iequals(sql.substr(i, 6), "INSERT");
    }

    static bool starts_with_checkpoint(std::string_view sql) {
        size_t i = 0;
        while (i < sql.size() && std::isspace(static_cast<unsigned char>(sql[i]))) {
            ++i;
        }
        if (i + 10 > sql.size()) {
            return false;
        }
        return util::iequals(sql.substr(i, 10), "CHECKPOINT");
    }

    ExecutionResult execute_checkpoint_sql(const std::string& sql) {
        ExecutionResult gate;
        if (!ensure_database_selected(gate)) {
            return gate;
        }

        size_t pos = 0;
        skip_spaces(sql, pos);
        if (!(pos + 10 <= sql.size() && util::iequals(std::string_view(sql.data() + pos, 10), "CHECKPOINT"))) {
            return {false, "invalid CHECKPOINT syntax", {}};
        }
        pos += 10;
        skip_spaces(sql, pos);

        size_t start = pos;
        while (pos < sql.size() && !std::isspace(static_cast<unsigned char>(sql[pos])) && sql[pos] != ';') {
            ++pos;
        }
        if (start == pos) {
            return {false, "CHECKPOINT table missing", {}};
        }

        std::string table_name = util::to_upper(std::string_view(sql.data() + start, pos - start));
        auto table = find_table(table_name);
        if (!table) {
            return {false, "table not found", {}};
        }

        std::string checkpoint_error;
        if (!checkpoint_table(table, checkpoint_error)) {
            return {false, "checkpoint failed: " + checkpoint_error, {}};
        }
        util::log_info("CHECKPOINT " + table_name + " db=" + active_db.name);
        return {true, {}, {}};
    }

    ExecutionResult execute_insert_fast_sql(const std::string& sql) {
        ExecutionResult gate;
        if (!ensure_database_selected(gate)) {
            return gate;
        }

        size_t pos = 0;
        skip_spaces(sql, pos);

        if (!(pos + 6 <= sql.size() && util::iequals(std::string_view(sql.data() + pos, 6), "INSERT"))) {
            return {false, "invalid INSERT syntax", {}};
        }
        pos += 6;
        skip_spaces(sql, pos);

        if (!(pos + 4 <= sql.size() && util::iequals(std::string_view(sql.data() + pos, 4), "INTO"))) {
            return {false, "INSERT INTO expected", {}};
        }
        pos += 4;
        skip_spaces(sql, pos);

        size_t table_start = pos;
        while (pos < sql.size() && !std::isspace(static_cast<unsigned char>(sql[pos])) && sql[pos] != ';') {
            ++pos;
        }
        if (table_start == pos) {
            return {false, "table name missing", {}};
        }

        std::string table_name = util::to_upper(std::string_view(sql.data() + table_start, pos - table_start));
        skip_spaces(sql, pos);

        if (!(pos + 6 <= sql.size() && util::iequals(std::string_view(sql.data() + pos, 6), "VALUES"))) {
            return {false, "VALUES expected", {}};
        }
        pos += 6;

        auto table = find_table(table_name);
        if (!table) {
            return {false, "table not found", {}};
        }

        size_t total_rows = 0;
        const std::string_view values_sql(sql.data() + pos, sql.size() - pos);
        std::string insert_error;
        if (!table->insert_values_sql(values_sql, total_rows, insert_error, nullptr)) {
            return {false, insert_error, {}};
        }

        std::string wal_error;
        if (!append_wal_frame(
                table_name,
            kWalOpInsertSqlValues,
            values_sql,
                false,
                wal_error)) {
            return {false, "wal append failed: " + wal_error, {}};
        }

        util::log_info("INSERT INTO " + table_name + " rows=" + std::to_string(total_rows) + " db=" + active_db.name);
        return {true, {}, {}};
    }

    std::optional<ResolvedColumn> resolve_column_single(const storage::Table& table, const parser::QualifiedName& ref) const {
        if (!ref.table.empty() && ref.table != table.name()) {
            return std::nullopt;
        }
        auto idx = table.column_index(ref.column);
        if (!idx.has_value()) {
            return std::nullopt;
        }
        return ResolvedColumn{0, *idx};
    }

    std::optional<ResolvedColumn> resolve_column_join(
        const storage::Table& left,
        const storage::Table& right,
        const parser::QualifiedName& ref,
        std::string& error) const {
        if (!ref.table.empty()) {
            if (ref.table == left.name()) {
                auto idx = left.column_index(ref.column);
                if (!idx.has_value()) {
                    return std::nullopt;
                }
                return ResolvedColumn{0, *idx};
            }
            if (ref.table == right.name()) {
                auto idx = right.column_index(ref.column);
                if (!idx.has_value()) {
                    return std::nullopt;
                }
                return ResolvedColumn{1, *idx};
            }
            return std::nullopt;
        }

        auto l = left.column_index(ref.column);
        auto r = right.column_index(ref.column);
        if (l.has_value() && r.has_value()) {
            error = "ambiguous column";
            return std::nullopt;
        }
        if (l.has_value()) {
            return ResolvedColumn{0, *l};
        }
        if (r.has_value()) {
            return ResolvedColumn{1, *r};
        }
        return std::nullopt;
    }

    bool evaluate_condition_single(
        const storage::Table& table,
        size_t row_idx,
        const parser::Condition& cond,
        const ResolvedColumn& col) const {
        const auto type = table.column_type(col.column_index);
        const auto cell = table.get_cell(row_idx, col.column_index);

        if (type == parser::ColumnType::Varchar) {
            const std::string lhs = table.get_string(cell);
            return compare_string(lhs, cond.op, cond.rhs.text);
        }

        double lhs = (type == parser::ColumnType::Decimal) ? cell.f64 : static_cast<double>(cell.i64);
        double rhs = 0.0;
        if (!parse_double(cond.rhs.text, rhs)) {
            return false;
        }
        return compare_numeric(lhs, cond.op, rhs);
    }

    bool evaluate_condition_join(
        const storage::Table& left,
        size_t left_row,
        const storage::Table& right,
        size_t right_row,
        const parser::Condition& cond,
        const ResolvedColumn& col) const {
        const storage::Table& table = (col.table_id == 0) ? left : right;
        const size_t row = (col.table_id == 0) ? left_row : right_row;

        const auto type = table.column_type(col.column_index);
        const auto cell = table.get_cell(row, col.column_index);

        if (type == parser::ColumnType::Varchar) {
            const std::string lhs = table.get_string(cell);
            return compare_string(lhs, cond.op, cond.rhs.text);
        }

        double lhs = (type == parser::ColumnType::Decimal) ? cell.f64 : static_cast<double>(cell.i64);
        double rhs = 0.0;
        if (!parse_double(cond.rhs.text, rhs)) {
            return false;
        }
        return compare_numeric(lhs, cond.op, rhs);
    }

    std::string stringify_cell(const storage::Table& table, size_t row, size_t col) const {
        return table.value_as_string(row, col);
    }

    ExecutionResult execute_select_single(const parser::SelectStatement& stmt, const std::shared_ptr<storage::Table>& table) {
        std::shared_lock<std::shared_mutex> lock(table->mutex_);

        std::vector<ResolvedColumn> projections;
        projections.reserve(stmt.select_all ? table->column_count() : stmt.projections.size());

        if (stmt.select_all) {
            for (size_t i = 0; i < table->column_count(); ++i) {
                projections.push_back({0, i});
            }
        } else {
            for (const auto& ref : stmt.projections) {
                auto resolved = resolve_column_single(*table, ref);
                if (!resolved.has_value()) {
                    return {false, "unknown projection column", {}};
                }
                projections.push_back(*resolved);
            }
        }

        std::optional<ResolvedColumn> where_col;
        if (stmt.where.has_value()) {
            where_col = resolve_column_single(*table, stmt.where->lhs);
            if (!where_col.has_value()) {
                return {false, "unknown WHERE column", {}};
            }
        }

        std::optional<ResolvedColumn> order_col;
        if (stmt.order_by.has_value()) {
            order_col = resolve_column_single(*table, stmt.order_by->key);
            if (!order_col.has_value()) {
                return {false, "unknown ORDER BY column", {}};
            }
        }

        const int64_t now_epoch = static_cast<int64_t>(std::time(nullptr));

        std::vector<size_t> candidates;
        candidates.reserve(table->row_count());

        if (stmt.where.has_value() && stmt.where->op == parser::Operator::Eq &&
            table->primary_key_column_matches(stmt.where->lhs.column)) {
            int64_t key = 0;
            if (parse_int64(stmt.where->rhs.text, key)) {
                auto row = table->lookup_primary_key(key);
                if (row.has_value() && *row < table->row_count() && !table->is_row_expired(*row, now_epoch)) {
                    if (evaluate_condition_single(*table, *row, *stmt.where, *where_col)) {
                        candidates.push_back(*row);
                    }
                }
            }
        } else {
            for (size_t row = 0; row < table->row_count(); ++row) {
                if (table->is_row_expired(row, now_epoch)) {
                    continue;
                }
                if (stmt.where.has_value() && !evaluate_condition_single(*table, row, *stmt.where, *where_col)) {
                    continue;
                }
                candidates.push_back(row);
            }
        }

        struct ResultRow {
            std::vector<std::string> values;
            std::string sort_text;
            double sort_num = 0.0;
            bool sort_numeric = false;
        };

        std::vector<ResultRow> result_rows;
        result_rows.reserve(candidates.size());

        for (size_t row : candidates) {
            ResultRow rr;
            rr.values.reserve(projections.size());
            for (const auto& proj : projections) {
                rr.values.push_back(stringify_cell(*table, row, proj.column_index));
            }

            if (order_col.has_value()) {
                auto type = table->column_type(order_col->column_index);
                if (type == parser::ColumnType::Varchar) {
                    rr.sort_text = stringify_cell(*table, row, order_col->column_index);
                } else {
                    rr.sort_numeric = true;
                    auto cell = table->get_cell(row, order_col->column_index);
                    rr.sort_num = (type == parser::ColumnType::Decimal) ? cell.f64 : static_cast<double>(cell.i64);
                }
            }
            result_rows.push_back(std::move(rr));
        }

        if (stmt.order_by.has_value()) {
            const bool desc = stmt.order_by->desc;
            std::sort(result_rows.begin(), result_rows.end(), [desc](const ResultRow& a, const ResultRow& b) {
                if (a.sort_numeric || b.sort_numeric) {
                    return desc ? (a.sort_num > b.sort_num) : (a.sort_num < b.sort_num);
                }
                return desc ? (a.sort_text > b.sort_text) : (a.sort_text < b.sort_text);
            });
        }

        ExecutionResult out;
        out.ok = true;
        out.rows.reserve(result_rows.size());
        for (auto& rr : result_rows) {
            out.rows.push_back(std::move(rr.values));
        }
        return out;
    }

    ExecutionResult execute_select_join(
        const parser::SelectStatement& stmt,
        const std::shared_ptr<storage::Table>& left,
        const std::shared_ptr<storage::Table>& right) {
        std::shared_lock<std::shared_mutex> left_lock(left->mutex_);
        std::shared_lock<std::shared_mutex> right_lock(right->mutex_);

        std::vector<ResolvedColumn> projections;
        projections.reserve(stmt.projections.size());
        std::string resolve_error;

        if (stmt.select_all) {
            for (size_t i = 0; i < left->column_count(); ++i) {
                projections.push_back({0, i});
            }
            for (size_t i = 0; i < right->column_count(); ++i) {
                projections.push_back({1, i});
            }
        } else {
            for (const auto& ref : stmt.projections) {
                auto rc = resolve_column_join(*left, *right, ref, resolve_error);
                if (!rc.has_value()) {
                    return {false, resolve_error.empty() ? "unknown projection column" : resolve_error, {}};
                }
                projections.push_back(*rc);
            }
        }

        auto left_join_col = resolve_column_join(*left, *right, stmt.join->left_key, resolve_error);
        auto right_join_col = resolve_column_join(*left, *right, stmt.join->right_key, resolve_error);
        if (!left_join_col.has_value() || !right_join_col.has_value()) {
            return {false, "invalid join key", {}};
        }

        std::optional<ResolvedColumn> where_col;
        if (stmt.where.has_value()) {
            where_col = resolve_column_join(*left, *right, stmt.where->lhs, resolve_error);
            if (!where_col.has_value()) {
                return {false, resolve_error.empty() ? "unknown WHERE column" : resolve_error, {}};
            }
        }

        std::optional<ResolvedColumn> order_col;
        if (stmt.order_by.has_value()) {
            order_col = resolve_column_join(*left, *right, stmt.order_by->key, resolve_error);
            if (!order_col.has_value()) {
                return {false, resolve_error.empty() ? "unknown ORDER BY column" : resolve_error, {}};
            }
        }

        const int64_t now_epoch = static_cast<int64_t>(std::time(nullptr));

        std::unordered_multimap<std::string, size_t> right_index;
        right_index.reserve(right->row_count() * 2 + 1);

        for (size_t r = 0; r < right->row_count(); ++r) {
            if (right->is_row_expired(r, now_epoch)) {
                continue;
            }
            const auto key = stringify_cell(*right, r, right_join_col->column_index);
            right_index.emplace(key, r);
        }

        struct ResultRow {
            std::vector<std::string> values;
            std::string sort_text;
            double sort_num = 0.0;
            bool sort_numeric = false;
        };

        std::vector<ResultRow> result_rows;

        for (size_t l = 0; l < left->row_count(); ++l) {
            if (left->is_row_expired(l, now_epoch)) {
                continue;
            }

            const auto left_key = stringify_cell(*left, l, left_join_col->column_index);
            auto range = right_index.equal_range(left_key);
            for (auto it = range.first; it != range.second; ++it) {
                const size_t r = it->second;

                if (stmt.where.has_value() && !evaluate_condition_join(*left, l, *right, r, *stmt.where, *where_col)) {
                    continue;
                }

                ResultRow row;
                row.values.reserve(projections.size());
                for (const auto& proj : projections) {
                    if (proj.table_id == 0) {
                        row.values.push_back(stringify_cell(*left, l, proj.column_index));
                    } else {
                        row.values.push_back(stringify_cell(*right, r, proj.column_index));
                    }
                }

                if (order_col.has_value()) {
                    const storage::Table& order_table = (order_col->table_id == 0) ? *left : *right;
                    size_t order_row = (order_col->table_id == 0) ? l : r;
                    auto type = order_table.column_type(order_col->column_index);
                    if (type == parser::ColumnType::Varchar) {
                        row.sort_text = stringify_cell(order_table, order_row, order_col->column_index);
                    } else {
                        row.sort_numeric = true;
                        auto cell = order_table.get_cell(order_row, order_col->column_index);
                        row.sort_num = (type == parser::ColumnType::Decimal) ? cell.f64 : static_cast<double>(cell.i64);
                    }
                }

                result_rows.push_back(std::move(row));
            }
        }

        if (stmt.order_by.has_value()) {
            const bool desc = stmt.order_by->desc;
            std::sort(result_rows.begin(), result_rows.end(), [desc](const ResultRow& a, const ResultRow& b) {
                if (a.sort_numeric || b.sort_numeric) {
                    return desc ? (a.sort_num > b.sort_num) : (a.sort_num < b.sort_num);
                }
                return desc ? (a.sort_text > b.sort_text) : (a.sort_text < b.sort_text);
            });
        }

        ExecutionResult out;
        out.ok = true;
        out.rows.reserve(result_rows.size());
        for (auto& row : result_rows) {
            out.rows.push_back(std::move(row.values));
        }
        return out;
    }

    ExecutionResult execute_select(const parser::SelectStatement& stmt) {
        ExecutionResult gate;
        if (!ensure_database_selected(gate)) {
            return gate;
        }

        auto left = find_table(stmt.from_table);
        if (!left) {
            return {false, "table not found", {}};
        }

        if (!stmt.join.has_value()) {
            auto result = execute_select_single(stmt, left);
            util::log_info("SELECT FROM " + stmt.from_table + " rows=" + std::to_string(result.rows.size()) + " db=" + active_db.name);
            return result;
        }

        auto right = find_table(stmt.join->right_table);
        if (!right) {
            return {false, "join table not found", {}};
        }

        auto result = execute_select_join(stmt, left, right);
        util::log_info("SELECT JOIN " + stmt.from_table + " + " + stmt.join->right_table +
                       " rows=" + std::to_string(result.rows.size()) + " db=" + active_db.name);
        return result;
    }
};

Executor::Executor() : impl_(new Impl()) {}

Executor::~Executor() {
    delete impl_;
}

ExecutionResult Executor::execute(const std::string& sql) {
    if (impl_->starts_with_insert(sql)) {
        if (!impl_->cache.empty()) {
            impl_->cache.clear();
        }
        return impl_->execute_insert_fast_sql(sql);
    }

    if (impl_->starts_with_checkpoint(sql)) {
        return impl_->execute_checkpoint_sql(sql);
    }

    parser::Statement stmt;
    std::string parse_error;
    if (!impl_->parser.parse(sql, stmt, parse_error)) {
        util::log_error("parse failed: " + parse_error + " sql=" + sql);
        return {false, parse_error, {}};
    }

    return std::visit(
        [this, &sql](const auto& node) -> ExecutionResult {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, parser::CreateTableStatement>) {
                impl_->cache.clear();
                return impl_->execute_create(node);
            } else if constexpr (std::is_same_v<T, parser::CreateDatabaseStatement>) {
                impl_->cache.clear();
                return impl_->execute_create_database(node);
            } else if constexpr (std::is_same_v<T, parser::DropDatabaseStatement>) {
                impl_->cache.clear();
                return impl_->execute_drop_database(node);
            } else if constexpr (std::is_same_v<T, parser::UseDatabaseStatement>) {
                impl_->cache.clear();
                return impl_->execute_use_database(node);
            } else if constexpr (std::is_same_v<T, parser::DropTableStatement>) {
                impl_->cache.clear();
                return impl_->execute_drop_table(node);
            } else if constexpr (std::is_same_v<T, parser::DeleteStatement>) {
                impl_->cache.clear();
                return impl_->execute_delete(node);
            } else if constexpr (std::is_same_v<T, parser::InsertStatement>) {
                impl_->cache.clear();
                return impl_->execute_insert(node);
            } else if constexpr (std::is_same_v<T, parser::SelectStatement>) {
                ExecutionResult cached;
                if (impl_->cache.get(sql, cached)) {
                    return cached;
                }
                auto res = impl_->execute_select(node);
                impl_->cache.put(sql, res);
                return res;
            }
            return ExecutionResult{false, "unsupported statement", {}};
        },
        stmt);
}

}  // namespace flexql::engine
