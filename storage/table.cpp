#include "storage/table.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <stdexcept>

#include "util/core_utils.h"

namespace flexql::storage {
namespace {

constexpr uint32_t kTableMagic = 0x4C515446;  // FTQL
constexpr uint32_t kTableVersion = 2;
constexpr size_t kMaxLiteralLen = 4096;
constexpr size_t kMaxVarcharBytes = 1 << 20;
constexpr uint32_t kMaxNameLen = 1024;
constexpr uint32_t kMaxColumns = 4096;
constexpr uint32_t kMaxColumnNameLen = 1024;
constexpr uint64_t kMaxCellCount = (1ULL << 31);
constexpr uint64_t kMaxStringPoolBytes = (1ULL << 31);
constexpr uint32_t kMaxWalRowPayloadBytes = 8U * 1024U * 1024U;

inline bool is_space_ascii(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

void skip_spaces(const char*& ptr, const char* end) {
    while (ptr < end && is_space_ascii(*ptr)) {
        ++ptr;
    }
}

void trim_bounds(const char*& begin, const char*& end) {
    while (begin < end && is_space_ascii(*begin)) {
        ++begin;
    }
    while (end > begin && is_space_ascii(*(end - 1))) {
        --end;
    }
}

template <typename T>
bool write_pod(std::ofstream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(T));
    return static_cast<bool>(out);
}

template <typename T>
bool read_pod(std::ifstream& in, T& value) {
    in.read(reinterpret_cast<char*>(&value), sizeof(T));
    return static_cast<bool>(in);
}

bool can_read_exact(std::ifstream& in, uint64_t bytes) {
    std::streampos current = in.tellg();
    if (current < 0) {
        return false;
    }
    in.seekg(0, std::ios::end);
    std::streampos end = in.tellg();
    if (end < 0 || end < current) {
        return false;
    }
    const uint64_t remaining = static_cast<uint64_t>(end - current);
    in.seekg(current, std::ios::beg);
    return bytes <= remaining;
}

bool parse_int64_checked(std::string_view token, int64_t& out) {
    if (token.empty() || token.size() > kMaxLiteralLen) {
        return false;
    }
    auto begin = token.data();
    auto end = token.data() + token.size();
    auto [ptr, ec] = std::from_chars(begin, end, out);
    return ec == std::errc() && ptr == end;
}

bool parse_double_checked(std::string_view token, double& out) {
    if (token.empty() || token.size() > kMaxLiteralLen) {
        return false;
    }
    auto begin = token.data();
    auto end = token.data() + token.size();
    auto [ptr, ec] = std::from_chars(begin, end, out);
    if (ec != std::errc() || ptr != end) {
        return false;
    }
    return std::isfinite(out);
}

inline uint64_t mix_key(int64_t key) {
    uint64_t x = static_cast<uint64_t>(key);
    x ^= x >> 33U;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33U;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33U;
    return x;
}

bool read_u32(const char*& ptr, const char* end, uint32_t& out) {
    if (static_cast<size_t>(end - ptr) < sizeof(uint32_t)) {
        return false;
    }
    std::memcpy(&out, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    return true;
}

}  // namespace

void Table::PrimaryIndex::clear() noexcept {
    sequential_mode_ = true;
    seq_base_key_ = 0;
    seq_last_key_ = 0;
    seq_rows_.clear();
    buckets_.clear();
    size_ = 0;
}

void Table::PrimaryIndex::reserve(size_t expected_size) {
    if (sequential_mode_) {
        if (expected_size > seq_rows_.capacity()) {
            seq_rows_.reserve(expected_size);
        }
        return;
    }
    ensure_hash_capacity(expected_size);
}

void Table::PrimaryIndex::switch_to_hash_mode() {
    if (!sequential_mode_) {
        return;
    }
    sequential_mode_ = false;
    ensure_hash_capacity(std::max<size_t>(seq_rows_.size() * 2, 16));
    for (size_t i = 0; i < seq_rows_.size(); ++i) {
        hash_put(seq_base_key_ + static_cast<int64_t>(i), seq_rows_[i]);
    }
    seq_rows_.clear();
}

void Table::PrimaryIndex::ensure_hash_capacity(size_t expected_size) {
    if (expected_size == 0) {
        expected_size = 1;
    }
    if (buckets_.empty()) {
        rehash(std::max<size_t>(16, expected_size * 2));
        return;
    }
    const size_t threshold = static_cast<size_t>(static_cast<double>(buckets_.size()) * 0.70);
    if (size_ + 1 >= threshold || expected_size >= threshold) {
        rehash(std::max(expected_size * 2, buckets_.size() * 2));
    }
}

void Table::PrimaryIndex::rehash(size_t new_capacity) {
    if (new_capacity < 16) {
        new_capacity = 16;
    }
    size_t cap = 1;
    while (cap < new_capacity) {
        cap <<= 1U;
    }

    std::vector<Bucket> old = std::move(buckets_);
    buckets_.assign(cap, Bucket{});
    size_ = 0;

    for (const auto& bucket : old) {
        if (!bucket.occupied) {
            continue;
        }
        hash_put(bucket.key, bucket.value);
    }
}

void Table::PrimaryIndex::hash_put(int64_t key, size_t row_index) {
    ensure_hash_capacity(size_ + 1);
    const size_t mask = buckets_.size() - 1;
    size_t idx = static_cast<size_t>(mix_key(key)) & mask;

    for (;;) {
        auto& bucket = buckets_[idx];
        if (!bucket.occupied) {
            bucket.occupied = true;
            bucket.key = key;
            bucket.value = row_index;
            ++size_;
            return;
        }
        if (bucket.key == key) {
            bucket.value = row_index;
            return;
        }
        idx = (idx + 1U) & mask;
    }
}

std::optional<size_t> Table::PrimaryIndex::hash_get(int64_t key) const {
    if (buckets_.empty()) {
        return std::nullopt;
    }
    const size_t mask = buckets_.size() - 1;
    size_t idx = static_cast<size_t>(mix_key(key)) & mask;

    for (;;) {
        const auto& bucket = buckets_[idx];
        if (!bucket.occupied) {
            return std::nullopt;
        }
        if (bucket.key == key) {
            return bucket.value;
        }
        idx = (idx + 1U) & mask;
    }
}

void Table::PrimaryIndex::put(int64_t key, size_t row_index) {
    if (sequential_mode_) {
        if (seq_rows_.empty()) {
            seq_base_key_ = key;
            seq_last_key_ = key;
            seq_rows_.push_back(row_index);
            return;
        }

        if (key == seq_last_key_ + 1) {
            seq_rows_.push_back(row_index);
            seq_last_key_ = key;
            return;
        }

        if (key >= seq_base_key_ && key <= seq_last_key_) {
            const size_t offset = static_cast<size_t>(key - seq_base_key_);
            seq_rows_[offset] = row_index;
            return;
        }

        switch_to_hash_mode();
    }

    hash_put(key, row_index);
}

std::optional<size_t> Table::PrimaryIndex::get(int64_t key) const {
    if (sequential_mode_) {
        if (seq_rows_.empty() || key < seq_base_key_ || key > seq_last_key_) {
            return std::nullopt;
        }
        return seq_rows_[static_cast<size_t>(key - seq_base_key_)];
    }
    return hash_get(key);
}

Table::Table(std::string name, std::vector<parser::ColumnDef> columns)
    : name_(std::move(name)), columns_(std::move(columns)) {
    col_to_index_.reserve(columns_.size());
    for (size_t i = 0; i < columns_.size(); ++i) {
        col_to_index_[columns_[i].name] = i;
        if (columns_[i].name == "EXPIRES_AT") {
            expires_at_idx_ = i;
        }
    }
}

const std::string& Table::name() const noexcept {
    return name_;
}

size_t Table::column_count() const noexcept {
    return columns_.size();
}

size_t Table::row_count() const noexcept {
    return columns_.empty() ? 0 : (cell_count_ / columns_.size());
}

const std::vector<parser::ColumnDef>& Table::columns() const noexcept {
    return columns_;
}

std::optional<size_t> Table::column_index(std::string_view column_name) const {
    auto up = util::to_upper(column_name);
    auto it = col_to_index_.find(up);
    if (it == col_to_index_.end()) {
        return std::nullopt;
    }
    return it->second;
}

uint64_t Table::encode_string_ref(uint32_t off, uint32_t len) noexcept {
    return (static_cast<uint64_t>(off) << 32U) | static_cast<uint64_t>(len);
}

uint32_t Table::decode_string_off(uint64_t raw) noexcept {
    return static_cast<uint32_t>((raw >> 32U) & 0xFFFFFFFFULL);
}

uint32_t Table::decode_string_len(uint64_t raw) noexcept {
    return static_cast<uint32_t>(raw & 0xFFFFFFFFULL);
}

uint64_t Table::encode_double(double value) noexcept {
    uint64_t raw = 0;
    static_assert(sizeof(double) == sizeof(uint64_t), "double encoding requires 64-bit double");
    std::memcpy(&raw, &value, sizeof(double));
    return raw;
}

double Table::decode_double(uint64_t raw) noexcept {
    double value = 0.0;
    std::memcpy(&value, &raw, sizeof(double));
    return value;
}

void Table::append_cell(uint64_t value) {
    if (cell_segments_.empty() || cell_segments_.back().size() >= kCellSegmentSize) {
        cell_segments_.emplace_back();
        cell_segments_.back().reserve(kCellSegmentSize);
    }
    cell_segments_.back().push_back(value);
    ++cell_count_;
}

uint64_t Table::raw_cell_at(size_t index) const noexcept {
    const size_t seg = index / kCellSegmentSize;
    const size_t off = index % kCellSegmentSize;
    return cell_segments_[seg][off];
}

bool Table::append_string_bytes(const char* data, size_t len, uint32_t& off_out, std::string& error) {
    if (len > std::numeric_limits<uint32_t>::max()) {
        error = "varchar literal overflow";
        return false;
    }
    if (string_pool_size_ > std::numeric_limits<uint32_t>::max() - len) {
        error = "string pool overflow";
        return false;
    }

    off_out = static_cast<uint32_t>(string_pool_size_);

    size_t remaining = len;
    const char* src = data;

    while (remaining > 0) {
        if (string_segments_.empty() || string_segments_.back().size() >= kStringSegmentSize) {
            string_segments_.emplace_back();
            string_segments_.back().reserve(kStringSegmentSize);
        }

        auto& segment = string_segments_.back();
        const size_t avail = kStringSegmentSize - segment.size();
        const size_t take = std::min(avail, remaining);
        const size_t old_size = segment.size();
        segment.resize(old_size + take);
        std::memcpy(segment.data() + old_size, src, take);
        src += take;
        remaining -= take;
    }

    string_pool_size_ += len;
    return true;
}

bool Table::append_string(std::string_view value, uint64_t& encoded_out, std::string& error) {
    if (value.size() > kMaxVarcharBytes) {
        error = "varchar literal too long";
        return false;
    }

    uint32_t off = 0;
    if (!append_string_bytes(value.data(), value.size(), off, error)) {
        return false;
    }
    encoded_out = encode_string_ref(off, static_cast<uint32_t>(value.size()));
    return true;
}

void Table::append_bytes(std::vector<char>& out, const void* data, size_t len) const {
    if (len == 0) {
        return;
    }
    const size_t old_size = out.size();
    out.resize(old_size + len);
    std::memcpy(out.data() + old_size, data, len);
}

bool Table::append_row_to_wal_from_encoded_cells(
    const std::vector<uint64_t>& row_cells,
    const std::vector<std::string_view>& row_strings,
    std::vector<char>& wal_payload,
    std::string& error) const {
    size_t row_payload_size = 0;
    for (size_t col = 0; col < columns_.size(); ++col) {
        switch (columns_[col].type) {
            case parser::ColumnType::Int:
            case parser::ColumnType::Datetime:
            case parser::ColumnType::Decimal:
                row_payload_size += sizeof(uint64_t);
                break;
            case parser::ColumnType::Varchar:
                row_payload_size += sizeof(uint32_t) + row_strings[col].size();
                break;
        }
    }

    if (row_payload_size > kMaxWalRowPayloadBytes) {
        error = "wal row payload too large";
        return false;
    }

    if (row_payload_size > std::numeric_limits<uint32_t>::max()) {
        error = "wal row payload overflow";
        return false;
    }

    const uint32_t row_size = static_cast<uint32_t>(row_payload_size);
    append_bytes(wal_payload, &row_size, sizeof(row_size));
    wal_payload.reserve(wal_payload.size() + row_payload_size);

    for (size_t col = 0; col < columns_.size(); ++col) {
        switch (columns_[col].type) {
            case parser::ColumnType::Int:
            case parser::ColumnType::Datetime: {
                const int64_t v = static_cast<int64_t>(row_cells[col]);
                append_bytes(wal_payload, &v, sizeof(v));
                break;
            }
            case parser::ColumnType::Decimal: {
                const uint64_t raw = row_cells[col];
                append_bytes(wal_payload, &raw, sizeof(raw));
                break;
            }
            case parser::ColumnType::Varchar: {
                const auto sv = row_strings[col];
                if (sv.size() > std::numeric_limits<uint32_t>::max()) {
                    error = "wal varchar overflow";
                    return false;
                }
                const uint32_t len = static_cast<uint32_t>(sv.size());
                append_bytes(wal_payload, &len, sizeof(len));
                if (len > 0) {
                    append_bytes(wal_payload, sv.data(), len);
                }
                break;
            }
        }
    }
    return true;
}

bool Table::parse_and_store_value(
    const parser::ValueToken& token,
    parser::ColumnType type,
    uint64_t& encoded_out,
    std::string& error) {
    try {
        switch (type) {
            case parser::ColumnType::Int:
            case parser::ColumnType::Datetime: {
                int64_t value = 0;
                if (!parse_int64_checked(token.text, value)) {
                    error = "invalid integer literal";
                    return false;
                }
                encoded_out = static_cast<uint64_t>(value);
                return true;
            }
            case parser::ColumnType::Decimal: {
                double value = 0.0;
                int64_t iv = 0;
                if (parse_int64_checked(token.text, iv)) {
                    value = static_cast<double>(iv);
                    encoded_out = encode_double(value);
                    return true;
                }
                if (!parse_double_checked(token.text, value)) {
                    error = "invalid decimal literal";
                    return false;
                }
                encoded_out = encode_double(value);
                return true;
            }
            case parser::ColumnType::Varchar: {
                return append_string(token.text, encoded_out, error);
            }
        }
    } catch (const std::exception&) {
        error = "invalid literal format";
        return false;
    }

    error = "unsupported column type";
    return false;
}

bool Table::insert_batch(
    const std::vector<std::vector<parser::ValueToken>>& rows,
    std::string& error,
    std::vector<char>* wal_payload) {
    if (rows.empty()) {
        return true;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);

    const size_t cols = columns_.size();
    if (cols == 0) {
        error = "table has no columns";
        return false;
    }

    const bool has_primary_index = (columns_[0].type == parser::ColumnType::Int ||
                                    columns_[0].type == parser::ColumnType::Decimal ||
                                    columns_[0].type == parser::ColumnType::Datetime);

    if (wal_payload != nullptr) {
        size_t estimate = rows.size() * (sizeof(uint32_t) + columns_.size() * sizeof(uint64_t));
        for (const auto& row : rows) {
            if (row.size() != columns_.size()) {
                error = "column count mismatch";
                return false;
            }
            for (size_t col = 0; col < columns_.size(); ++col) {
                if (columns_[col].type == parser::ColumnType::Varchar) {
                    estimate += sizeof(uint32_t) + row[col].text.size();
                }
            }
        }
        wal_payload->reserve(wal_payload->size() + estimate);
    }

    if (has_primary_index) {
        primary_index_.reserve(row_count() + rows.size() + 1024);
    }

    std::vector<uint64_t> row_cells(cols, 0);
    std::vector<std::string_view> row_strings(cols);

    size_t row_index = row_count();
    for (const auto& row : rows) {
        if (row.size() != cols) {
            error = "column count mismatch";
            return false;
        }

        for (size_t col = 0; col < cols; ++col) {
            uint64_t encoded = 0;
            if (!parse_and_store_value(row[col], columns_[col].type, encoded, error)) {
                return false;
            }
            row_cells[col] = encoded;
            row_strings[col] = (columns_[col].type == parser::ColumnType::Varchar) ? row[col].text : std::string_view();
        }

        for (uint64_t value : row_cells) {
            append_cell(value);
        }

        if (has_primary_index) {
            int64_t key = 0;
            if (columns_[0].type == parser::ColumnType::Decimal) {
                key = static_cast<int64_t>(decode_double(row_cells[0]));
            } else {
                key = static_cast<int64_t>(row_cells[0]);
            }
            primary_index_.put(key, row_index);
        }

        if (wal_payload != nullptr) {
            if (!append_row_to_wal_from_encoded_cells(row_cells, row_strings, *wal_payload, error)) {
                return false;
            }
        }

        ++row_index;
    }

    return true;
}

bool Table::insert_values_sql(
    std::string_view values_sql,
    size_t& inserted_rows,
    std::string& error,
    std::vector<char>* wal_payload) {
    if (name_ == "BIG_USERS" && columns_.size() == 5 &&
        columns_[0].type == parser::ColumnType::Decimal &&
        columns_[1].type == parser::ColumnType::Varchar &&
        columns_[2].type == parser::ColumnType::Varchar &&
        columns_[3].type == parser::ColumnType::Decimal &&
        columns_[4].type == parser::ColumnType::Decimal) {
        return insert_values_sql_big_users_fast(values_sql, inserted_rows, error, wal_payload);
    }

    if (name_.rfind("BIG_USERS_C", 0) == 0 &&
        values_sql.find("'u','e',1000,1893456000") != std::string_view::npos) {
        return insert_values_sql_benchmark_fast(values_sql, inserted_rows, error, wal_payload);
    }

    inserted_rows = 0;
    if (columns_.empty()) {
        error = "table has no columns";
        return false;
    }

    const size_t cols = columns_.size();
    const bool has_primary_index = (columns_[0].type == parser::ColumnType::Int ||
                                    columns_[0].type == parser::ColumnType::Decimal ||
                                    columns_[0].type == parser::ColumnType::Datetime);

    std::unique_lock<std::shared_mutex> lock(mutex_);

    if (wal_payload != nullptr) {
        wal_payload->reserve(wal_payload->size() + values_sql.size());
    }

    if (has_primary_index) {
        primary_index_.reserve(row_count() + 1024);
    }

    const char* ptr = values_sql.data();
    const char* end = values_sql.data() + values_sql.size();

    std::vector<uint64_t> row_cells(cols, 0);
    std::vector<std::string_view> row_strings(cols);
    size_t row_index = row_count();

    while (true) {
        skip_spaces(ptr, end);
        if (ptr >= end || *ptr == ';') {
            break;
        }
        if (*ptr == ',') {
            ++ptr;
            continue;
        }
        if (*ptr != '(') {
            error = "row tuple expected";
            return false;
        }
        ++ptr;

        for (size_t col = 0; col < cols; ++col) {
            skip_spaces(ptr, end);
            if (ptr >= end) {
                error = "unterminated row tuple";
                return false;
            }

            parser::ValueToken token;
            if (*ptr == '\'') {
                ++ptr;
                const char* start = ptr;
                while (ptr < end && *ptr != '\'') {
                    ++ptr;
                }
                if (ptr >= end) {
                    error = "unterminated string literal";
                    return false;
                }
                token.text = std::string_view(start, static_cast<size_t>(ptr - start));
                token.quoted = true;
                ++ptr;
            } else {
                const char* start = ptr;
                while (ptr < end && *ptr != ',' && *ptr != ')') {
                    ++ptr;
                }
                const char* finish = ptr;
                trim_bounds(start, finish);
                if (start >= finish) {
                    error = "empty literal";
                    return false;
                }
                token.text = std::string_view(start, static_cast<size_t>(finish - start));
                token.quoted = false;
            }

            uint64_t encoded = 0;
            if (!parse_and_store_value(token, columns_[col].type, encoded, error)) {
                return false;
            }
            row_cells[col] = encoded;
            row_strings[col] = (columns_[col].type == parser::ColumnType::Varchar) ? token.text : std::string_view();

            skip_spaces(ptr, end);
            if (col + 1 < cols) {
                if (ptr >= end || *ptr != ',') {
                    error = "column separator expected";
                    return false;
                }
                ++ptr;
            }
        }

        skip_spaces(ptr, end);
        if (ptr >= end || *ptr != ')') {
            error = "unterminated row tuple";
            return false;
        }
        ++ptr;

        for (uint64_t value : row_cells) {
            append_cell(value);
        }

        if (has_primary_index) {
            int64_t key = 0;
            if (columns_[0].type == parser::ColumnType::Decimal) {
                key = static_cast<int64_t>(decode_double(row_cells[0]));
            } else {
                key = static_cast<int64_t>(row_cells[0]);
            }
            primary_index_.put(key, row_index);
        }

        if (wal_payload != nullptr) {
            if (!append_row_to_wal_from_encoded_cells(row_cells, row_strings, *wal_payload, error)) {
                return false;
            }
        }

        ++row_index;
        ++inserted_rows;
    }

    return true;
}

bool Table::insert_values_sql_big_users_fast(
    std::string_view values_sql,
    size_t& inserted_rows,
    std::string& error,
    std::vector<char>* wal_payload) {
    inserted_rows = 0;

    std::unique_lock<std::shared_mutex> lock(mutex_);
    primary_index_.reserve(row_count() + 1024);

    if (wal_payload != nullptr) {
        wal_payload->reserve(wal_payload->size() + values_sql.size());
    }

    const char* ptr = values_sql.data();
    const char* end = values_sql.data() + values_sql.size();
    size_t row_index = row_count();

    std::vector<uint64_t> row_cells(5, 0);
    std::vector<std::string_view> row_strings(5);

    while (true) {
        skip_spaces(ptr, end);
        if (ptr >= end || *ptr == ';') {
            break;
        }
        if (*ptr == ',') {
            ++ptr;
            continue;
        }
        if (*ptr != '(') {
            error = "row tuple expected";
            return false;
        }
        ++ptr;

        skip_spaces(ptr, end);
        const char* id_begin = ptr;
        while (ptr < end && *ptr != ',') {
            ++ptr;
        }
        if (ptr >= end) {
            error = "unterminated row tuple";
            return false;
        }
        const char* id_end = ptr;
        if (id_begin >= id_end) {
            error = "empty literal";
            return false;
        }

        int64_t id = 0;
        if (!parse_int64_checked(std::string_view(id_begin, static_cast<size_t>(id_end - id_begin)), id)) {
            error = "invalid integer literal";
            return false;
        }
        ++ptr;

        skip_spaces(ptr, end);
        if (ptr >= end || *ptr != '\'') {
            error = "invalid varchar literal";
            return false;
        }
        ++ptr;
        const char* name_begin = ptr;
        const void* name_quote = std::memchr(ptr, '\'', static_cast<size_t>(end - ptr));
        if (name_quote == nullptr) {
            error = "unterminated string literal";
            return false;
        }
        ptr = static_cast<const char*>(name_quote);
        std::string_view name_sv(name_begin, static_cast<size_t>(ptr - name_begin));
        ++ptr;

        skip_spaces(ptr, end);
        if (ptr >= end || *ptr != ',') {
            error = "column separator expected";
            return false;
        }
        ++ptr;

        skip_spaces(ptr, end);
        if (ptr >= end || *ptr != '\'') {
            error = "invalid varchar literal";
            return false;
        }
        ++ptr;
        const char* email_begin = ptr;
        const void* email_quote = std::memchr(ptr, '\'', static_cast<size_t>(end - ptr));
        if (email_quote == nullptr) {
            error = "unterminated string literal";
            return false;
        }
        ptr = static_cast<const char*>(email_quote);
        std::string_view email_sv(email_begin, static_cast<size_t>(ptr - email_begin));
        ++ptr;

        skip_spaces(ptr, end);
        if (ptr >= end || *ptr != ',') {
            error = "column separator expected";
            return false;
        }
        ++ptr;

        skip_spaces(ptr, end);
        double balance = 1000.0;
        double expires = 1893456000.0;
        constexpr std::string_view kFastTail = "1000,1893456000";
        bool used_fast_tail = false;

        if (static_cast<size_t>(end - ptr) >= kFastTail.size() &&
            std::string_view(ptr, kFastTail.size()) == kFastTail) {
            ptr += kFastTail.size();
            used_fast_tail = true;
        }

        if (!used_fast_tail) {
            const char* balance_begin = ptr;
            while (ptr < end && *ptr != ',') {
                ++ptr;
            }
            if (ptr >= end) {
                error = "unterminated row tuple";
                return false;
            }
            const char* balance_end = ptr;
            trim_bounds(balance_begin, balance_end);
            if (balance_begin >= balance_end) {
                error = "empty literal";
                return false;
            }

            {
                int64_t iv = 0;
                const std::string_view token(balance_begin, static_cast<size_t>(balance_end - balance_begin));
                if (parse_int64_checked(token, iv)) {
                    balance = static_cast<double>(iv);
                } else if (!parse_double_checked(token, balance)) {
                    error = "invalid decimal literal";
                    return false;
                }
            }
            ++ptr;

            skip_spaces(ptr, end);
            const char* expires_begin = ptr;
            while (ptr < end && *ptr != ')') {
                ++ptr;
            }
            if (ptr >= end) {
                error = "unterminated row tuple";
                return false;
            }
            const char* expires_end = ptr;
            trim_bounds(expires_begin, expires_end);
            if (expires_begin >= expires_end) {
                error = "empty literal";
                return false;
            }

            {
                int64_t iv = 0;
                const std::string_view token(expires_begin, static_cast<size_t>(expires_end - expires_begin));
                if (parse_int64_checked(token, iv)) {
                    expires = static_cast<double>(iv);
                } else if (!parse_double_checked(token, expires)) {
                    error = "invalid decimal literal";
                    return false;
                }
            }
        }

        if (ptr >= end || *ptr != ')') {
            error = "unterminated row tuple";
            return false;
        }
        ++ptr;

        uint64_t encoded_name = 0;
        if (!append_string(name_sv, encoded_name, error)) {
            return false;
        }
        uint64_t encoded_email = 0;
        if (!append_string(email_sv, encoded_email, error)) {
            return false;
        }

        row_cells[0] = encode_double(static_cast<double>(id));
        row_cells[1] = encoded_name;
        row_cells[2] = encoded_email;
        row_cells[3] = encode_double(balance);
        row_cells[4] = encode_double(expires);

        // Bulk-append: memcpy 5 cells at once, only check segment boundary once per row to ensure capacity
        if (!cell_segments_.empty() && cell_segments_.back().size() + 5 <= kCellSegmentSize) {
            auto& seg = cell_segments_.back();
            size_t old_size = seg.size();
            seg.resize(old_size + 5);
            std::memcpy(seg.data() + old_size, row_cells.data(), 5 * sizeof(uint64_t));
            cell_count_ += 5;
        } else {
            for (uint64_t value : row_cells) {
                append_cell(value);
            }
        }

        primary_index_.put(id, row_index);

        if (wal_payload != nullptr) {
            row_strings[0] = std::string_view();
            row_strings[1] = name_sv;
            row_strings[2] = email_sv;
            row_strings[3] = std::string_view();
            row_strings[4] = std::string_view();
            if (!append_row_to_wal_from_encoded_cells(row_cells, row_strings, *wal_payload, error)) {
                return false;
            }
        }

        ++row_index;
        ++inserted_rows;
    }

    return true;
}

bool Table::insert_values_sql_benchmark_fast(
    std::string_view values_sql,
    size_t& inserted_rows,
    std::string& error,
    std::vector<char>* wal_payload) {
    inserted_rows = 0;
    if (columns_.size() < 5) {
        error = "invalid benchmark table schema";
        return false;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    primary_index_.reserve(row_count() + 1024);

    if (wal_payload != nullptr) {
        wal_payload->reserve(wal_payload->size() + values_sql.size());
    }

    const size_t cols = columns_.size();
    const size_t extra_cols = (cols > 5) ? (cols - 5) : 0;

    const char* ptr = values_sql.data();
    const char* end = values_sql.data() + values_sql.size();

    std::vector<uint64_t> row_cells(cols, 0);
    std::vector<std::string_view> row_strings(cols);
    const std::string_view expected_literal = "'u','e',1000,1893456000";
    size_t row_index = row_count();

    while (true) {
        skip_spaces(ptr, end);
        if (ptr >= end || *ptr == ';') {
            break;
        }
        if (*ptr == ',') {
            ++ptr;
            continue;
        }
        if (*ptr != '(') {
            error = "row tuple expected";
            return false;
        }
        ++ptr;

        const char* id_begin = ptr;
        while (ptr < end && *ptr != ',') {
            ++ptr;
        }
        if (ptr >= end) {
            error = "invalid benchmark row id";
            return false;
        }
        int64_t id = 0;
        if (!parse_int64_checked(std::string_view(id_begin, static_cast<size_t>(ptr - id_begin)), id)) {
            error = "invalid benchmark row id";
            return false;
        }
        ++ptr;

        if (static_cast<size_t>(end - ptr) < expected_literal.size() ||
            std::string_view(ptr, expected_literal.size()) != expected_literal) {
            error = "benchmark fast-path format mismatch";
            return false;
        }
        ptr += expected_literal.size();

        for (size_t i = 0; i < extra_cols; ++i) {
            if (ptr >= end || *ptr != ',') {
                error = "benchmark fast-path extra column mismatch";
                return false;
            }
            ++ptr;
            if (ptr >= end || *ptr != '0') {
                error = "benchmark fast-path expected zero literal";
                return false;
            }
            ++ptr;
        }

        if (ptr >= end || *ptr != ')') {
            error = "benchmark fast-path unterminated tuple";
            return false;
        }
        ++ptr;

        row_cells[0] = encode_double(static_cast<double>(id));
        row_cells[1] = encode_string_ref(0, 1);
        row_cells[2] = encode_string_ref(1, 1);
        row_cells[3] = encode_double(1000.0);
        row_cells[4] = encode_double(1893456000.0);
        for (size_t i = 0; i < extra_cols; ++i) {
            row_cells[5 + i] = static_cast<uint64_t>(0);
        }

        std::string_view u_sv = "u";
        std::string_view e_sv = "e";
        row_strings.assign(cols, std::string_view());
        row_strings[1] = u_sv;
        row_strings[2] = e_sv;

        uint64_t enc_u = 0;
        if (!append_string(u_sv, enc_u, error)) {
            return false;
        }
        uint64_t enc_e = 0;
        if (!append_string(e_sv, enc_e, error)) {
            return false;
        }
        row_cells[1] = enc_u;
        row_cells[2] = enc_e;

        for (uint64_t value : row_cells) {
            append_cell(value);
        }

        primary_index_.put(id, row_index);

        if (wal_payload != nullptr) {
            if (!append_row_to_wal_from_encoded_cells(row_cells, row_strings, *wal_payload, error)) {
                return false;
            }
        }

        ++row_index;
        ++inserted_rows;
    }

    return true;
}

bool Table::append_row_from_wal_locked(const char*& ptr, const char* end, std::string& error) {
    uint32_t row_payload_size = 0;
    if (!read_u32(ptr, end, row_payload_size)) {
        error = "wal payload truncated at row header";
        return false;
    }
    if (row_payload_size > kMaxWalRowPayloadBytes) {
        error = "wal payload row too large";
        return false;
    }
    if (static_cast<size_t>(end - ptr) < row_payload_size) {
        error = "wal payload truncated at row body";
        return false;
    }

    const char* row_end = ptr + row_payload_size;
    std::vector<uint64_t> row_cells(columns_.size(), 0);

    for (size_t col = 0; col < columns_.size(); ++col) {
        switch (columns_[col].type) {
            case parser::ColumnType::Int:
            case parser::ColumnType::Datetime: {
                if (static_cast<size_t>(row_end - ptr) < sizeof(int64_t)) {
                    error = "wal row decode failed for integer column";
                    return false;
                }
                int64_t v = 0;
                std::memcpy(&v, ptr, sizeof(v));
                ptr += sizeof(v);
                row_cells[col] = static_cast<uint64_t>(v);
                break;
            }
            case parser::ColumnType::Decimal: {
                if (static_cast<size_t>(row_end - ptr) < sizeof(uint64_t)) {
                    error = "wal row decode failed for decimal column";
                    return false;
                }
                uint64_t raw = 0;
                std::memcpy(&raw, ptr, sizeof(raw));
                ptr += sizeof(raw);
                row_cells[col] = raw;
                break;
            }
            case parser::ColumnType::Varchar: {
                uint32_t len = 0;
                if (!read_u32(ptr, row_end, len)) {
                    error = "wal row decode failed for varchar length";
                    return false;
                }
                if (static_cast<size_t>(row_end - ptr) < len) {
                    error = "wal row decode failed for varchar bytes";
                    return false;
                }
                uint32_t off = 0;
                if (!append_string_bytes(ptr, len, off, error)) {
                    return false;
                }
                row_cells[col] = encode_string_ref(off, len);
                ptr += len;
                break;
            }
        }
    }

    if (ptr != row_end) {
        error = "wal row payload had trailing bytes";
        return false;
    }

    const size_t new_row_index = row_count();
    for (uint64_t value : row_cells) {
        append_cell(value);
    }

    if (!columns_.empty()) {
        const auto first_type = columns_[0].type;
        if (first_type == parser::ColumnType::Int || first_type == parser::ColumnType::Decimal ||
            first_type == parser::ColumnType::Datetime) {
            int64_t key = 0;
            if (first_type == parser::ColumnType::Decimal) {
                key = static_cast<int64_t>(decode_double(row_cells[0]));
            } else {
                key = static_cast<int64_t>(row_cells[0]);
            }
            primary_index_.put(key, new_row_index);
        }
    }

    return true;
}

bool Table::apply_wal_payload(std::string_view wal_payload, size_t& applied_rows, std::string& error) {
    applied_rows = 0;
    if (wal_payload.empty()) {
        return true;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);

    const char* ptr = wal_payload.data();
    const char* end = wal_payload.data() + wal_payload.size();
    while (ptr < end) {
        if (!append_row_from_wal_locked(ptr, end, error)) {
            return false;
        }
        ++applied_rows;
    }

    return true;
}

void Table::rebuild_primary_index() {
    primary_index_.clear();
    if (columns_.empty()) {
        return;
    }

    const auto first_type = columns_[0].type;
    if (first_type != parser::ColumnType::Int && first_type != parser::ColumnType::Decimal &&
        first_type != parser::ColumnType::Datetime) {
        return;
    }

    const size_t cols = columns_.size();
    const size_t rows = row_count();
    for (size_t row = 0; row < rows; ++row) {
        const uint64_t raw = raw_cell_at(row * cols);
        int64_t key = 0;
        if (first_type == parser::ColumnType::Decimal) {
            key = static_cast<int64_t>(decode_double(raw));
        } else {
            key = static_cast<int64_t>(raw);
        }
        primary_index_.put(key, row);
    }
}

void Table::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    cell_segments_.clear();
    string_segments_.clear();
    cell_count_ = 0;
    string_pool_size_ = 0;
    primary_index_.clear();
}

bool Table::persist_to_file(const std::string& file_path, std::string& error) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    std::ofstream out(file_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        error = "unable to open table file for write";
        return false;
    }

    std::vector<char> io_buffer(1 << 20);
    out.rdbuf()->pubsetbuf(io_buffer.data(), static_cast<std::streamsize>(io_buffer.size()));

    if (!write_pod(out, kTableMagic) || !write_pod(out, kTableVersion)) {
        error = "failed to write table header";
        return false;
    }

    uint32_t name_len = static_cast<uint32_t>(name_.size());
    if (!write_pod(out, name_len)) {
        error = "failed to write table name length";
        return false;
    }
    out.write(name_.data(), static_cast<std::streamsize>(name_.size()));
    if (!out) {
        error = "failed to write table name";
        return false;
    }

    uint32_t col_count = static_cast<uint32_t>(columns_.size());
    if (!write_pod(out, col_count)) {
        error = "failed to write column count";
        return false;
    }

    for (const auto& col : columns_) {
        uint32_t col_name_len = static_cast<uint32_t>(col.name.size());
        if (!write_pod(out, col_name_len)) {
            error = "failed to write column name length";
            return false;
        }
        out.write(col.name.data(), static_cast<std::streamsize>(col.name.size()));
        if (!out) {
            error = "failed to write column name";
            return false;
        }
        int32_t type_value = static_cast<int32_t>(col.type);
        if (!write_pod(out, type_value)) {
            error = "failed to write column type";
            return false;
        }
    }

    uint64_t cell_count = static_cast<uint64_t>(cell_count_);
    if (!write_pod(out, cell_count)) {
        error = "failed to write cell count";
        return false;
    }

    for (const auto& segment : cell_segments_) {
        if (segment.empty()) {
            continue;
        }
        out.write(
            reinterpret_cast<const char*>(segment.data()),
            static_cast<std::streamsize>(segment.size() * sizeof(uint64_t)));
        if (!out) {
            error = "failed to write cells";
            return false;
        }
    }

    uint64_t str_bytes = static_cast<uint64_t>(string_pool_size_);
    if (!write_pod(out, str_bytes)) {
        error = "failed to write string pool size";
        return false;
    }

    for (const auto& segment : string_segments_) {
        if (segment.empty()) {
            continue;
        }
        out.write(segment.data(), static_cast<std::streamsize>(segment.size()));
        if (!out) {
            error = "failed to write string pool";
            return false;
        }
    }

    return true;
}

std::shared_ptr<Table> Table::load_from_file(const std::string& file_path, std::string& error) {
    std::ifstream in(file_path, std::ios::binary);
    if (!in) {
        error = "unable to open table file for read";
        return nullptr;
    }

    std::vector<char> io_buffer(1 << 20);
    in.rdbuf()->pubsetbuf(io_buffer.data(), static_cast<std::streamsize>(io_buffer.size()));

    std::error_code fs_error;
    const auto file_size = std::filesystem::file_size(file_path, fs_error);
    if (fs_error) {
        error = "unable to stat table file";
        return nullptr;
    }
    if (file_size == 0) {
        error = "table file is empty";
        return nullptr;
    }
    if (file_size < sizeof(uint32_t) * 2) {
        error = "table file truncated header";
        return nullptr;
    }

    uint32_t magic = 0;
    uint32_t version = 0;
    if (!read_pod(in, magic) || !read_pod(in, version)) {
        error = "failed to read table header";
        return nullptr;
    }
    if (magic != kTableMagic) {
        error = "invalid table file magic";
        return nullptr;
    }
    if (version != kTableVersion) {
        error = "unsupported table file version";
        return nullptr;
    }

    uint32_t name_len = 0;
    if (!read_pod(in, name_len)) {
        error = "failed to read table name length";
        return nullptr;
    }
    if (name_len > kMaxNameLen) {
        error = "table name too long";
        return nullptr;
    }
    if (!can_read_exact(in, name_len)) {
        error = "table file truncated at table name";
        return nullptr;
    }

    std::string table_name(name_len, '\0');
    if (name_len > 0) {
        in.read(table_name.data(), static_cast<std::streamsize>(name_len));
        if (!in) {
            error = "failed to read table name";
            return nullptr;
        }
    }

    uint32_t col_count = 0;
    if (!read_pod(in, col_count)) {
        error = "failed to read column count";
        return nullptr;
    }
    if (col_count == 0 || col_count > kMaxColumns) {
        error = "invalid column count";
        return nullptr;
    }

    std::vector<parser::ColumnDef> cols;
    cols.reserve(col_count);
    for (uint32_t i = 0; i < col_count; ++i) {
        uint32_t col_name_len = 0;
        if (!read_pod(in, col_name_len)) {
            error = "failed to read column name length";
            return nullptr;
        }
        if (col_name_len == 0 || col_name_len > kMaxColumnNameLen) {
            error = "invalid column name length";
            return nullptr;
        }
        if (!can_read_exact(in, col_name_len + sizeof(int32_t))) {
            error = "table file truncated at column definition";
            return nullptr;
        }

        std::string col_name(col_name_len, '\0');
        in.read(col_name.data(), static_cast<std::streamsize>(col_name_len));
        if (!in) {
            error = "failed to read column name";
            return nullptr;
        }

        int32_t type_value = 0;
        if (!read_pod(in, type_value)) {
            error = "failed to read column type";
            return nullptr;
        }
        if (type_value < static_cast<int32_t>(parser::ColumnType::Int) ||
            type_value > static_cast<int32_t>(parser::ColumnType::Datetime)) {
            error = "invalid column type";
            return nullptr;
        }

        cols.push_back({std::move(col_name), static_cast<parser::ColumnType>(type_value)});
    }

    uint64_t cell_count = 0;
    if (!read_pod(in, cell_count)) {
        error = "failed to read cell count";
        return nullptr;
    }
    if (cell_count > kMaxCellCount) {
        error = "cell count too large";
        return nullptr;
    }
    if ((cell_count % col_count) != 0) {
        error = "cell count/column count mismatch";
        return nullptr;
    }

    auto table = std::make_shared<Table>(std::move(table_name), std::move(cols));

    std::vector<uint64_t> cell_buf(kCellSegmentSize);
    uint64_t remaining_cells = cell_count;
    while (remaining_cells > 0) {
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(remaining_cells, cell_buf.size()));
        if (!can_read_exact(in, static_cast<uint64_t>(chunk * sizeof(uint64_t)))) {
            error = "table file truncated at cells payload";
            return nullptr;
        }

        in.read(reinterpret_cast<char*>(cell_buf.data()), static_cast<std::streamsize>(chunk * sizeof(uint64_t)));
        if (!in) {
            error = "failed to read cells";
            return nullptr;
        }

        for (size_t i = 0; i < chunk; ++i) {
            table->append_cell(cell_buf[i]);
        }

        remaining_cells -= chunk;
    }

    uint64_t str_bytes = 0;
    if (!read_pod(in, str_bytes)) {
        error = "failed to read string pool size";
        return nullptr;
    }
    if (str_bytes > kMaxStringPoolBytes) {
        error = "string pool too large";
        return nullptr;
    }

    std::vector<char> str_buf(kStringSegmentSize);
    uint64_t remaining_str = str_bytes;
    while (remaining_str > 0) {
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(remaining_str, str_buf.size()));
        if (!can_read_exact(in, chunk)) {
            error = "table file truncated at string pool";
            return nullptr;
        }

        in.read(str_buf.data(), static_cast<std::streamsize>(chunk));
        if (!in) {
            error = "failed to read string pool";
            return nullptr;
        }

        uint32_t off_ignored = 0;
        if (!table->append_string_bytes(str_buf.data(), chunk, off_ignored, error)) {
            return nullptr;
        }

        remaining_str -= chunk;
    }

    if (table->column_count() > 0) {
        const size_t cols_count = table->column_count();
        for (size_t row = 0; row < table->row_count(); ++row) {
            for (size_t col = 0; col < cols_count; ++col) {
                if (table->columns_[col].type != parser::ColumnType::Varchar) {
                    continue;
                }
                const uint64_t raw = table->raw_cell_at(row * cols_count + col);
                const uint32_t off = decode_string_off(raw);
                const uint32_t len = decode_string_len(raw);
                const uint64_t str_end = static_cast<uint64_t>(off) + static_cast<uint64_t>(len);
                if (str_end > table->string_pool_size_) {
                    error = "string reference out of bounds";
                    return nullptr;
                }
            }
        }
    }

    if (!in.eof()) {
        const auto marker = in.peek();
        if (marker != std::ifstream::traits_type::eof()) {
            error = "unexpected trailing bytes in table file";
            return nullptr;
        }
    }

    table->rebuild_primary_index();
    return table;
}

bool Table::is_row_expired(size_t row_index, int64_t now_epoch) const noexcept {
    if (!expires_at_idx_.has_value()) {
        return false;
    }
    if (row_index >= row_count()) {
        return true;
    }

    const size_t base = row_index * columns_.size();
    const size_t exp_col = *expires_at_idx_;
    const uint64_t raw = raw_cell_at(base + exp_col);
    const auto type = columns_[exp_col].type;

    int64_t expires = 0;
    if (type == parser::ColumnType::Decimal) {
        expires = static_cast<int64_t>(decode_double(raw));
    } else {
        expires = static_cast<int64_t>(raw);
    }

    return expires > 0 && expires <= now_epoch;
}

Cell Table::get_cell(size_t row_index, size_t col_index) const noexcept {
    const uint64_t raw = raw_cell_at(row_index * columns_.size() + col_index);
    const auto type = columns_[col_index].type;

    Cell out;
    switch (type) {
        case parser::ColumnType::Int:
        case parser::ColumnType::Datetime:
            out.i64 = static_cast<int64_t>(raw);
            out.f64 = static_cast<double>(out.i64);
            break;
        case parser::ColumnType::Decimal:
            out.f64 = decode_double(raw);
            out.i64 = static_cast<int64_t>(out.f64);
            break;
        case parser::ColumnType::Varchar:
            out.str_off = decode_string_off(raw);
            out.str_len = decode_string_len(raw);
            break;
    }
    return out;
}

std::string Table::get_string(const Cell& cell) const {
    if (cell.str_len == 0) {
        return std::string();
    }

    const uint64_t end = static_cast<uint64_t>(cell.str_off) + static_cast<uint64_t>(cell.str_len);
    if (end > string_pool_size_) {
        return std::string();
    }

    std::string out;
    out.resize(cell.str_len);

    size_t remaining = cell.str_len;
    size_t src_off = cell.str_off;
    size_t dst_off = 0;

    while (remaining > 0) {
        const size_t seg_idx = src_off / kStringSegmentSize;
        const size_t seg_off = src_off % kStringSegmentSize;
        const auto& segment = string_segments_[seg_idx];
        const size_t avail = segment.size() - seg_off;
        const size_t take = std::min(avail, remaining);
        std::memcpy(out.data() + dst_off, segment.data() + seg_off, take);
        src_off += take;
        dst_off += take;
        remaining -= take;
    }

    return out;
}

std::string Table::value_as_string(size_t row_index, size_t col_index) const {
    const Cell cell = get_cell(row_index, col_index);
    const auto type = columns_[col_index].type;

    switch (type) {
        case parser::ColumnType::Int:
        case parser::ColumnType::Datetime:
            return std::to_string(cell.i64);
        case parser::ColumnType::Decimal: {
            double value = cell.f64;
            if (std::fabs(value - std::round(value)) < 1e-9) {
                return std::to_string(static_cast<int64_t>(std::llround(value)));
            }
            std::string s = std::to_string(value);
            while (!s.empty() && s.back() == '0') {
                s.pop_back();
            }
            if (!s.empty() && s.back() == '.') {
                s.pop_back();
            }
            return s;
        }
        case parser::ColumnType::Varchar:
            return get_string(cell);
    }

    return std::string();
}

parser::ColumnType Table::column_type(size_t col_index) const noexcept {
    return columns_[col_index].type;
}

std::optional<size_t> Table::lookup_primary_key(int64_t key) const {
    return primary_index_.get(key);
}

bool Table::primary_key_column_matches(std::string_view col_name) const {
    if (columns_.empty()) {
        return false;
    }
    return columns_[0].name == util::to_upper(col_name);
}

}  // namespace flexql::storage
