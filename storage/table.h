#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include <unordered_map>

#include "parser/ast.h"

namespace flexql::storage {

struct Cell {
    int64_t i64 = 0;
    double f64 = 0.0;
    uint32_t str_off = 0;
    uint32_t str_len = 0;
};

class Table {
public:
    Table(std::string name, std::vector<parser::ColumnDef> columns);

    [[nodiscard]] const std::string& name() const noexcept;
    [[nodiscard]] size_t column_count() const noexcept;
    [[nodiscard]] size_t row_count() const noexcept;
    [[nodiscard]] const std::vector<parser::ColumnDef>& columns() const noexcept;

    [[nodiscard]] std::optional<size_t> column_index(std::string_view column_name) const;

    [[nodiscard]]
    bool insert_batch(
        const std::vector<std::vector<parser::ValueToken>>& rows,
        std::string& error,
        std::vector<char>* wal_payload = nullptr);
    [[nodiscard]]
    bool insert_values_sql(
        std::string_view values_sql,
        size_t& inserted_rows,
        std::string& error,
        std::vector<char>* wal_payload = nullptr);
    [[nodiscard]]
    bool apply_wal_payload(std::string_view wal_payload, size_t& applied_rows, std::string& error);
    void clear();
    [[nodiscard]]
    bool persist_to_file(const std::string& file_path, std::string& error) const;
    [[nodiscard]]
    static std::shared_ptr<Table> load_from_file(const std::string& file_path, std::string& error);

    [[nodiscard]] bool is_row_expired(size_t row_index, int64_t now_epoch) const noexcept;

    [[nodiscard]] Cell get_cell(size_t row_index, size_t col_index) const noexcept;
    [[nodiscard]] std::string get_string(const Cell& cell) const;
    [[nodiscard]] std::string value_as_string(size_t row_index, size_t col_index) const;

    [[nodiscard]] parser::ColumnType column_type(size_t col_index) const noexcept;

    [[nodiscard]] std::optional<size_t> lookup_primary_key(int64_t key) const;
    [[nodiscard]] bool primary_key_column_matches(std::string_view col_name) const;

    mutable std::shared_mutex mutex_;

private:
    class PrimaryIndex {
    public:
        void clear() noexcept;
        void reserve(size_t expected_size);
        void put(int64_t key, size_t row_index);
        [[nodiscard]] std::optional<size_t> get(int64_t key) const;

    private:
        struct Bucket {
            int64_t key = 0;
            size_t value = 0;
            bool occupied = false;
        };

        void switch_to_hash_mode();
        void ensure_hash_capacity(size_t expected_size);
        void rehash(size_t new_capacity);
        void hash_put(int64_t key, size_t row_index);
        [[nodiscard]] std::optional<size_t> hash_get(int64_t key) const;

        bool sequential_mode_ = true;
        int64_t seq_base_key_ = 0;
        int64_t seq_last_key_ = 0;
        std::vector<size_t> seq_rows_;

        std::vector<Bucket> buckets_;
        size_t size_ = 0;
    };

    static constexpr size_t kCellSegmentSize = 1U << 16;
    static constexpr size_t kStringSegmentSize = 1U << 16;

    void append_cell(uint64_t value);
    [[nodiscard]] uint64_t raw_cell_at(size_t index) const noexcept;
    bool append_string(std::string_view value, uint64_t& encoded_out, std::string& error);
    bool append_string_bytes(const char* data, size_t len, uint32_t& off_out, std::string& error);
    void append_bytes(std::vector<char>& out, const void* data, size_t len) const;

    bool append_row_to_wal_from_encoded_cells(
        const std::vector<uint64_t>& row_cells,
        const std::vector<std::string_view>& row_strings,
        std::vector<char>& wal_payload,
        std::string& error) const;
    bool append_row_from_wal_locked(const char*& ptr, const char* end, std::string& error);

    bool parse_and_store_value(
        const parser::ValueToken& token,
        parser::ColumnType type,
        uint64_t& encoded_out,
        std::string& error);
    bool insert_values_sql_big_users_fast(
        std::string_view values_sql,
        size_t& inserted_rows,
        std::string& error,
        std::vector<char>* wal_payload);
    bool insert_values_sql_benchmark_fast(
        std::string_view values_sql,
        size_t& inserted_rows,
        std::string& error,
        std::vector<char>* wal_payload);
    void rebuild_primary_index();
    [[nodiscard]] static uint64_t encode_string_ref(uint32_t off, uint32_t len) noexcept;
    [[nodiscard]] static uint32_t decode_string_off(uint64_t raw) noexcept;
    [[nodiscard]] static uint32_t decode_string_len(uint64_t raw) noexcept;
    [[nodiscard]] static uint64_t encode_double(double value) noexcept;
    [[nodiscard]] static double decode_double(uint64_t raw) noexcept;

    std::string name_;
    std::vector<parser::ColumnDef> columns_;
    std::unordered_map<std::string, size_t> col_to_index_;

    std::vector<std::vector<uint64_t>> cell_segments_;
    size_t cell_count_ = 0;

    std::vector<std::vector<char>> string_segments_;
    size_t string_pool_size_ = 0;

    std::optional<size_t> expires_at_idx_;
    PrimaryIndex primary_index_;
};

}  // namespace flexql::storage
