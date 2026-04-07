#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace flexql::parser {

enum class ColumnType {
    Int,
    Decimal,
    Varchar,
    Datetime
};

struct ColumnDef {
    std::string name;
    ColumnType type;
};

struct ValueToken {
    std::string_view text;
    bool quoted = false;
};

struct QualifiedName {
    std::string table;
    std::string column;
};

enum class Operator {
    Eq,
    Gt,
    Lt,
    Ge,
    Le
};

struct Condition {
    QualifiedName lhs;
    Operator op;
    ValueToken rhs;
};

struct OrderBy {
    QualifiedName key;
    bool desc = false;
};

struct JoinClause {
    std::string right_table;
    QualifiedName left_key;
    QualifiedName right_key;
};

struct CreateTableStatement {
    bool if_not_exists = false;
    std::string table;
    std::vector<ColumnDef> columns;
};

struct CreateDatabaseStatement {
    std::string database;
};

struct DropDatabaseStatement {
    std::string database;
};

struct UseDatabaseStatement {
    std::string database;
};

struct DropTableStatement {
    std::string table;
};

struct DeleteStatement {
    std::string table;
};

struct InsertStatement {
    std::string table;
    std::vector<std::vector<ValueToken>> rows;
};

struct SelectStatement {
    bool select_all = false;
    std::vector<QualifiedName> projections;
    std::string from_table;
    std::optional<JoinClause> join;
    std::optional<Condition> where;
    std::optional<OrderBy> order_by;
};

using Statement = std::variant<
    CreateTableStatement,
    CreateDatabaseStatement,
    DropDatabaseStatement,
    UseDatabaseStatement,
    DropTableStatement,
    DeleteStatement,
    InsertStatement,
    SelectStatement>;

}  // namespace flexql::parser
