#include "parser/parser.h"

#include <cctype>
#include <string>
#include <string_view>
#include <vector>

#include "util/core_utils.h"

namespace flexql::parser {
namespace {

constexpr size_t kMaxTokenLength = 4096;
constexpr size_t kMaxTokens = 200000;

struct Token {
    std::string_view text;
    bool quoted = false;
};

bool tokenize(std::string_view sql, std::vector<Token>& tokens, std::string& error) {
    tokens.clear();
    tokens.reserve(sql.size() / 3 + 1);

    size_t i = 0;
    while (i < sql.size()) {
        const char c = sql[i];
        if (std::isspace(static_cast<unsigned char>(c))) {
            ++i;
            continue;
        }

        if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
            i += 2;
            while (i < sql.size() && sql[i] != '\n') {
                ++i;
            }
            continue;
        }

        if (c == '#') {
            ++i;
            while (i < sql.size() && sql[i] != '\n') {
                ++i;
            }
            continue;
        }

        if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
            i += 2;
            bool closed = false;
            while (i + 1 < sql.size()) {
                if (sql[i] == '*' && sql[i + 1] == '/') {
                    i += 2;
                    closed = true;
                    break;
                }
                ++i;
            }
            if (!closed) {
                error = "unterminated block comment";
                return false;
            }
            continue;
        }

        if (c == '\'') {
            const size_t start = i + 1;
            ++i;
            while (i < sql.size() && sql[i] != '\'') {
                ++i;
            }
            if (i >= sql.size()) {
                error = "unterminated string literal";
                return false;
            }
            const size_t end = i;
            if ((end - start) > kMaxTokenLength) {
                error = "string literal too long";
                return false;
            }
            ++i;
            tokens.push_back({sql.substr(start, end - start), true});
            if (tokens.size() > kMaxTokens) {
                error = "too many SQL tokens";
                return false;
            }
            continue;
        }

        if (c == '(' || c == ')' || c == ',' || c == ';' || c == '.') {
            tokens.push_back({sql.substr(i, 1), false});
            ++i;
            if (tokens.size() > kMaxTokens) {
                error = "too many SQL tokens";
                return false;
            }
            continue;
        }

        if (c == '>' || c == '<' || c == '=') {
            if (i + 1 < sql.size() && sql[i + 1] == '=') {
                tokens.push_back({sql.substr(i, 2), false});
                i += 2;
            } else {
                tokens.push_back({sql.substr(i, 1), false});
                ++i;
            }
            if (tokens.size() > kMaxTokens) {
                error = "too many SQL tokens";
                return false;
            }
            continue;
        }

        const size_t start = i;
        while (i < sql.size()) {
            const char k = sql[i];
            if (std::isspace(static_cast<unsigned char>(k)) || k == '(' || k == ')' || k == ',' || k == ';' ||
                k == '\'' || k == '>' || k == '<' || k == '=' || k == '.') {
                break;
            }
            ++i;
        }
        if ((i - start) > kMaxTokenLength) {
            error = "token too long";
            return false;
        }
        tokens.push_back({sql.substr(start, i - start), false});
        if (tokens.size() > kMaxTokens) {
            error = "too many SQL tokens";
            return false;
        }
    }

    return true;
}

bool is_keyword(const Token& token, std::string_view keyword) {
    return util::iequals(token.text, keyword);
}

bool parse_identifier(const Token& token, std::string& out) {
    if (token.text.empty() || token.quoted || token.text == ";" || token.text == "," ||
        token.text == "(" || token.text == ")" || token.text == ".") {
        return false;
    }

    std::string_view raw = token.text;
    if (raw.size() >= 2) {
        const char first = raw.front();
        const char last = raw.back();
        if ((first == '`' && last == '`') || (first == '"' && last == '"')) {
            raw = raw.substr(1, raw.size() - 2);
        }
    }

    if (raw.empty()) {
        return false;
    }

    out = util::to_upper(raw);
    return true;
}

bool parse_qualified(const std::vector<Token>& tokens, size_t& pos, QualifiedName& out) {
    if (pos >= tokens.size()) {
        return false;
    }

    if (tokens[pos].text.empty() || tokens[pos].quoted || tokens[pos].text == ";" || tokens[pos].text == "," ||
        tokens[pos].text == "(" || tokens[pos].text == ")" || tokens[pos].text == ".") {
        return false;
    }

    std::string first;
    if (!parse_identifier(tokens[pos], first)) {
        return false;
    }
    ++pos;

    if (pos + 1 < tokens.size() && tokens[pos].text == ".") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos].quoted || tokens[pos].text.empty()) {
            return false;
        }
        out.table = first;
        if (!parse_identifier(tokens[pos], out.column)) {
            return false;
        }
        ++pos;
    } else {
        out.table.clear();
        out.column = first;
    }

    return true;
}

std::optional<ColumnType> parse_column_type(const std::vector<Token>& tokens, size_t& pos) {
    if (pos >= tokens.size()) {
        return std::nullopt;
    }
    if (is_keyword(tokens[pos], "INT")) {
        ++pos;
        return ColumnType::Int;
    }
    if (is_keyword(tokens[pos], "DECIMAL")) {
        ++pos;
        return ColumnType::Decimal;
    }
    if (is_keyword(tokens[pos], "DATETIME")) {
        ++pos;
        return ColumnType::Datetime;
    }
    if (is_keyword(tokens[pos], "VARCHAR")) {
        ++pos;
        if (pos < tokens.size() && tokens[pos].text == "(") {
            ++pos;
            if (pos < tokens.size()) {
                ++pos;
            }
            if (pos < tokens.size() && tokens[pos].text == ")") {
                ++pos;
            }
        }
        return ColumnType::Varchar;
    }
    return std::nullopt;
}

bool parse_create(const std::vector<Token>& tokens, Statement& out, std::string& error) {
    size_t pos = 0;
    if (tokens.size() < 3 || !is_keyword(tokens[pos], "CREATE")) {
        error = "invalid CREATE syntax";
        return false;
    }
    ++pos;
    if (pos >= tokens.size()) {
        error = "CREATE target missing";
        return false;
    }

    if (is_keyword(tokens[pos], "DATABASE")) {
        ++pos;
        if (pos >= tokens.size() || tokens[pos].quoted || tokens[pos].text == ";") {
            error = "database name missing";
            return false;
        }
        CreateDatabaseStatement stmt;
        if (!parse_identifier(tokens[pos], stmt.database)) {
            error = "database name missing";
            return false;
        }
        ++pos;
        if (pos < tokens.size() && tokens[pos].text == ";") {
            ++pos;
        }
        if (pos != tokens.size()) {
            error = "unexpected tokens after CREATE DATABASE";
            return false;
        }
        out = std::move(stmt);
        return true;
    }

    if (!is_keyword(tokens[pos], "TABLE")) {
        error = "CREATE TABLE or CREATE DATABASE expected";
        return false;
    }
    ++pos;

    CreateTableStatement stmt;
    if (pos + 2 < tokens.size() && is_keyword(tokens[pos], "IF") && is_keyword(tokens[pos + 1], "NOT") &&
        is_keyword(tokens[pos + 2], "EXISTS")) {
        stmt.if_not_exists = true;
        pos += 3;
    }

    if (pos >= tokens.size()) {
        error = "table name missing";
        return false;
    }
    if (!parse_identifier(tokens[pos], stmt.table)) {
        error = "table name missing";
        return false;
    }
    ++pos;

    if (pos >= tokens.size() || tokens[pos].text != "(") {
        error = "column list expected";
        return false;
    }
    ++pos;

    bool closed = false;
    while (pos < tokens.size()) {
        if (tokens[pos].text == ")") {
            ++pos;
            closed = true;
            break;
        }

        if (tokens[pos].quoted || tokens[pos].text == ";") {
            error = "invalid column definition";
            return false;
        }

        ColumnDef col;
        if (!parse_identifier(tokens[pos], col.name)) {
            error = "invalid column name";
            return false;
        }
        ++pos;

        auto type = parse_column_type(tokens, pos);
        if (!type.has_value()) {
            error = "unsupported column type";
            return false;
        }
        col.type = *type;
        stmt.columns.push_back(std::move(col));

        if (pos < tokens.size() && tokens[pos].text == ",") {
            ++pos;
        }
    }

    if (!closed) {
        error = "unterminated column list";
        return false;
    }

    if (pos < tokens.size() && tokens[pos].text == ";") {
        ++pos;
    }
    if (pos != tokens.size()) {
        error = "unexpected tokens after CREATE TABLE";
        return false;
    }

    if (stmt.columns.empty()) {
        error = "no columns defined";
        return false;
    }

    out = std::move(stmt);
    return true;
}

bool parse_drop(const std::vector<Token>& tokens, Statement& out, std::string& error) {
    if (tokens.size() < 3 || !is_keyword(tokens[0], "DROP")) {
        error = "invalid DROP syntax";
        return false;
    }

    if (is_keyword(tokens[1], "DATABASE")) {
        if (tokens[2].quoted || tokens[2].text == ";") {
            error = "database name missing";
            return false;
        }
        DropDatabaseStatement stmt;
        if (!parse_identifier(tokens[2], stmt.database)) {
            error = "database name missing";
            return false;
        }

        size_t pos = 3;
        if (pos < tokens.size() && tokens[pos].text == ";") {
            ++pos;
        }
        if (pos != tokens.size()) {
            error = "unexpected tokens after DROP DATABASE";
            return false;
        }
        out = std::move(stmt);
        return true;
    }

    if (is_keyword(tokens[1], "TABLE")) {
        if (tokens[2].quoted || tokens[2].text == ";") {
            error = "table name missing";
            return false;
        }
        DropTableStatement stmt;
        if (!parse_identifier(tokens[2], stmt.table)) {
            error = "table name missing";
            return false;
        }

        size_t pos = 3;
        if (pos < tokens.size() && tokens[pos].text == ";") {
            ++pos;
        }
        if (pos != tokens.size()) {
            error = "unexpected tokens after DROP TABLE";
            return false;
        }
        out = std::move(stmt);
        return true;
    }

    error = "DROP TABLE or DROP DATABASE expected";
    return false;
}

bool parse_use(const std::vector<Token>& tokens, Statement& out, std::string& error) {
    if (tokens.size() < 2 || !is_keyword(tokens[0], "USE")) {
        error = "invalid USE syntax";
        return false;
    }
    if (tokens[1].quoted || tokens[1].text == ";") {
        error = "database name missing";
        return false;
    }

    UseDatabaseStatement stmt;
    if (!parse_identifier(tokens[1], stmt.database)) {
        error = "database name missing";
        return false;
    }

    size_t pos = 2;
    if (pos < tokens.size() && tokens[pos].text == ";") {
        ++pos;
    }
    if (pos != tokens.size()) {
        error = "unexpected tokens after USE";
        return false;
    }

    out = std::move(stmt);
    return true;
}

bool parse_delete(const std::vector<Token>& tokens, Statement& out, std::string& error) {
    if (tokens.size() < 3 || !is_keyword(tokens[0], "DELETE") || !is_keyword(tokens[1], "FROM")) {
        error = "invalid DELETE syntax";
        return false;
    }

    DeleteStatement stmt;
    if (tokens[2].quoted || tokens[2].text == ";") {
        error = "table name missing";
        return false;
    }
    if (!parse_identifier(tokens[2], stmt.table)) {
        error = "table name missing";
        return false;
    }

    size_t pos = 3;
    if (pos < tokens.size() && tokens[pos].text == ";") {
        ++pos;
    }
    if (pos != tokens.size()) {
        error = "unexpected tokens after DELETE";
        return false;
    }

    out = std::move(stmt);
    return true;
}

bool parse_insert(const std::vector<Token>& tokens, Statement& out, std::string& error) {
    size_t pos = 0;
    if (tokens.size() < 6 || !is_keyword(tokens[pos], "INSERT")) {
        error = "invalid INSERT syntax";
        return false;
    }
    ++pos;
    if (pos >= tokens.size() || !is_keyword(tokens[pos], "INTO")) {
        error = "INSERT INTO expected";
        return false;
    }
    ++pos;

    if (pos >= tokens.size()) {
        error = "table name missing";
        return false;
    }

    InsertStatement stmt;
    if (!parse_identifier(tokens[pos], stmt.table)) {
        error = "table name missing";
        return false;
    }
    ++pos;

    if (pos >= tokens.size() || !is_keyword(tokens[pos], "VALUES")) {
        error = "VALUES expected";
        return false;
    }
    ++pos;

    size_t estimated_rows = 0;
    for (size_t i = pos; i < tokens.size(); ++i) {
        if (tokens[i].text == "(") {
            ++estimated_rows;
        }
    }
    if (estimated_rows > 0) {
        stmt.rows.reserve(estimated_rows);
    }

    size_t estimated_cols = 0;
    size_t probe = pos;
    if (probe < tokens.size() && tokens[probe].text == "(") {
        ++probe;
        while (probe < tokens.size() && tokens[probe].text != ")") {
            if (tokens[probe].text != ",") {
                ++estimated_cols;
            }
            ++probe;
        }
    }

    while (pos < tokens.size()) {
        if (tokens[pos].text == ";") {
            break;
        }
        if (tokens[pos].text != "(") {
            error = "row tuple expected";
            return false;
        }
        ++pos;

        std::vector<ValueToken> row;
        if (estimated_cols > 0) {
            row.reserve(estimated_cols);
        }
        while (pos < tokens.size() && tokens[pos].text != ")") {
            if (tokens[pos].text == ",") {
                ++pos;
                continue;
            }
            row.push_back({tokens[pos].text, tokens[pos].quoted});
            ++pos;
        }

        if (pos >= tokens.size() || tokens[pos].text != ")") {
            error = "unterminated row tuple";
            return false;
        }
        ++pos;
        if (row.empty()) {
            error = "empty row tuple";
            return false;
        }
        stmt.rows.push_back(std::move(row));

        if (pos < tokens.size() && tokens[pos].text == ",") {
            ++pos;
        }
    }

    if (stmt.rows.empty()) {
        error = "no rows provided";
        return false;
    }

    if (pos < tokens.size() && tokens[pos].text == ";") {
        ++pos;
    }
    if (pos != tokens.size()) {
        error = "unexpected tokens after INSERT";
        return false;
    }

    out = std::move(stmt);
    return true;
}

std::optional<Operator> parse_operator(const Token& token) {
    if (token.text == "=") return Operator::Eq;
    if (token.text == ">") return Operator::Gt;
    if (token.text == "<") return Operator::Lt;
    if (token.text == ">=") return Operator::Ge;
    if (token.text == "<=") return Operator::Le;
    return std::nullopt;
}

bool parse_select(const std::vector<Token>& tokens, Statement& out, std::string& error) {
    size_t pos = 0;
    if (tokens.empty() || !is_keyword(tokens[pos], "SELECT")) {
        error = "invalid SELECT syntax";
        return false;
    }
    ++pos;

    SelectStatement stmt;
    if (pos < tokens.size() && tokens[pos].text == "*") {
        stmt.select_all = true;
        ++pos;
    } else {
        while (pos < tokens.size()) {
            QualifiedName qn;
            if (!parse_qualified(tokens, pos, qn)) {
                error = "invalid projection";
                return false;
            }
            stmt.projections.push_back(std::move(qn));

            if (pos < tokens.size() && tokens[pos].text == ",") {
                ++pos;
                continue;
            }
            break;
        }
    }

    if (pos >= tokens.size() || !is_keyword(tokens[pos], "FROM")) {
        error = "FROM expected";
        return false;
    }
    ++pos;

    if (pos >= tokens.size()) {
        error = "table name after FROM expected";
        return false;
    }
    if (!parse_identifier(tokens[pos], stmt.from_table)) {
        error = "table name after FROM expected";
        return false;
    }
    ++pos;

    if (pos + 6 < tokens.size() && is_keyword(tokens[pos], "INNER") && is_keyword(tokens[pos + 1], "JOIN")) {
        JoinClause join;
        if (!parse_identifier(tokens[pos + 2], join.right_table)) {
            error = "invalid JOIN table";
            return false;
        }
        pos += 3;

        if (pos >= tokens.size() || !is_keyword(tokens[pos], "ON")) {
            error = "ON expected for JOIN";
            return false;
        }
        ++pos;

        if (!parse_qualified(tokens, pos, join.left_key)) {
            error = "invalid join left key";
            return false;
        }

        if (pos >= tokens.size() || tokens[pos].text != "=") {
            error = "join must use equality";
            return false;
        }
        ++pos;

        if (!parse_qualified(tokens, pos, join.right_key)) {
            error = "invalid join right key";
            return false;
        }

        stmt.join = std::move(join);
    }

    if (pos + 3 < tokens.size() && is_keyword(tokens[pos], "WHERE")) {
        ++pos;
        Condition cond;
        if (!parse_qualified(tokens, pos, cond.lhs)) {
            error = "invalid WHERE column";
            return false;
        }

        if (pos >= tokens.size()) {
            error = "WHERE operator missing";
            return false;
        }
        auto op = parse_operator(tokens[pos]);
        if (!op.has_value()) {
            error = "unsupported WHERE operator";
            return false;
        }
        cond.op = *op;
        ++pos;

        if (pos >= tokens.size()) {
            error = "WHERE literal missing";
            return false;
        }
        cond.rhs = {tokens[pos].text, tokens[pos].quoted};
        ++pos;

        stmt.where = std::move(cond);
    }

    if (pos + 2 < tokens.size() && is_keyword(tokens[pos], "ORDER") && is_keyword(tokens[pos + 1], "BY")) {
        pos += 2;
        OrderBy order;
        if (!parse_qualified(tokens, pos, order.key)) {
            error = "invalid ORDER BY key";
            return false;
        }
        if (pos < tokens.size() && is_keyword(tokens[pos], "DESC")) {
            order.desc = true;
            ++pos;
        } else if (pos < tokens.size() && is_keyword(tokens[pos], "ASC")) {
            ++pos;
        }
        stmt.order_by = std::move(order);
    }

    if (pos < tokens.size() && tokens[pos].text == ";") {
        ++pos;
    }
    if (pos != tokens.size()) {
        error = "unexpected tokens after SELECT";
        return false;
    }

    out = std::move(stmt);
    return true;
}

}  // namespace

bool SQLParser::parse(const std::string& sql, Statement& out, std::string& error) const {
    std::string_view view(sql);
    size_t start = 0;
    size_t end = view.size();
    while (start < end && std::isspace(static_cast<unsigned char>(view[start]))) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(view[end - 1]))) {
        --end;
    }

    view = view.substr(start, end - start);
    if (view.empty()) {
        error = "empty sql";
        return false;
    }

    std::vector<Token> tokens;
    if (!tokenize(view, tokens, error)) {
        return false;
    }
    if (tokens.empty()) {
        error = "empty sql";
        return false;
    }

    size_t begin = 0;
    size_t end_idx = tokens.size();
    while (begin < end_idx && tokens[begin].text == ";") {
        ++begin;
    }
    while (end_idx > begin && tokens[end_idx - 1].text == ";") {
        --end_idx;
    }

    if (begin == end_idx) {
        error = "empty sql";
        return false;
    }

    std::vector<Token> normalized;
    normalized.reserve(end_idx - begin + 1);
    for (size_t i = begin; i < end_idx; ++i) {
        if (tokens[i].text == ";") {
            error = "multiple SQL statements are not allowed";
            return false;
        }
        normalized.push_back(tokens[i]);
    }

    normalized.push_back({";", false});
    tokens.swap(normalized);

    if (is_keyword(tokens[0], "CREATE")) {
        return parse_create(tokens, out, error);
    }
    if (is_keyword(tokens[0], "DROP")) {
        return parse_drop(tokens, out, error);
    }
    if (is_keyword(tokens[0], "USE")) {
        return parse_use(tokens, out, error);
    }
    if (is_keyword(tokens[0], "DELETE")) {
        return parse_delete(tokens, out, error);
    }
    if (is_keyword(tokens[0], "INSERT")) {
        return parse_insert(tokens, out, error);
    }
    if (is_keyword(tokens[0], "SELECT")) {
        return parse_select(tokens, out, error);
    }

    error = "unsupported SQL statement";
    return false;
}

}  // namespace flexql::parser
