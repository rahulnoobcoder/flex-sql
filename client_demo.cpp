#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <cctype>
#include <filesystem>
#include <algorithm>

#include "flexql.h"

namespace {

std::string trim_copy(const std::string& s) {
    size_t start = 0;
    size_t end = s.size();
    while (start < end && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(start, end - start);
}

std::string to_upper_copy(const std::string& s) {
    std::string out = s;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return out;
}

bool starts_with_icase(const std::string& text, const std::string& prefix) {
    if (text.size() < prefix.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(text[i])) !=
            std::tolower(static_cast<unsigned char>(prefix[i]))) {
            return false;
        }
    }
    return true;
}

std::string normalize_meta_file_path(std::string path) {
    path = trim_copy(path);
    if (!path.empty() && path.back() == ';') {
        path.pop_back();
        path = trim_copy(path);
    }
    if (path.size() >= 2) {
        const char first = path.front();
        const char last = path.back();
        if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
            path = path.substr(1, path.size() - 2);
            path = trim_copy(path);
        }
    }
    return path;
}

std::vector<std::string> split_packed_row(const std::string& packed) {
    std::vector<std::string> cols;
    std::string current;
    bool prev_space = false;

    for (char c : packed) {
        if (c == ' ') {
            if (!current.empty()) {
                cols.push_back(current);
                current.clear();
            }
            prev_space = true;
            continue;
        }
        if (prev_space) {
            prev_space = false;
        }
        current.push_back(c);
    }
    if (!current.empty()) {
        cols.push_back(current);
    }
    return cols;
}

enum class OutputMode {
    Pipe,
    Box
};

struct ClientState {
    std::string current_db = "TEST";
    OutputMode mode = OutputMode::Box;
};

struct QueryCapture {
    std::vector<std::vector<std::string>> rows;
};

int capture_row(void* data, int argc, char** argv, char**) {
    QueryCapture* capture = static_cast<QueryCapture*>(data);
    if (!capture) {
        return 0;
    }

    if (argc == 1 && argv && argv[0]) {
        capture->rows.push_back(split_packed_row(argv[0]));
        return 0;
    }

    std::vector<std::string> row;
    row.reserve(static_cast<size_t>(argc));
    for (int i = 0; i < argc; ++i) {
        row.emplace_back(argv[i] ? argv[i] : "NULL");
    }
    capture->rows.push_back(std::move(row));
    return 0;
}

std::vector<std::string> derive_headers(const std::string& sql, size_t fallback_cols) {
    std::vector<std::string> headers;
    std::string trimmed = trim_copy(sql);
    std::string upper = to_upper_copy(trimmed);

    if (!starts_with_icase(trimmed, "SELECT")) {
        return headers;
    }

    const size_t select_pos = upper.find("SELECT");
    const size_t from_pos = upper.find(" FROM ");
    if (select_pos == std::string::npos || from_pos == std::string::npos || from_pos <= select_pos + 6) {
        return headers;
    }

    std::string projection = trim_copy(trimmed.substr(select_pos + 6, from_pos - (select_pos + 6)));
    if (projection == "*") {
        headers.reserve(fallback_cols);
        for (size_t i = 0; i < fallback_cols; ++i) {
            headers.push_back("col" + std::to_string(i + 1));
        }
        return headers;
    }

    std::stringstream ss(projection);
    std::string token;
    while (std::getline(ss, token, ',')) {
        token = trim_copy(token);
        if (token.empty()) {
            continue;
        }
        size_t dot = token.rfind('.');
        if (dot != std::string::npos && dot + 1 < token.size()) {
            token = token.substr(dot + 1);
        }
        token = trim_copy(token);
        if (!token.empty() && token.front() == '"' && token.back() == '"' && token.size() >= 2) {
            token = token.substr(1, token.size() - 2);
        }
        headers.push_back(token.empty() ? "col" + std::to_string(headers.size() + 1) : token);
    }

    if (headers.size() < fallback_cols) {
        while (headers.size() < fallback_cols) {
            headers.push_back("col" + std::to_string(headers.size() + 1));
        }
    }

    return headers;
}

void print_pipe_rows(const std::vector<std::vector<std::string>>& rows) {
    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i) {
                std::cout << '|';
            }
            std::cout << row[i];
        }
        std::cout << '\n';
    }
}

void print_box_table(const std::vector<std::vector<std::string>>& rows, const std::vector<std::string>& headers) {
    size_t cols = headers.size();
    for (const auto& row : rows) {
        cols = std::max(cols, row.size());
    }
    if (cols == 0) {
        return;
    }

    std::vector<std::string> final_headers = headers;
    if (final_headers.size() < cols) {
        while (final_headers.size() < cols) {
            final_headers.push_back("col" + std::to_string(final_headers.size() + 1));
        }
    }

    std::vector<size_t> widths(cols, 1);
    for (size_t i = 0; i < cols; ++i) {
        widths[i] = final_headers[i].size();
    }
    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            widths[i] = std::max(widths[i], row[i].size());
        }
    }

    auto print_sep = [&]() {
        std::cout << '+';
        for (size_t i = 0; i < cols; ++i) {
            std::cout << std::string(widths[i] + 2, '-') << '+';
        }
        std::cout << '\n';
    };

    auto print_row = [&](const std::vector<std::string>& row) {
        std::cout << '|';
        for (size_t i = 0; i < cols; ++i) {
            const std::string cell = (i < row.size()) ? row[i] : "";
            std::cout << ' ' << cell;
            if (widths[i] > cell.size()) {
                std::cout << std::string(widths[i] - cell.size(), ' ');
            }
            std::cout << '|' ;
        }
        std::cout << '\n';
    };

    print_sep();
    print_row(final_headers);
    print_sep();
    for (const auto& row : rows) {
        print_row(row);
    }
    print_sep();
}

bool extract_used_db_from_sql(const std::string& sql, std::string& out_db) {
    std::string trimmed = trim_copy(sql);
    if (!starts_with_icase(trimmed, "USE")) {
        return false;
    }

    if (trimmed.size() <= 3) {
        return false;
    }
    std::string tail = trim_copy(trimmed.substr(3));
    if (tail.empty()) {
        return false;
    }
    if (!tail.empty() && tail.back() == ';') {
        tail.pop_back();
        tail = trim_copy(tail);
    }
    if (tail.size() >= 2) {
        const char first = tail.front();
        const char last = tail.back();
        if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
            tail = tail.substr(1, tail.size() - 2);
        }
    }
    tail = trim_copy(tail);
    if (tail.empty()) {
        return false;
    }

    out_db = to_upper_copy(tail);
    return true;
}

bool run_sql(FlexQL* db, std::string sql, ClientState& state, bool silent_empty_ok = false) {
    if (trim_copy(sql).empty()) {
        return true;
    }
    if (sql.find(';') == std::string::npos) {
        sql.push_back(';');
    }

    QueryCapture capture;
    char* err = nullptr;
    int rc = flexql_exec(db, sql.c_str(), capture_row, &capture, &err);
    if (rc != FLEXQL_OK) {
        std::cout << "Error: " << (err ? err : "unknown") << '\n';
        if (err) {
            flexql_free(err);
        }
        return false;
    }

    std::string used_db;
    if (extract_used_db_from_sql(sql, used_db)) {
        state.current_db = used_db;
    }

    if (capture.rows.empty()) {
        if (!silent_empty_ok) {
            std::cout << "Query OK\n";
        }
        return true;
    }

    if (state.mode == OutputMode::Pipe) {
        print_pipe_rows(capture.rows);
    } else {
        const size_t col_count = capture.rows.empty() ? 0 : capture.rows.front().size();
        print_box_table(capture.rows, derive_headers(sql, col_count));
    }
    return true;
}

std::vector<std::string> split_sql_statements(const std::string& sql_text) {
    std::vector<std::string> statements;
    std::string current;
    current.reserve(sql_text.size());

    bool in_quote = false;
    for (size_t i = 0; i < sql_text.size(); ++i) {
        char c = sql_text[i];
        if (c == '\'') {
            in_quote = !in_quote;
            current.push_back(c);
            continue;
        }

        if (c == ';' && !in_quote) {
            std::string stmt = trim_copy(current);
            if (!stmt.empty()) {
                stmt.push_back(';');
                statements.push_back(std::move(stmt));
            }
            current.clear();
            continue;
        }

        current.push_back(c);
    }

    std::string tail = trim_copy(current);
    if (!tail.empty()) {
        tail.push_back(';');
        statements.push_back(std::move(tail));
    }

    return statements;
}

bool run_sql_file(FlexQL* db, const std::string& file_path, ClientState& state) {
    std::ifstream in(file_path);
    if (!in) {
        std::cout << "Error: cannot open SQL file: " << file_path << '\n';
        return false;
    }

    std::ostringstream buffer;
    buffer << in.rdbuf();
    const auto statements = split_sql_statements(buffer.str());
    if (statements.empty()) {
        std::cout << "No SQL statements found in: " << file_path << '\n';
        return true;
    }

    for (const auto& stmt : statements) {
        if (!run_sql(db, stmt, state)) {
            return false;
        }
    }
    return true;
}

void print_help() {
    std::cout
        << "FlexQL interactive helpers:\n"
        << "  .help                 Show this help\n"
        << "  .exit / .quit         Exit client\n"
        << "  .source <file.sql>    Execute SQL from file\n"
        << "  .read <file.sql>      Alias of .source\n"
        << "  source <file.sql>     SQLite-style source command\n"
        << "  .db                   Show current database\n"
        << "  .databases            List databases under data/\n"
        << "  .tables               List tables in current database\n"
        << "  .use <dbname>         Switch database (client helper for USE)\n"
        << "  .mode box|pipe        Output mode\n"
        << "\n"
        << "SQL usage:\n"
        << "  - End SQL with ';'\n"
        << "  - Multi-line SQL is supported until ';'\n";
}

void print_databases() {
    std::error_code ec;
    const std::filesystem::path data_path("data");
    if (!std::filesystem::exists(data_path, ec)) {
        std::cout << "(no data directory)\n";
        return;
    }
    for (const auto& entry : std::filesystem::directory_iterator(data_path, ec)) {
        if (entry.is_directory()) {
            std::cout << entry.path().filename().string() << '\n';
        }
    }
}

void print_tables(const ClientState& state) {
    std::error_code ec;
    const std::filesystem::path db_path = std::filesystem::path("data") / state.current_db;
    if (!std::filesystem::exists(db_path, ec)) {
        std::cout << "(database directory missing: " << state.current_db << ")\n";
        return;
    }

    bool found = false;
    for (const auto& entry : std::filesystem::directory_iterator(db_path, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() == ".schema") {
            std::cout << entry.path().stem().string() << '\n';
            found = true;
        }
    }
    if (!found) {
        std::cout << "(no tables)\n";
    }
}

bool try_handle_meta_command(FlexQL* db, const std::string& line, ClientState& state, bool& should_exit) {
    std::string cmd = trim_copy(line);
    if (cmd.empty()) {
        return true;
    }

    if (cmd == ".help") {
        print_help();
        return true;
    }

    if (cmd == ".db") {
        std::cout << state.current_db << '\n';
        return true;
    }

    if (cmd == ".databases") {
        print_databases();
        return true;
    }

    if (cmd == ".tables") {
        print_tables(state);
        return true;
    }

    if (cmd == ".exit" || cmd == ".quit" || cmd == "exit;" || cmd == "EXIT;") {
        should_exit = true;
        return true;
    }

    if (starts_with_icase(cmd, ".mode")) {
        std::string mode = trim_copy(cmd.substr(5));
        mode = to_upper_copy(mode);
        if (mode == "BOX") {
            state.mode = OutputMode::Box;
            std::cout << "mode: box\n";
        } else if (mode == "PIPE") {
            state.mode = OutputMode::Pipe;
            std::cout << "mode: pipe\n";
        } else {
            std::cout << "Error: use .mode box or .mode pipe\n";
        }
        return true;
    }

    if (starts_with_icase(cmd, ".use")) {
        std::string db_name = normalize_meta_file_path(cmd.substr(4));
        if (db_name.empty()) {
            std::cout << "Error: missing database name\n";
            return true;
        }
        run_sql(db, "USE " + db_name + ";", state, true);
        return true;
    }

    auto read_file_cmd = [&](const std::string& prefix) -> bool {
        if (!starts_with_icase(cmd, prefix)) {
            return false;
        }
        std::string path = normalize_meta_file_path(cmd.substr(prefix.size()));
        if (path.empty()) {
            std::cout << "Error: missing file path\n";
            return true;
        }
        run_sql_file(db, path, state);
        return true;
    };

    if (read_file_cmd(".source")) return true;
    if (read_file_cmd(".read")) return true;
    if (read_file_cmd("source")) return true;

    return false;
}

}  // namespace

int main(int argc, char** argv) {
    FlexQL* db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL server\n";
        return 1;
    }

    ClientState state;
    static_cast<void>(run_sql(db, "USE TEST;", state, true));

    if (argc > 1) {
        std::string first_arg = argv[1];
        if (first_arg == "--help" || first_arg == "-h") {
            print_help();
            static_cast<void>(flexql_close(db));
            return 0;
        }

        if (first_arg == "--file" || first_arg == "-f") {
            if (argc < 3) {
                std::cerr << "Missing SQL file path after " << first_arg << "\n";
                static_cast<void>(flexql_close(db));
                return 1;
            }
            bool ok = run_sql_file(db, argv[2], state);
            static_cast<void>(flexql_close(db));
            return ok ? 0 : 1;
        }

        std::string sql;
        for (int i = 1; i < argc; ++i) {
            if (i > 1) {
                sql.push_back(' ');
            }
            sql += argv[i];
        }
        bool ok = run_sql(db, sql, state);
        static_cast<void>(flexql_close(db));
        return ok ? 0 : 1;
    }

    std::cout << "FlexQL interactive client\n";
    std::cout << "Current database: " << state.current_db << "\n";
    std::cout << "Enter \".help\" for usage hints.\n";
    std::cout << "sqlite> ";

    std::string line;
    std::string statement;
    bool should_exit = false;

    while (std::getline(std::cin, line)) {
        if (statement.empty()) {
            if (try_handle_meta_command(db, line, state, should_exit)) {
                if (should_exit) {
                    break;
                }
                std::string trimmed_line = trim_copy(line);
                if (trimmed_line.empty() || trimmed_line[0] == '.' || starts_with_icase(trimmed_line, "source")) {
                    std::cout << "sqlite> ";
                    continue;
                }
            }
        }

        statement += line;
        statement.push_back('\n');

        if (statement.find(';') == std::string::npos) {
            std::cout << "   ...> ";
            continue;
        }

        std::string trimmed = trim_copy(statement);
        if (trimmed == "exit;" || trimmed == "EXIT;" || trimmed == ".exit" || trimmed == ".quit") {
            break;
        }

        run_sql(db, statement, state);
        statement.clear();
        std::cout << "sqlite> ";
    }

    static_cast<void>(flexql_close(db));
    return 0;
}
