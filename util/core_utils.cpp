#include "util/core_utils.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>

namespace flexql::util {
namespace {
std::mutex g_log_mutex;

std::string now_string() {
    using clock = std::chrono::system_clock;
    auto now = clock::now();
    std::time_t t = clock::to_time_t(now);
    std::tm tmv{};
#if defined(_WIN32)
    localtime_s(&tmv, &t);
#else
    localtime_r(&t, &tmv);
#endif
    std::ostringstream out;
    out << std::put_time(&tmv, "%Y-%m-%d %H:%M:%S");
    return out.str();
}

static std::ofstream& get_log_file() {
    static std::ofstream file;
    return file;
}

std::string& get_global_log_path() {
    static std::string path = "logs/flexql.log";
    return path;
}

void write_line(const char* level, const std::string& message) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    auto& file = get_log_file();
    if (!file.is_open()) { 
        file.open(get_global_log_path(), std::ios::app); 
    }
    if (!file) {
        return;
    }
    file << '[' << now_string() << "] [" << level << "] " << message << '\n';
    file.flush();
}

}  // namespace

void init_logger(const std::string& log_path) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    get_global_log_path() = log_path;
    std::filesystem::path path(log_path);
    if (path.has_parent_path()) {
        std::filesystem::create_directories(path.parent_path());
    }
    auto& file = get_log_file();
    if (file.is_open()) {
        file.close();
    }
    file.open(get_global_log_path(), std::ios::app);
    if (file) {
        file << '[' << now_string() << "] [INFO] logger initialized\n";
    }
}

void log_info(const std::string& message) {
    write_line("INFO", message);
}

void log_error(const std::string& message) {
    write_line("ERROR", message);
}

std::string to_upper(std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (char c : input) {
        output.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    return output;
}

std::string trim_copy(std::string_view input) {
    size_t start = 0;
    size_t end = input.size();
    while (start < end && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    return std::string(input.substr(start, end - start));
}

bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        const unsigned char ac = static_cast<unsigned char>(a[i]);
        const unsigned char bc = static_cast<unsigned char>(b[i]);
        if (std::toupper(ac) != std::toupper(bc)) {
            return false;
        }
    }
    return true;
}

}  // namespace flexql::util
