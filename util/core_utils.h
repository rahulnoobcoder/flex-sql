#pragma once

#include <string>
#include <string_view>

namespace flexql::util {

void init_logger(const std::string& log_path);
void log_info(const std::string& message);
void log_error(const std::string& message);

[[nodiscard]] std::string to_upper(std::string_view input);
[[nodiscard]] std::string trim_copy(std::string_view input);
[[nodiscard]] bool iequals(std::string_view a, std::string_view b);

}  // namespace flexql::util
