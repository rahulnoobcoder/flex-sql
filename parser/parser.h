#pragma once

#include <string>

#include "parser/ast.h"

namespace flexql::parser {

class SQLParser {
public:
    bool parse(const std::string& sql, Statement& out, std::string& error) const;
};

}  // namespace flexql::parser
