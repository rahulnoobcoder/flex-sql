#pragma once

#include <string>
#include <vector>

namespace flexql::engine {

struct ExecutionResult {
    bool ok = false;
    std::string error;
    std::vector<std::vector<std::string>> rows;
};

class Executor {
public:
    Executor();
    ~Executor();
    Executor(const Executor&) = delete;
    Executor& operator=(const Executor&) = delete;
    Executor(Executor&&) = delete;
    Executor& operator=(Executor&&) = delete;
    [[nodiscard]] ExecutionResult execute(const std::string& sql);

private:
    struct Impl;
    Impl* impl_;
};

}  // namespace flexql::engine
