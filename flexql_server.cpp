#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/tcp.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <csignal>
#include <string>
#include <thread>
#include <vector>

#include "engine/executor.h"
#include "util/core_utils.h"

namespace {

constexpr int kPort = 9000;
constexpr size_t kBufferSize = 8 * 1024 * 1024;
constexpr size_t kMaxPendingBytes = 16 * 1024 * 1024;

std::atomic<uint64_t> g_client_id{0};
flexql::engine::Executor g_executor;

bool send_all(int sock, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

void append_result_payload(const flexql::engine::ExecutionResult& result, std::string& out) {
    if (!result.ok) {
        out.append("ERROR: ");
        out.append(result.error);
        out.push_back('\n');
        out.append("END\n");
        return;
    }

    for (const auto& row : result.rows) {
        out.append("ROW ");
        out.append(std::to_string(row.size()));
        out.push_back(' ');
        for (size_t i = 0; i < row.size(); ++i) {
            // New format: <len_name>:<name><len_val>:<value>
            // We'll use empty column names (length 0) since executor result doesn't provide them
            out.append("0:");
            out.append(std::to_string(row[i].size()));
            out.push_back(':');
            out.append(row[i]);
        }
        out.push_back('\n');
    }
    out.append("END\n");
}

void handle_client(int client_socket, uint64_t client_id) {
    flexql::util::log_info("client connected id=" + std::to_string(client_id));

    std::string pending;
    pending.reserve(kBufferSize);
    std::vector<char> buffer(kBufferSize);

    while (true) {
        ssize_t bytes = read(client_socket, buffer.data(), buffer.size());
        if (bytes < 0 && errno == EINTR) {
            continue;
        }
        if (bytes <= 0) {
            break;
        }

        pending.append(buffer.data(), static_cast<size_t>(bytes));
        if (pending.size() > kMaxPendingBytes) {
            static const char kPayloadTooLarge[] = "ERROR: SQL payload too large\nEND\n";
            send_all(client_socket, kPayloadTooLarge, sizeof(kPayloadTooLarge) - 1);
            break;
        }

        size_t read_pos = 0;
        size_t semicolon_pos = std::string::npos;
        while ((semicolon_pos = pending.find(';', read_pos)) != std::string::npos) {
            std::string sql = flexql::util::trim_copy(std::string_view(pending.data() + read_pos, semicolon_pos - read_pos + 1));
            read_pos = semicolon_pos + 1;

            if (sql.empty() || sql == ";") {
                continue;
            }

            auto result = g_executor.execute(sql);
            std::string payload;
            payload.reserve(128 + result.rows.size() * 32);
            append_result_payload(result, payload);

            if (!send_all(client_socket, payload.data(), payload.size())) {
                flexql::util::log_error("send failed id=" + std::to_string(client_id));
                close(client_socket);
                return;
            }
        }
        pending.erase(0, read_pos);
    }

    close(client_socket);
    flexql::util::log_info("client disconnected id=" + std::to_string(client_id));
}

}  // namespace

int main() {
    std::signal(SIGPIPE, SIG_IGN);
    flexql::util::init_logger("logs/flexql.log");

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket() failed\n";
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    int buf_size = 4 * 1024 * 1024; // 4MB
    setsockopt(server_fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    setsockopt(server_fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
#if defined(SO_REUSEPORT)
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(kPort);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        std::cerr << "bind() failed: " << std::strerror(errno) << "\n";
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 128) < 0) {
        std::cerr << "listen() failed\n";
        close(server_fd);
        return 1;
    }

    std::cout << "FlexQL Server running on port " << kPort << std::endl;
    flexql::util::log_info("server started on port " + std::to_string(kPort));

    while (true) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_socket = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
        if (client_socket < 0) {
            if (errno == EINTR) {
                continue;
            }
            continue;
        }

        uint64_t client_id = g_client_id.fetch_add(1) + 1;
        std::thread(handle_client, client_socket, client_id).detach();
    }

    close(server_fd);
    return 0;
}
