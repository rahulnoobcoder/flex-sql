CXX ?= g++
CXXFLAGS ?= -O3 -std=c++17 -pthread -Wall -Wextra -Wno-unused-parameter

# Source files for the server
SERVER_SRCS = \
	flexql_server.cpp \
	engine/executor.cpp \
	parser/parser.cpp \
	storage/table.cpp \
	util/core_utils.cpp

# Output binary names
SERVER_BIN = server
BENCHMARK_BIN = benchmark
RIGOROUS_BENCHMARK_BIN = benchmark_rigorous
CLIENT_BIN = client_demo

.PHONY: all clean

all: $(SERVER_BIN) $(BENCHMARK_BIN) $(RIGOROUS_BENCHMARK_BIN) $(CLIENT_BIN)

$(SERVER_BIN): $(SERVER_SRCS)
	$(CXX) $(CXXFLAGS) -I. $(SERVER_SRCS) -o $(SERVER_BIN)

$(BENCHMARK_BIN): flexql.cpp benchmark_flexql.cpp
	$(CXX) $(CXXFLAGS) -I. flexql.cpp benchmark_flexql.cpp -o $(BENCHMARK_BIN)

$(RIGOROUS_BENCHMARK_BIN): flexql.cpp benchmark_rigorous.cpp
	$(CXX) $(CXXFLAGS) -I. flexql.cpp benchmark_rigorous.cpp -o $(RIGOROUS_BENCHMARK_BIN)

$(CLIENT_BIN): flexql.cpp client_demo.cpp
	$(CXX) $(CXXFLAGS) -I. flexql.cpp client_demo.cpp -o $(CLIENT_BIN)

clean:
	rm -f $(SERVER_BIN) $(BENCHMARK_BIN) $(RIGOROUS_BENCHMARK_BIN) $(CLIENT_BIN)
	rm -rf data logs
