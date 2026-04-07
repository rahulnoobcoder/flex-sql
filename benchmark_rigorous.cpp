#include <algorithm>
#include <chrono>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

#include "flexql.h"

using namespace std;
using namespace std::chrono;

static const long long DEFAULT_INSERT_ROWS = 2000000LL;
static const int INSERT_BATCH_SIZE = 20000;
static const int DEFAULT_QUERY_REPEATS = 5;

struct QueryStats {
    long long rows = 0;
};

struct TimedQueryResult {
    bool ok = false;
    long long elapsed_ms = 0;
    long long rows = 0;
    string error;
};

struct RepeatedTiming {
    bool ok = false;
    long long cold_ms = 0;
    long long warm_avg_ms = 0;
    long long warm_min_ms = 0;
    long long warm_max_ms = 0;
    long long rows = 0;
    string error;
};

static int count_rows_callback(void* data, int argc, char** argv, char** column_names) {
    (void)argc;
    (void)argv;
    (void)column_names;
    QueryStats* stats = static_cast<QueryStats*>(data);
    if (stats != nullptr) {
        stats->rows++;
    }
    return 0;
}

static bool run_exec(FlexQL* db, const string& sql, const string& label, long long* elapsed_ms_out = nullptr) {
    char* error_message = nullptr;
    auto start = high_resolution_clock::now();
    int status = flexql_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
    auto end = high_resolution_clock::now();
    long long elapsed_ms = duration_cast<milliseconds>(end - start).count();

    if (elapsed_ms_out != nullptr) {
        *elapsed_ms_out = elapsed_ms;
    }

    if (status != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (error_message ? error_message : "unknown error") << "\n";
        if (error_message != nullptr) {
            flexql_free(error_message);
        }
        return false;
    }

    cout << "[PASS] " << label << " (" << elapsed_ms << " ms)\n";
    return true;
}

static TimedQueryResult run_timed_query(FlexQL* db, const string& sql) {
    QueryStats stats;
    char* error_message = nullptr;

    auto start = high_resolution_clock::now();
    int status = flexql_exec(db, sql.c_str(), count_rows_callback, &stats, &error_message);
    auto end = high_resolution_clock::now();

    TimedQueryResult result;
    result.elapsed_ms = duration_cast<milliseconds>(end - start).count();
    result.rows = stats.rows;

    if (status != FLEXQL_OK) {
        result.ok = false;
        result.error = (error_message ? error_message : "unknown error");
        if (error_message != nullptr) {
            flexql_free(error_message);
        }
        return result;
    }

    result.ok = true;
    return result;
}

static RepeatedTiming run_repeated_query(FlexQL* db, const string& sql, int repeats) {
    RepeatedTiming summary;
    repeats = max(1, repeats);

    vector<long long> timings;
    timings.reserve(static_cast<size_t>(repeats));

    for (int run_index = 0; run_index < repeats; ++run_index) {
        TimedQueryResult run_result = run_timed_query(db, sql);
        if (!run_result.ok) {
            summary.ok = false;
            summary.error = run_result.error;
            return summary;
        }
        timings.push_back(run_result.elapsed_ms);
        summary.rows = run_result.rows;
    }

    summary.ok = true;
    summary.cold_ms = timings.front();

    if (timings.size() == 1) {
        summary.warm_avg_ms = timings.front();
        summary.warm_min_ms = timings.front();
        summary.warm_max_ms = timings.front();
        return summary;
    }

    vector<long long> warm_timings(timings.begin() + 1, timings.end());
    summary.warm_min_ms = *min_element(warm_timings.begin(), warm_timings.end());
    summary.warm_max_ms = *max_element(warm_timings.begin(), warm_timings.end());
    long long warm_total = accumulate(warm_timings.begin(), warm_timings.end(), 0LL);
    summary.warm_avg_ms = warm_total / static_cast<long long>(warm_timings.size());

    return summary;
}

static bool create_benchmark_tables(FlexQL* db) {
    if (!run_exec(
            db,
            "CREATE TABLE BIG_USERS_RIG(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_USERS_RIG")) {
        return false;
    }

    if (!run_exec(
            db,
            "CREATE TABLE BIG_ORDERS_RIG(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_ORDERS_RIG")) {
        return false;
    }

    return true;
}

static bool insert_big_users(FlexQL* db, long long total_rows, long long& elapsed_ms_out) {
    cout << "\n[INSERT] BIG_USERS_RIG rows=" << total_rows << "\n";

    long long inserted_rows = 0;
    long long progress_step = max(1LL, total_rows / 10LL);
    long long next_progress = progress_step;

    auto start = high_resolution_clock::now();

    while (inserted_rows < total_rows) {
        string sql;
        sql.reserve(static_cast<size_t>(INSERT_BATCH_SIZE) * 84U);
        sql.append("INSERT INTO BIG_USERS_RIG VALUES ");

        int rows_in_batch = 0;
        while (rows_in_batch < INSERT_BATCH_SIZE && inserted_rows < total_rows) {
            long long id = inserted_rows + 1;

            sql.push_back('(');
            sql.append(to_string(id));
            sql.append(", 'user");
            sql.append(to_string(id));
            sql.append("', 'user");
            sql.append(to_string(id));
            sql.append("@mail.com', ");
            sql.append(to_string(1000 + (id % 10000)));
            sql.append(", 1893456000)");

            inserted_rows++;
            rows_in_batch++;
            if (rows_in_batch < INSERT_BATCH_SIZE && inserted_rows < total_rows) {
                sql.push_back(',');
            }
        }
        sql.push_back(';');

        char* error_message = nullptr;
        if (flexql_exec(db, sql.c_str(), nullptr, nullptr, &error_message) != FLEXQL_OK) {
            cout << "[FAIL] INSERT BIG_USERS_RIG batch -> " << (error_message ? error_message : "unknown error") << "\n";
            if (error_message != nullptr) {
                flexql_free(error_message);
            }
            return false;
        }

        if (inserted_rows >= next_progress || inserted_rows == total_rows) {
            cout << "Progress USERS: " << inserted_rows << "/" << total_rows << "\n";
            next_progress += progress_step;
        }
    }

    auto end = high_resolution_clock::now();
    elapsed_ms_out = duration_cast<milliseconds>(end - start).count();

    long long throughput = (elapsed_ms_out > 0) ? (total_rows * 1000LL / elapsed_ms_out) : total_rows;
    cout << "[PASS] INSERT BIG_USERS_RIG done | elapsed=" << elapsed_ms_out << " ms | throughput=" << throughput << " rows/sec\n";
    return true;
}

static bool insert_big_orders(FlexQL* db, long long total_user_rows, long long& elapsed_ms_out, long long& inserted_orders_out) {
    long long target_orders = min(500000LL, max(100000LL, total_user_rows / 5LL));
    inserted_orders_out = target_orders;

    cout << "\n[INSERT] BIG_ORDERS_RIG rows=" << target_orders << "\n";

    long long inserted_rows = 0;
    long long progress_step = max(1LL, target_orders / 10LL);
    long long next_progress = progress_step;

    auto start = high_resolution_clock::now();

    while (inserted_rows < target_orders) {
        string sql;
        sql.reserve(static_cast<size_t>(INSERT_BATCH_SIZE) * 58U);
        sql.append("INSERT INTO BIG_ORDERS_RIG VALUES ");

        int rows_in_batch = 0;
        while (rows_in_batch < INSERT_BATCH_SIZE && inserted_rows < target_orders) {
            long long order_id = inserted_rows + 1;
            long long user_id = (order_id % total_user_rows) + 1;
            long long amount = (order_id % 1000);

            sql.push_back('(');
            sql.append(to_string(order_id));
            sql.append(", ");
            sql.append(to_string(user_id));
            sql.append(", ");
            sql.append(to_string(amount));
            sql.append(", 1893456000)");

            inserted_rows++;
            rows_in_batch++;
            if (rows_in_batch < INSERT_BATCH_SIZE && inserted_rows < target_orders) {
                sql.push_back(',');
            }
        }
        sql.push_back(';');

        char* error_message = nullptr;
        if (flexql_exec(db, sql.c_str(), nullptr, nullptr, &error_message) != FLEXQL_OK) {
            cout << "[FAIL] INSERT BIG_ORDERS_RIG batch -> " << (error_message ? error_message : "unknown error") << "\n";
            if (error_message != nullptr) {
                flexql_free(error_message);
            }
            return false;
        }

        if (inserted_rows >= next_progress || inserted_rows == target_orders) {
            cout << "Progress ORDERS: " << inserted_rows << "/" << target_orders << "\n";
            next_progress += progress_step;
        }
    }

    auto end = high_resolution_clock::now();
    elapsed_ms_out = duration_cast<milliseconds>(end - start).count();
    long long throughput = (elapsed_ms_out > 0) ? (target_orders * 1000LL / elapsed_ms_out) : target_orders;
    cout << "[PASS] INSERT BIG_ORDERS_RIG done | elapsed=" << elapsed_ms_out << " ms | throughput=" << throughput << " rows/sec\n";
    return true;
}

static bool benchmark_select_variants(FlexQL* db, long long inserted_user_rows, int repeats) {
    cout << "\n===== Rigorous SELECT Benchmark (retrieve-only, repeats=" << repeats << ") =====\n";

    vector<pair<string, string>> variants;

    long long middle_id = max(1LL, inserted_user_rows / 2LL);
    variants.push_back({
            "PK_POINT_LOOKUP",
            "SELECT NAME, EMAIL FROM BIG_USERS_RIG WHERE ID = " + to_string(middle_id) + ";"});

    variants.push_back({
            "PK_POINT_LOOKUP_TAIL",
            "SELECT NAME, EMAIL FROM BIG_USERS_RIG WHERE ID = " + to_string(inserted_user_rows) + ";"});

        variants.push_back({
            "SELECT_STAR_POINT",
            "SELECT * FROM BIG_USERS_RIG WHERE ID = " + to_string(middle_id) + ";"});

        variants.push_back({
            "SELECT_STAR_SELECTIVE_RANGE",
            "SELECT * FROM BIG_USERS_RIG WHERE BALANCE > 10990;"});

    variants.push_back({
            "NUMERIC_RANGE_SELECTIVE",
            "SELECT ID, BALANCE FROM BIG_USERS_RIG WHERE BALANCE > 10950;"});

    variants.push_back({
            "NUMERIC_RANGE_BROAD",
            "SELECT ID FROM BIG_USERS_RIG WHERE BALANCE > 5000;"});

    variants.push_back({
            "STRING_EQ_LOOKUP",
            "SELECT ID FROM BIG_USERS_RIG WHERE NAME = 'user12345';"});

        variants.push_back({
            "COUNT_ALL",
            "SELECT COUNT(*) FROM BIG_USERS_RIG;"});

        variants.push_back({
            "COUNT_RANGE",
            "SELECT COUNT(*) FROM BIG_USERS_RIG WHERE BALANCE > 5000;"});

    variants.push_back({
            "JOIN_SELECTIVE",
            "SELECT BIG_USERS_RIG.NAME, BIG_ORDERS_RIG.AMOUNT "
            "FROM BIG_USERS_RIG INNER JOIN BIG_ORDERS_RIG "
            "ON BIG_USERS_RIG.ID = BIG_ORDERS_RIG.USER_ID "
            "WHERE BIG_ORDERS_RIG.AMOUNT > 950;"});

    bool all_ok = true;

    for (const auto& entry : variants) {
        const string& label = entry.first;
        const string& sql = entry.second;

        RepeatedTiming timing = run_repeated_query(db, sql, repeats);
        if (!timing.ok) {
            const bool count_variant = (label.rfind("COUNT_", 0) == 0);
            const bool count_unsupported = count_variant &&
                                           (timing.error.find("FROM expected") != string::npos ||
                                            timing.error.find("unsupported") != string::npos);

            if (count_unsupported) {
                cout << "[INFO] " << label << " -> unsupported by current parser (" << timing.error
                     << "), kept in suite as requested\n";
            } else {
                cout << "[FAIL] " << label << " -> " << timing.error << "\n";
                all_ok = false;
            }
            continue;
        }

        cout << "[PASS] " << label
             << " | rows=" << timing.rows
             << " | cold=" << timing.cold_ms << " ms"
             << " | warm_avg=" << timing.warm_avg_ms << " ms"
             << " | warm_min=" << timing.warm_min_ms << " ms"
             << " | warm_max=" << timing.warm_max_ms << " ms"
             << "\n";
    }

    return all_ok;
}

int main(int argc, char** argv) {
    FlexQL* db = nullptr;
    long long target_insert_rows = DEFAULT_INSERT_ROWS;
    int query_repeats = DEFAULT_QUERY_REPEATS;

    if (argc > 1) {
        target_insert_rows = atoll(argv[1]);
        if (target_insert_rows <= 0) {
            cout << "Invalid insert row count.\n";
            return 1;
        }
    }

    if (argc > 2) {
        query_repeats = atoi(argv[2]);
        if (query_repeats <= 0) {
            cout << "Invalid query repeats value.\n";
            return 1;
        }
    }

    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cout << "Cannot open FlexQL\n";
        return 1;
    }

    cout << "Connected to FlexQL\n";
    cout << "Rigorous benchmark config: users=" << target_insert_rows
         << ", query_repeats=" << query_repeats
         << ", insert_batch_size=" << INSERT_BATCH_SIZE << "\n";

    if (!create_benchmark_tables(db)) {
        flexql_close(db);
        return 1;
    }

    long long users_elapsed_ms = 0;
    if (!insert_big_users(db, target_insert_rows, users_elapsed_ms)) {
        flexql_close(db);
        return 1;
    }

    long long orders_elapsed_ms = 0;
    long long inserted_orders = 0;
    if (!insert_big_orders(db, target_insert_rows, orders_elapsed_ms, inserted_orders)) {
        flexql_close(db);
        return 1;
    }

    bool selects_ok = benchmark_select_variants(db, target_insert_rows, query_repeats);

    cout << "\n===== Rigorous Benchmark Summary =====\n";
    cout << "BIG_USERS_RIG rows: " << target_insert_rows << ", insert_ms=" << users_elapsed_ms << "\n";
    cout << "BIG_ORDERS_RIG rows: " << inserted_orders << ", insert_ms=" << orders_elapsed_ms << "\n";
    cout << "SELECT variants status: " << (selects_ok ? "PASS" : "PARTIAL/FAIL") << "\n";

    flexql_close(db);
    return selects_ok ? 0 : 2;
}
