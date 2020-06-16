/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

#include "es_statement.h"

#include <aws/core/client/AWSClient.h>
#include <aws/core/http/HttpClient.h>

#include <condition_variable>
#include <thread>

#include "environ.h"  // Critical section for statment
#include "es_apifunc.h"
#include "es_helper.h"
#include "es_result_queue.h"
#include "misc.h"
#include <future>

extern "C" void *common_cs;

static const std::string JSON_SCHEMA =
    "{"  // This was generated from the example elasticsearch data
    "\"type\": \"object\","
    "\"properties\": {"
    "\"schema\": {"
    "\"type\": \"array\","
    "\"items\": [{"
    "\"type\": \"object\","
    "\"properties\": {"
    "\"name\": { \"type\": \"string\" },"
    "\"type\": { \"type\": \"string\" }"
    "},"
    "\"required\": [ \"name\", \"type\" ]"
    "}]"
    "},"
    "\"cursor\": { \"type\": \"string\" },"
    "\"total\": { \"type\": \"integer\" },"
    "\"datarows\": {"
    "\"type\": \"array\","
    "\"items\": {}"
    "},"
    "\"size\": { \"type\": \"integer\" },"
    "\"status\": { \"type\": \"integer\" }"
    "},"
    "\"required\": [\"schema\", \"total\", \"datarows\", \"size\", \"status\"]"
    "}";
static const std::string CURSOR_JSON_SCHEMA =
    "{"  // This was generated from the example elasticsearch data
    "\"type\": \"object\","
    "\"properties\": {"
    "\"cursor\": { \"type\": \"string\" },"
    "\"datarows\": {"
    "\"type\": \"array\","
    "\"items\": {}"
    "},"
    "\"status\": { \"type\": \"integer\" }"
    "},"
    "\"required\":  [\"datarows\"]"
    "}";

class ESResultContext {
   public:
    ESResultContext();
    ~ESResultContext();
    SQLRETURN ESResultContext::ExecuteQuery(StatementClass *stmt);
    ESResult *ESResultContext::GetResult();
    void ExecuteCursorQueries(StatementClass *stmt, const char *_cursor);

   private:
    void ConstructESResult(ESResult &result);
    void GetJsonSchema(ESResult &es_result);
    void PrepareCursorResult(ESResult &es_result);

    StatementClass m_stmt;
    ESResultQueue m_result_queue;
    std::mutex m_mutex;
    std::condition_variable m_condition_variable;
    std::thread m_result_thread;
    std::string m_error_message;
};

inline void LogMsg(ESLogLevel level, const char *msg) {
#if WIN32
#pragma warning(push)
#pragma warning(disable : 4551)
#endif  // WIN32
    // cppcheck outputs an erroneous missing argument error which breaks build.
    // Disable for this function call
    MYLOG(level, "%s\n", msg);
#if WIN32
#pragma warning(pop)
#endif  // WIN32
}

ESResultContext::ESResultContext() {
}

ESResultContext::~ESResultContext() {
    m_result_queue.clear();
}

void ESResultContext::GetJsonSchema(ESResult &es_result) {
    // Prepare document and validate schema
    try {
        LogMsg(ES_DEBUG, "Parsing result JSON with schema.");
        es_result.es_result_doc.parse(es_result.result_json, JSON_SCHEMA);
    } catch (const rabbit::parse_error &e) {
        // The exception rabbit gives is quite useless - providing the json
        // will aid debugging for users
        std::string str = "Exception obtained '" + std::string(e.what())
                          + "' when parsing json string '"
                          + es_result.result_json + "'.";
        throw std::runtime_error(str.c_str());
    }
}

void ESResultContext::PrepareCursorResult(ESResult &es_result) {
    // Prepare document and validate result
    try {
        LogMsg(ES_DEBUG, "Parsing result JSON with cursor.");
        es_result.es_result_doc.parse(es_result.result_json,
                                      CURSOR_JSON_SCHEMA);
    } catch (const rabbit::parse_error &e) {
        // The exception rabbit gives is quite useless - providing the json
        // will aid debugging for users
        std::string str = "Exception obtained '" + std::string(e.what())
                          + "' when parsing json string '"
                          + es_result.result_json + "'.";
        throw std::runtime_error(str.c_str());
    }
}

void ESResultContext::ConstructESResult(ESResult &result) {
    GetJsonSchema(result);
    rabbit::array schema_array = result.es_result_doc["schema"];
    for (rabbit::array::iterator it = schema_array.begin();
         it != schema_array.end(); ++it) {
        std::string column_name = it->at("name").as_string();

        ColumnInfo col_info;
        col_info.field_name = column_name;
        col_info.type_oid = KEYWORD_TYPE_OID;
        col_info.type_size = KEYWORD_TYPE_SIZE;
        col_info.display_size = KEYWORD_DISPLAY_SIZE;
        col_info.length_of_str = KEYWORD_TYPE_SIZE;
        col_info.relation_id = 0;
        col_info.attribute_number = 0;

        result.column_info.push_back(col_info);
    }
    if (result.es_result_doc.has("cursor")) {
        result.cursor = result.es_result_doc["cursor"].as_string();
    }
    result.command_type = "SELECT";
    result.num_fields = (uint16_t)schema_array.size();
}

SQLRETURN ESResultContext::ExecuteQuery(StatementClass *stmt) {
    if (stmt == NULL)
        return SQL_ERROR;

    // Send command
    ConnectionClass *conn = SC_get_conn(stmt);

    std::shared_ptr< Aws::Http::HttpResponse > response =
        ESExecDirect(conn->esconn, stmt->statement, conn->connInfo.fetch_size);

    // Convert body from Aws IOStream to string
    ESResult *result = new ESResult;
    ESAwsHttpResponseToString(conn, response, result->result_json);

    // If response was not valid, set error
    if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
        m_error_message =
            "Http response code was not OK. Code received: "
            + std::to_string(static_cast< long >(response->GetResponseCode()))
            + ".";
        if (response->HasClientError())
            m_error_message +=
                " Client error: '" + response->GetClientErrorMessage() + "'.";
        if (!result->result_json.empty()) {
            m_error_message +=
                " Response error: '" + result->result_json + "'.";
        }
        LogMsg(ES_ERROR, m_error_message.c_str());
        delete result;
        return -1;
    }

    // Add to result queue and return
    try {
        ConstructESResult(*result);
    } catch (std::runtime_error &e) {
        m_error_message =
            "Received runtime exception: " + std::string(e.what());
        if (!result->result_json.empty())
            m_error_message += " Result body: " + result->result_json;
        LogMsg(ES_ERROR, m_error_message.c_str());
        delete result;
        return -1;
    }
    SQLRETURN ret =
        m_result_queue.push(std::reference_wrapper< ESResult >(*result));
    if (ret == SQL_ERROR) {
        LogMsg(ES_ERROR, "Failed to add result in queue");
    }

    if (!result->cursor.empty()) {
        // If response has cursor, this thread will retrives more results pages
        auto send_cursor_queries = std::async(std::launch::async, [&]() {
            ExecuteCursorQueries(stmt, result->cursor.c_str());
        });
    }

    return SQL_SUCCESS;
}

void ESResultContext::ExecuteCursorQueries(StatementClass *stmt,
                                           const char *_cursor) {
    if (_cursor == NULL) {
        return;
    }
    try {
        std::string cursor(_cursor);
        ConnectionClass *conn = SC_get_conn(stmt);

        if (conn == NULL) {
            return;
        }

        while (!cursor.empty() && !m_result_queue.IsFull()) {
            std::shared_ptr< Aws::Http::HttpResponse > response =
                ESSendCursorQuery(conn->esconn, _cursor);

            ESResult *es_result = new ESResult;
            ESAwsHttpResponseToString(conn, response, es_result->result_json);
            PrepareCursorResult(*es_result);
            if (es_result->es_result_doc.has("cursor")) {
                cursor = es_result->es_result_doc["cursor"].as_string();
                es_result->cursor =
                    es_result->es_result_doc["cursor"].as_string();
            } else {
                SendCloseCursorRequest(conn, cursor);
                cursor.clear();
            }
            SQLRETURN ret = m_result_queue.push(
                std::reference_wrapper< ESResult >(*es_result));
            if (ret == SQL_ERROR) {
                LogMsg(ES_ERROR, "Failed to add result in queue");
            }
        }
    } catch (std::runtime_error &e) {
        m_error_message =
            "Received runtime exception: " + std::string(e.what());
        LogMsg(ES_ERROR, m_error_message.c_str());
    }
}

void GetResultContext(StatementClass *stmt) {
    if (stmt == NULL) {
        return;
    }
    if (stmt->result_context == NULL) {
        ESResultContext *result_context = new ESResultContext();
        stmt->result_context = result_context;
    }
}

ESResult* ESResultContext::GetResult() {
    if (m_result_queue.empty()) {
        LogMsg(ES_WARNING, "Result queue is empty; returning null result.");
        return NULL;
    }
    ESResult &result = m_result_queue.pop_front().get();

    return &result;
}

RETCODE ExecuteStatement(StatementClass *stmt, BOOL commit) {
    CSTR func = "ExecuteStatement";
    int func_cs_count = 0;
    ConnectionClass *conn = SC_get_conn(stmt);
    CONN_Status oldstatus = conn->status;

    auto CleanUp = [&]() -> RETCODE {
        SC_SetExecuting(stmt, FALSE);
        CLEANUP_FUNC_CONN_CS(func_cs_count, conn);
        if (conn->status != CONN_DOWN)
            conn->status = oldstatus;
        if (SC_get_errornumber(stmt) == STMT_OK)
            return SQL_SUCCESS;
        else if (SC_get_errornumber(stmt) < STMT_OK)
            return SQL_SUCCESS_WITH_INFO;
        else {
            if (!SC_get_errormsg(stmt) || !SC_get_errormsg(stmt)[0]) {
                if (STMT_NO_MEMORY_ERROR != SC_get_errornumber(stmt))
                    SC_set_errormsg(stmt, "Error while executing the query");
                SC_log_error(func, NULL, stmt);
            }
            return SQL_ERROR;
        }
    };

    ENTER_INNER_CONN_CS(conn, func_cs_count);

    if (stmt->result_context == NULL) {
        GetResultContext(stmt);
    }

    if (conn->status == CONN_EXECUTING) {
        SC_set_error(stmt, STMT_SEQUENCE_ERROR, "Connection is already in use.",
                     func);
        return CleanUp();
    }

    if (!SC_SetExecuting(stmt, TRUE)) {
        SC_set_error(stmt, STMT_OPERATION_CANCELLED, "Cancel Request Accepted",
                     func);
        return CleanUp();
    }

    conn->status = CONN_EXECUTING;

    QResultClass *res = SendQueryGetResult(stmt, commit);
    if (!res) {
        std::string es_conn_err = GetErrorMsg(SC_get_conn(stmt)->esconn);
        std::string es_parse_err = GetResultParserError();
        if (!es_conn_err.empty()) {
            SC_set_error(stmt, STMT_NO_RESPONSE, es_conn_err.c_str(), func);
        } else if (!es_parse_err.empty()) {
            SC_set_error(stmt, STMT_EXEC_ERROR, es_parse_err.c_str(), func);
        } else if (SC_get_errornumber(stmt) <= 0) {
            SC_set_error(
                stmt, STMT_NO_RESPONSE,
                "Failed to error message from result. Connection may be down.",
                func);
        }
        return CleanUp();
    }

    if (CONN_DOWN != conn->status)
        conn->status = oldstatus;
    stmt->status = STMT_FINISHED;
    LEAVE_INNER_CONN_CS(func_cs_count, conn);

    // Check the status of the result
    if (SC_get_errornumber(stmt) < 0) {
        if (QR_command_successful(res))
            SC_set_errornumber(stmt, STMT_OK);
        else if (QR_command_nonfatal(res))
            SC_set_errornumber(stmt, STMT_INFO_ONLY);
        else
            SC_set_errorinfo(stmt, res, 0);
    }

    // Set cursor before the first tuple in the list
    stmt->currTuple = -1;
    SC_set_current_col(stmt, static_cast< int >(stmt->currTuple));
    SC_set_rowset_start(stmt, stmt->currTuple, FALSE);

    // Only perform if query was not aborted
    if (!QR_get_aborted(res)) {
        // Check if result columns were obtained from query
        for (QResultClass *tres = res; tres; tres = tres->next) {
            Int2 numcols = QR_NumResultCols(tres);
            if (numcols <= 0)
                continue;
            ARDFields *opts = SC_get_ARDF(stmt);
            extend_column_bindings(opts, numcols);
            if (opts->bindings)
                break;

            // Failed to allocate
            QR_Destructor(res);
            SC_set_error(stmt, STMT_NO_MEMORY_ERROR,
                         "Could not get enough free memory to store "
                         "the binding information",
                         func);
            return CleanUp();
        }
    }

    QResultClass *last = SC_get_Result(stmt);
    if (last) {
        // Statement already contains a result
        // Append to end if this hasn't happened
        while (last->next != NULL) {
            if (last == res)
                break;
            last = last->next;
        }
        if (last != res)
            last->next = res;
    } else {
        // Statement does not contain a result
        // Assign directly
        SC_set_Result(stmt, res);
    }

    // This will commit results for SQLExecDirect and will not commit
    // results for SQLPrepare since only metadata is required for SQLPrepare
    if (commit) {
        GetNextResultSet(stmt);
    }

    if (!SC_get_Curres(stmt))
        SC_set_Curres(stmt, SC_get_Result(stmt));
    stmt->diag_row_count = res->recent_processed_row_count;

    return CleanUp();
}

SQLRETURN GetNextResultSet(StatementClass *stmt) {
    ConnectionClass *conn = SC_get_conn(stmt);
    QResultClass *q_res = SC_get_Result(stmt);
    if ((q_res == NULL) && (conn == NULL)) {
        return SQL_ERROR;
    }

    SQLSMALLINT total_columns = -1;
    if (!SQL_SUCCEEDED(SQLNumResultCols(stmt, &total_columns))
        || (total_columns == -1)) {
        return SQL_ERROR;
    }

    if (stmt->result_context == NULL) {
        return NULL;
    }

    ESResultContext *result_context =
        static_cast< ESResultContext * >(stmt->result_context);

    ESResult *es_res = result_context->GetResult();
    while (es_res != NULL) {
        // Save server cursor id to fetch more pages later
        if (es_res->es_result_doc.has("cursor")) {
            QR_set_server_cursor_id(
                q_res, es_res->es_result_doc["cursor"].as_string().c_str());
        } else {
            QR_set_server_cursor_id(q_res, NULL);
        }

        // Responsible for looping through rows, allocating tuples and
        // appending these rows in q_result
        CC_Append_Table_Data(es_res->es_result_doc, q_res, total_columns,
                             *(q_res->fields));
        es_res = result_context->GetResult();
    }
    return SQL_SUCCESS;
}

RETCODE RePrepareStatement(StatementClass *stmt) {
    CSTR func = "RePrepareStatement";
    RETCODE result = SC_initialize_and_recycle(stmt);
    if (result != SQL_SUCCESS)
        return result;
    if (!stmt->statement) {
        SC_set_error(stmt, STMT_NO_MEMORY_ERROR,
                     "Expected statement to be allocated.", func);
        return SQL_ERROR;
    }

    // If an SQLPrepare was performed prior to this, but was left in the
    // described state because an error prior to SQLExecute then set the
    // statement to finished so it can be recycled.
    if (stmt->status == STMT_DESCRIBED)
        stmt->status = STMT_FINISHED;

    return SQL_SUCCESS;
}

RETCODE PrepareStatement(StatementClass *stmt, const SQLCHAR *stmt_str,
                         SQLINTEGER stmt_sz) {
    CSTR func = "PrepareStatement";
    RETCODE result = SC_initialize_and_recycle(stmt);
    if (result != SQL_SUCCESS)
        return result;

    stmt->statement = make_string(stmt_str, stmt_sz, NULL, 0);
    if (!stmt->statement) {
        SC_set_error(stmt, STMT_NO_MEMORY_ERROR,
                     "No memory available to store statement", func);
        return SQL_ERROR;
    }

    // If an SQLPrepare was performed prior to this, but was left in the
    // described state because an error prior to SQLExecute then set the
    // statement to finished so it can be recycled.
    if (stmt->status == STMT_DESCRIBED)
        stmt->status = STMT_FINISHED;
    stmt->statement_type = (short)statement_type(stmt->statement);

    return SQL_SUCCESS;
}

QResultClass *SendQueryGetResult(StatementClass *stmt, BOOL commit) {
    if (stmt == NULL)
        return NULL;

    // Allocate QResultClass
    QResultClass *res = QR_Constructor();
    if (res == NULL)
        return NULL;

    // Send command
    ConnectionClass *conn = SC_get_conn(stmt);
    if (stmt->result_context == NULL) {
        return NULL;
    }

    ESResultContext *result_context =
        static_cast< ESResultContext * >(stmt->result_context);

    if (result_context->ExecuteQuery(stmt) != 0) {
        QR_Destructor(res);
        return NULL;
    }
    res->rstatus = PORES_COMMAND_OK;

    // Get ESResult
    ESResult *es_res = result_context->GetResult();
    if (es_res == NULL) {
        QR_Destructor(res);
        return NULL;
    }

    BOOL success =
        commit
            ? CC_from_ESResult(res, conn, res->cursor_name, *es_res)
            : CC_Metadata_from_ESResult(res, conn, res->cursor_name, *es_res);

    // Convert result to QResultClass
    if (!success) {
        QR_Destructor(res);
        res = NULL;
    }

    if (commit) {
        // Deallocate ESResult
        ESClearResult(es_res);
        res->es_result = NULL;
    } else {
        // Set ESResult into connection class so it can be used later
        res->es_result = es_res;
    }
    return res;
}

RETCODE AssignResult(StatementClass *stmt) {
    if (stmt == NULL)
        return SQL_ERROR;

    QResultClass *res = SC_get_Result(stmt);
    if (!res || !res->es_result) {
        return SQL_ERROR;
    }

    // Commit result to QResultClass
    ESResult *es_res = static_cast< ESResult * >(res->es_result);
    ConnectionClass *conn = SC_get_conn(stmt);
    if (!CC_No_Metadata_from_ESResult(res, conn, res->cursor_name, *es_res)) {
        QR_Destructor(res);
        return SQL_ERROR;
    }
    GetNextResultSet(stmt);

    // Deallocate and return result
    ESClearResult(es_res);
    res->es_result = NULL;
    return SQL_SUCCESS;
}

void ClearESResult(void *es_result) {
    if (es_result != NULL) {
        ESResult *es_res = static_cast< ESResult * >(es_result);
        ESClearResult(es_res);
    }
}

void ClearResultContext(void *result_context_) {
    if (result_context_ != NULL) {
        ESResultContext *result_context = static_cast< ESResultContext * >(result_context_);
        result_context->~ESResultContext();
    }
}

void ESSendCursorQueries(StatementClass *stmt, char *server_cursor_id) {
    if (stmt->result_context != NULL) {
        ESResultContext *result_context =
            static_cast< ESResultContext * >(stmt->result_context);
        result_context->ExecuteCursorQueries(stmt, server_cursor_id);
    }
}

SQLRETURN ESAPI_Cancel(HSTMT hstmt) {
    // Verify pointer validity and convert to StatementClass
    if (hstmt == NULL)
        return SQL_INVALID_HANDLE;
    StatementClass *stmt = (StatementClass *)hstmt;

    // Get execution delegate (if applicable) and initialize return code
    StatementClass *estmt =
        (stmt->execute_delegate == NULL) ? stmt : stmt->execute_delegate;
    SQLRETURN ret = SQL_SUCCESS;

    // Entry common critical section
    ENTER_COMMON_CS;

    // Waiting for more data from SQLParamData/SQLPutData - cancel statement
    if (estmt->data_at_exec >= 0) {
        // Enter statement critical section
        ENTER_STMT_CS(stmt);

        // Clear info and cancel need data
        SC_clear_error(stmt);
        estmt->data_at_exec = -1;
        estmt->put_data = FALSE;
        cancelNeedDataState(estmt);

        // Leave statement critical section
        LEAVE_STMT_CS(stmt);
    }

    // Leave common critical section
    LEAVE_COMMON_CS;

    return ret;
}
