/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SERVER
#include "config_bridge.h"
#include "share/config/ob_config_helper.h"
#include "ob_server_duty_task.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/ob_sql_memory_manager.h"
#include "observer/omt/ob_server_runtime.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase::common;

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace sql;
namespace observer {

// The AI column worker lives as long as the process: it is registered on the server
// timer, which may still reference it while the server is shutting down, so it is
// deliberately never destroyed.
ObAiColumnServerWorker &ai_column_server_worker()
{
  static ObAiColumnServerWorker *worker = new (std::nothrow) ObAiColumnServerWorker();
  return *worker;
}

ObServerDutyTask::ObServerDutyTask()
  : allocator_(ObModIds::OB_DUTY_TASK)
{
}

void ObServerDutyTask::runTimerTask()
{
  allocator_.reset_remain_one_page();
  update_runtime_settings();
}

int ObServerDutyTask::schedule(common::ObTimer &timer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(timer.schedule(*this, SCHEDULE_PERIOD, true))) {
  } else if (OB_FAIL(ai_column_server_worker().schedule(timer))) {
    LOG_WARN("failed to schedule the ai column worker", KR(ret));
  }
  return ret;
}

void ObServerDutyTask::update_runtime_settings()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_ctx_memory_throttle())) {
  }
}

int ObServerDutyTask::update_ctx_memory_throttle()
{
  int ret = OB_SUCCESS;
  {
    ObCtxMemoryLimitChecker checker;
    uint64_t ctx_id = 0;
    int64_t limit = 0;
    ObMallocAllocator *alloc = ObMallocAllocator::get_instance();
    rust::String limits = config::_ctx_memory_limit();
    if (!checker.check(limits.c_str(), ctx_id, limit)) {
      // do nothing
    } else {
      if (limits.empty()) {
        ctx_id = ObCtxIds::MAX_CTX_ID;
        limit = INT64_MAX; // empty str means no limit, and not care ctx_id.
      }
      for (int i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
        if (ObCtxIds::WORK_AREA == i ||
            ObCtxIds::META_OBJ_CTX_ID == i) {
          continue;
        }
        auto allocator = alloc->get_ctx_allocator(i);
        if (OB_NOT_NULL(allocator)) {
          if (OB_FAIL(allocator->set_limit(ctx_id == i ? limit : INT64_MAX))) {
          }
        }
      }
    }
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////

int ObSqlMemoryTimerTask::schedule(common::ObTimer &timer)
{
  return timer.schedule(*this, SCHEDULE_PERIOD, true);
}

void ObSqlMemoryTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  {
    SERVER_MODULE_SCOPE {
      ObSqlMemoryManager *sql_mem_mgr = ::oceanbase::share::server_service<::oceanbase::sql::ObSqlMemoryManager>();
      if (OB_UNLIKELY(nullptr == sql_mem_mgr)) {
      } else if (OB_FAIL(sql_mem_mgr->calculate_global_bound_size())) {
      }
    }
  }
}

//////////////////////////////////////////////////////////////////////////////////////////

namespace {

// one pending __all_ai_column_task row joined with its AI column metadata
struct ObAiColumnTaskRow
{
  ObAiColumnTaskRow()
    : table_id_(OB_INVALID_ID), column_id_(OB_INVALID_ID), retry_count_(0), max_retries_(0)
  {
  }
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t retry_count_;
  int64_t max_retries_;
  ObString row_key_;
  ObString rowkey_predicate_;
  ObString column_name_;
  ObString expr_text_;
  ObString table_name_;
  ObString database_name_;
};

// the separator between the model name and the value expression inside
// __all_ai_column.expr_text
const char AI_COLUMN_EXPR_SEPARATOR = '\x1f';
// the answer format hints, the system message of the model request
const char *AI_COLUMN_JSON_PROMPT = "Return exactly one valid JSON value.";
const char *AI_COLUMN_INT_PROMPT = "Return only a base-10 integer.";

// split "<model name>\x1f<value expression>"
void split_ai_column_expr_text(const ObString &expr_text,
                               ObString &model_name,
                               ObString &value_expr)
{
  model_name.reset();
  value_expr.reset();
  if (!expr_text.empty()) {
    const char *ptr = expr_text.ptr();
    const int64_t len = expr_text.length();
    int64_t sep = -1;
    for (int64_t i = 0; -1 == sep && i < len; ++i) {
      if (AI_COLUMN_EXPR_SEPARATOR == ptr[i]) {
        sep = i;
      }
    }
    if (-1 == sep) {
      model_name = expr_text;
    } else {
      model_name.assign_ptr(ptr, static_cast<int32_t>(sep));
      value_expr.assign_ptr(ptr + sep + 1, static_cast<int32_t>(len - sep - 1));
    }
  }
}

// append %value as a single quoted SQL string literal
int append_sql_string(const ObString &value, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
      const char c = value.ptr()[i];
      if ('\'' == c) {
        if (OB_FAIL(sql.append("''"))) {
        }
      } else if ('\\' == c) {
        if (OB_FAIL(sql.append("\\\\"))) {
        }
      } else if (OB_FAIL(sql.append(&c, 1))) {
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append("'"))) {
    }
  }
  return ret;
}
}  // namespace

ObAiColumnServerWorker::ObAiColumnServerWorker()
  : allocator_(ObModIds::OB_DUTY_TASK), inited_(false), last_error_ts_(0)
{
}

// The server log of this deployment keeps INFO lines only, so the worker reports its
// failures at INFO level. The throttle keeps a persistent failure from flooding it.
void ObAiColumnServerWorker::report_error(const int error_code)
{
  const int64_t now = ObTimeUtility::current_time();
  if (now - last_error_ts_ > 1000 * 1000L) {
    LOG_INFO("ai column worker failed", "ret", error_code);
    last_error_ts_ = now;
  }
}

int ObAiColumnServerWorker::schedule(common::ObTimer &timer)
{
  return timer.schedule(*this, SCHEDULE_PERIOD, true);
}

void ObAiColumnServerWorker::runTimerTask()
{
  int ret = OB_SUCCESS;
  allocator_.reset_remain_one_page();
  SERVER_MODULE_SCOPE {
    if (OB_FAIL(process_pending_tasks())) {
      report_error(ret);
    }
  }
}

// Pick the rows that are waiting for a value and let the model fill them in. A task
// row is only a hint: whether the value is still needed is decided by the AI column
// being NULL, so a stale task row can never overwrite a newer value.
int ObAiColumnServerWorker::process_pending_tasks()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt(
               "SELECT task.table_id, task.column_id, task.row_key, task.rowkey_predicate, "
               "task.retry_count, c.column_name, c.expr_text, c.max_retries, "
               "tb.table_name, db.database_name "
               "FROM %s.%s task "
               "JOIN %s.%s c ON c.table_id = task.table_id AND c.column_id = task.column_id "
               "JOIN %s.%s tb ON tb.table_id = task.table_id "
               "JOIN %s.%s db ON db.database_id = tb.database_id "
               "WHERE task.state IN (0, 3) "
               "AND (task.eligible_ts IS NULL OR task.eligible_ts <= now(6)) "
               "LIMIT %ld",
               OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME,
               OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TNAME,
               OB_SYS_DATABASE_NAME, OB_ALL_TABLE_TNAME,
               OB_SYS_DATABASE_NAME, OB_ALL_DATABASE_TNAME,
               MAX_TASK_PER_ROUND))) {
  } else {
    ObSEArray<ObAiColumnTaskRow, 8> tasks;
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        while (OB_SUCC(ret)) {
          ObAiColumnTaskRow row;
          ObString tmp;
          int64_t int_value = 0;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            }
            break;
          } else if (OB_FAIL(result->get_int(static_cast<int64_t>(0), int_value))) {
          } else if (FALSE_IT(row.table_id_ = static_cast<uint64_t>(int_value))) {
          } else if (OB_FAIL(result->get_int(static_cast<int64_t>(1), int_value))) {
          } else if (FALSE_IT(row.column_id_ = static_cast<uint64_t>(int_value))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(2), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.row_key_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(3), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.rowkey_predicate_))) {
          } else if (OB_FAIL(result->get_int(static_cast<int64_t>(4), row.retry_count_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(5), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.column_name_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(6), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.expr_text_))) {
          } else if (OB_FAIL(result->get_int(static_cast<int64_t>(7), row.max_retries_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(8), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.table_name_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(9), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.database_name_))) {
          } else if (OB_FAIL(tasks.push_back(row))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
        const ObAiColumnTaskRow &row = tasks.at(i);
        int tmp_ret = process_one_task(row.table_id_, row.column_id_, row.row_key_,
                                       row.rowkey_predicate_, row.retry_count_,
                                       row.max_retries_, row.column_name_, row.expr_text_,
                                       row.table_name_, row.database_name_);
        if (OB_SUCCESS != tmp_ret) {
          // one failing task must not stop the others
          report_error(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObAiColumnServerWorker::get_typed_prompt(const int64_t table_id,
                                             const int64_t column_id,
                                             ObString &typed_prompt)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = NULL;
  const share::schema::ObColumnSchemaV2 *column_schema = NULL;
  typed_prompt.reset();
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(schema_guard))) {
  } else if (OB_FAIL(schema_guard.get_table_schema(static_cast<uint64_t>(table_id),
                                                   table_schema))) {
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(
                 static_cast<uint64_t>(column_id)))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (column_schema->is_json()) {
    typed_prompt = ObString::make_string(AI_COLUMN_JSON_PROMPT);
  } else if (ob_is_integer_type(column_schema->get_data_type())
             || ob_is_number_tc(column_schema->get_data_type())) {
    typed_prompt = ObString::make_string(AI_COLUMN_INT_PROMPT);
  } else {
    // a plain string column needs no format hint
  }
  return ret;
}

int ObAiColumnServerWorker::render_value(ObIAllocator &allocator,
                                         const ObString &database_name,
                                         const ObString &table_name,
                                         const ObString &value_expr,
                                         const ObString &rowkey_predicate,
                                         ObString &value,
                                         bool &found)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  found = false;
  value.reset();
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt("SELECT CAST(%.*s AS CHAR) FROM `%.*s`.`%.*s` WHERE %.*s",
                                    value_expr.length(), value_expr.ptr(),
                                    database_name.length(), database_name.ptr(),
                                    table_name.length(), table_name.ptr(),
                                    rowkey_predicate.length(), rowkey_predicate.ptr()))) {
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          // the row is gone, the task has to go as well
          ret = OB_SUCCESS;
        }
      } else {
        ObString tmp;
        found = true;
        if (OB_FAIL(result->get_varchar(static_cast<int64_t>(0), tmp))) {
        } else if (OB_FAIL(ob_write_string(allocator, tmp, value))) {
        }
      }
    }
  }
  return ret;
}

int ObAiColumnServerWorker::call_model(ObIAllocator &allocator,
                                       const ObString &model_name,
                                       const ObString &typed_prompt,
                                       const ObString &content,
                                       ObString &result)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *info = NULL;
  share::ObAiModelEndpointInfo endpoint_info;
  query::ObIAiEndpointResolver *endpoint_resolver =
      ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  ObAIFuncIComplete *complete_provider = NULL;
  ObArray<ObString> headers;
  ObJsonObject *body = NULL;
  ObJsonObject *response = NULL;
  ObIJsonBase *result_base = NULL;
  ObString access_key;
  ObAIFuncClient client;
  ObString system_prompt = typed_prompt;
  ObString user_content = content;
  result.reset();
  if (content.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_name, info))) {
  } else if (OB_ISNULL(info) || OB_ISNULL(endpoint_resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(model_name, allocator,
                                                             endpoint_info))) {
  } else if (OB_FAIL(ObAIFuncUtils::get_complete_provider(allocator,
                 endpoint_info.get_provider(), complete_provider))) {
  } else if (OB_ISNULL(complete_provider)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(endpoint_info.get_unencrypted_access_key(allocator, access_key))) {
  } else if (OB_FAIL(complete_provider->get_header(allocator, access_key, headers))) {
  } else {
    // the typed hint is the system message, the rendered row value is the user message
    ObString request_model_name = endpoint_info.get_request_model_name().empty()
                                      ? info->model_ : endpoint_info.get_request_model_name();
    if (OB_FAIL(complete_provider->get_body(allocator, request_model_name, system_prompt,
                                            user_content, NULL, body))) {
    } else if (OB_FAIL(client.send_post(allocator, endpoint_info.get_url(), headers,
                                        body, response))) {
    } else if (OB_FAIL(complete_provider->parse_output(allocator, response, result_base))) {
    } else if (OB_ISNULL(result_base)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (ObJsonNodeType::J_STRING == result_base->json_type()) {
      // the model answer is a string, the column conversion parses it as JSON when
      // the AI column is a json column
      result.assign_ptr(result_base->get_data(),
                        static_cast<int32_t>(result_base->get_data_length()));
    } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, result_base, result))) {
    }
  }
  return ret;
}

int ObAiColumnServerWorker::delete_task(const int64_t table_id,
                                        const int64_t column_id,
                                        const ObString &row_key)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt("DELETE FROM %s.%s WHERE table_id = %ld "
                                    "AND column_id = %ld AND row_key = ",
                                    OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME,
                                    table_id, column_id))) {
  } else if (OB_FAIL(append_sql_string(row_key, sql))) {
  } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to delete ai column task", KR(ret), K(sql));
  }
  return ret;
}

int ObAiColumnServerWorker::update_row(const ObString &database_name,
                                       const ObString &table_name,
                                       const ObString &column_name,
                                       const ObString &rowkey_predicate,
                                       const ObString &value)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt("UPDATE `%.*s`.`%.*s` SET `%.*s` = ",
                                    database_name.length(), database_name.ptr(),
                                    table_name.length(), table_name.ptr(),
                                    column_name.length(), column_name.ptr()))) {
  } else if (OB_FAIL(append_sql_string(value, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" WHERE %.*s", rowkey_predicate.length(),
                                    rowkey_predicate.ptr()))) {
  } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write back ai column value", KR(ret), K(sql));
  }
  return ret;
}

int ObAiColumnServerWorker::retry_later(const int64_t table_id,
                                        const int64_t column_id,
                                        const ObString &row_key,
                                        const int64_t retry_count,
                                        const int64_t max_retries,
                                        const int error_code)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const bool terminal = (max_retries >= 0 && retry_count + 1 > max_retries);
  const int64_t next_state = terminal ? 2 : 3;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s.%s SET state = %ld, "
                                    "retry_count = retry_count + 1, "
                                    "eligible_ts = DATE_ADD(now(6), INTERVAL 1 SECOND), "
                                    "last_error = '%d', gmt_modified = now(6) "
                                    "WHERE table_id = %ld AND column_id = %ld AND row_key = ",
                                    OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME,
                                    next_state, error_code, table_id, column_id))) {
  } else if (OB_FAIL(append_sql_string(row_key, sql))) {
  } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to update ai column task state", KR(ret), K(sql));
  }
  return ret;
}

int ObAiColumnServerWorker::process_one_task(const int64_t table_id,
                                             const int64_t column_id,
                                             const ObString &row_key,
                                             const ObString &rowkey_predicate,
                                             const int64_t retry_count,
                                             const int64_t max_retries,
                                             const ObString &column_name,
                                             const ObString &expr_text,
                                             const ObString &table_name,
                                             const ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AiColWorker");
  ObString model_name;
  ObString value_expr;
  ObString typed_prompt;
  ObString value;
  ObString result;
  bool found = false;
  split_ai_column_expr_text(expr_text, model_name, value_expr);
  if (model_name.empty() || value_expr.empty()) {
    // the metadata of this AI column is unusable, drop the task instead of spinning
    if (OB_FAIL(delete_task(table_id, column_id, row_key))) {
    }
  } else if (OB_FAIL(get_typed_prompt(table_id, column_id, typed_prompt))) {
  } else if (OB_FAIL(render_value(allocator, database_name, table_name, value_expr,
                                  rowkey_predicate, value, found))) {
  } else if (!found) {
    // the row disappeared, there is nothing to compute
    if (OB_FAIL(delete_task(table_id, column_id, row_key))) {
    }
  } else if (OB_FAIL(call_model(allocator, model_name, typed_prompt, value, result))) {
    LOG_WARN("ai column model call failed", KR(ret), K(model_name), K(value));
    int tmp_ret = ret;
    if (OB_FAIL(retry_later(table_id, column_id, row_key, retry_count, max_retries,
                            tmp_ret))) {
    }
    ret = OB_SUCCESS;
  } else {
    // The task row goes first: a reader that already sees the new value must not
    // see the task row any more, otherwise the two could be observed at once.
    if (OB_FAIL(delete_task(table_id, column_id, row_key))) {
    } else if (OB_FAIL(update_row(database_name, table_name, column_name,
                                  rowkey_predicate, result))) {
      // put the task back so that the value is computed on the next round
      int tmp_ret = ret;
      LOG_WARN("failed to write back the ai column value", KR(tmp_ret));
      if (OB_FAIL(retry_later(table_id, column_id, row_key, retry_count, max_retries,
                              tmp_ret))) {
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

}  // observer
}  // oceanbase
