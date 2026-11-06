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
               "SELECT task.table_id, task.column_id, task.request_id, task.row_key, "
               "task.rowkey_predicate, task.retry_count, c.column_name, c.expr_text, "
               "c.max_retries, tb.table_name, db.database_name "
               "FROM %s.%s task "
               "JOIN %s.%s c ON c.table_id = task.table_id AND c.column_id = task.column_id "
               "JOIN %s.%s tb ON tb.table_id = task.table_id "
               "JOIN %s.%s db ON db.database_id = tb.database_id "
               "WHERE task.state IN (0, 3) "
               "AND (task.eligible_ts IS NULL OR task.eligible_ts <= now(6)) "
               // the rows of one request group have to stay adjacent, because the
               // columns of a group are written together in a single UPDATE
               "ORDER BY task.table_id, task.row_key, task.request_id, task.column_id "
               "LIMIT %ld",
               OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME,
               OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TNAME,
               OB_SYS_DATABASE_NAME, OB_ALL_TABLE_TNAME,
               OB_SYS_DATABASE_NAME, OB_ALL_DATABASE_TNAME,
               MAX_TASK_PER_ROUND * 8))) {
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
          } else if (OB_FAIL(result->get_int(static_cast<int64_t>(2), row.request_id_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(3), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.row_key_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(4), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.rowkey_predicate_))) {
          } else if (OB_FAIL(result->get_int(static_cast<int64_t>(5), row.retry_count_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(6), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.column_name_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(7), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.expr_text_))) {
          } else if (OB_FAIL(result->get_int(static_cast<int64_t>(8), row.max_retries_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(9), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.table_name_))) {
          } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(10), tmp))) {
          } else if (OB_FAIL(ob_write_string(allocator_, tmp, row.database_name_))) {
          } else if (OB_FAIL(tasks.push_back(row))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      // A request group is the set of AI columns of one row that the same statement
      // created. The columns of a group are rendered, computed and written together:
      // writing them with one UPDATE is what keeps a reader from seeing one column of
      // the row filled while another one is still missing.
      int64_t begin = 0;
      while (OB_SUCC(ret) && begin < tasks.count()) {
        int64_t end = begin + 1;
        while (end < tasks.count()
               && tasks.at(end).table_id_ == tasks.at(begin).table_id_
               && tasks.at(end).request_id_ == tasks.at(begin).request_id_
               && 0 == tasks.at(end).row_key_.compare(tasks.at(begin).row_key_)) {
          ++end;
        }
        ObSEArray<const ObAiColumnTaskRow *, 4> group;
        for (int64_t g = begin; OB_SUCC(ret) && g < end; ++g) {
          if (OB_FAIL(group.push_back(&tasks.at(g)))) {
          }
        }
        if (OB_SUCC(ret)) {
          int tmp_ret = process_task_group(group);
          if (OB_SUCCESS != tmp_ret) {
            // one failing group must not stop the others
            report_error(tmp_ret);
          }
        }
        begin = end;
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
  } else if (ObAIFuncUtils::is_dense_embedding_type(info)) {
    // An embedding model answers with the vector itself, there is no prompt and no
    // answer format to ask for.
    ObAIFuncModel model(allocator, *info, endpoint_info);
    if (OB_FAIL(model.call_dense_embedding(user_content, NULL, result))) {
      LOG_INFO("ai column embedding call failed", "ret", ret, K(model_name), K(content));
    }
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
    LOG_INFO("failed to delete ai column task", "ret", ret, K(sql));
  }
  return ret;
}

// Drop every task row of the group in one statement.
int ObAiColumnServerWorker::delete_task_group(
    const common::ObIArray<const ObAiColumnTaskRow *> &group)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy) || group.empty()) {
    ret = group.empty() ? OB_INVALID_ARGUMENT : OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt("DELETE FROM %s.%s WHERE table_id = %ld "
                                    "AND row_key = ",
                                    OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME,
                                    static_cast<int64_t>(group.at(0)->table_id_)))) {
  } else if (OB_FAIL(append_sql_string(group.at(0)->row_key_, sql))) {
  } else if (OB_FAIL(sql.append(" AND column_id IN ("))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
      if (OB_FAIL(sql.append_fmt("%s%ld", 0 == i ? "" : ", ",
                                 static_cast<int64_t>(group.at(i)->column_id_)))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append(")"))) {
    } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
      LOG_INFO("failed to delete ai column task group", "ret", ret, K(sql));
    }
  }
  return ret;
}

// Write the answers of a whole group. The task rows go first: a reader that already
// sees the new values must not see the task rows any more.
int ObAiColumnServerWorker::write_task_group(
    const common::ObIArray<const ObAiColumnTaskRow *> &group,
    const common::ObIArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_NOT_INIT;
  } else if (group.empty() || group.count() != results.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(delete_task_group(group))) {
  } else {
    const ObAiColumnTaskRow *first = group.at(0);
    if (OB_FAIL(sql.append_fmt("UPDATE `%.*s`.`%.*s` SET ",
                               first->database_name_.length(), first->database_name_.ptr(),
                               first->table_name_.length(), first->table_name_.ptr()))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
        if (OB_FAIL(sql.append_fmt("%s`%.*s` = ", 0 == i ? "" : ", ",
                                   group.at(i)->column_name_.length(),
                                   group.at(i)->column_name_.ptr()))) {
        } else if (OB_FAIL(append_sql_string(results.at(i), sql))) {
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql.append_fmt(" WHERE %.*s", first->rowkey_predicate_.length(),
                                        first->rowkey_predicate_.ptr()))) {
      } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
        LOG_INFO("failed to write back ai column values", "ret", ret, K(sql));
      }
    }
  }
  return ret;
}

int ObAiColumnServerWorker::retry_later(const ObAiColumnTaskRow &task, const int error_code)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const bool terminal = (task.max_retries_ >= 0 && task.retry_count_ + 1 > task.max_retries_);
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_NOT_INIT;
  } else if (terminal) {
    // out of retries: the row keeps its NULL value and owns no task any more
    if (OB_FAIL(delete_task(static_cast<int64_t>(task.table_id_),
                            static_cast<int64_t>(task.column_id_), task.row_key_))) {
    }
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s.%s SET state = 3, "
                                    "retry_count = retry_count + 1, "
                                    "eligible_ts = DATE_ADD(now(6), INTERVAL 1 SECOND), "
                                    "last_error = '%d', gmt_modified = now(6) "
                                    "WHERE table_id = %ld AND column_id = %ld AND row_key = ",
                                    OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME,
                                    error_code,
                                    static_cast<int64_t>(task.table_id_),
                                    static_cast<int64_t>(task.column_id_)))) {
  } else if (OB_FAIL(append_sql_string(task.row_key_, sql))) {
  } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
    LOG_INFO("failed to update ai column task state", "ret", ret, K(sql));
  }
  return ret;
}

// Render every column of the group, ask the model for each of them and then write the
// whole group at once.
int ObAiColumnServerWorker::process_task_group(
    const common::ObIArray<const ObAiColumnTaskRow *> &group)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AiColGroup");
  // the members that can be answered, their rendered input and the model answer
  ObSEArray<const ObAiColumnTaskRow *, 4> live;
  ObSEArray<ObString, 4> values;
  ObSEArray<ObString, 4> results;
  bool complete = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
    const ObAiColumnTaskRow *task = group.at(i);
    ObString model_name;
    ObString value_expr;
    ObString typed_prompt;
    ObString value;
    ObString result;
    bool found = false;
    split_ai_column_expr_text(task->expr_text_, model_name, value_expr);
    if (model_name.empty() || value_expr.empty()) {
      // the metadata of this AI column is unusable, drop the task instead of spinning
      if (OB_FAIL(delete_task(static_cast<int64_t>(task->table_id_),
                              static_cast<int64_t>(task->column_id_), task->row_key_))) {
      }
    } else if (OB_FAIL(get_typed_prompt(static_cast<int64_t>(task->table_id_),
                                        static_cast<int64_t>(task->column_id_),
                                        typed_prompt))) {
    } else if (OB_FAIL(render_value(allocator, task->database_name_, task->table_name_,
                                    value_expr, task->rowkey_predicate_, value, found))) {
    } else if (!found) {
      // the row disappeared, there is nothing to compute
      if (OB_FAIL(delete_task(static_cast<int64_t>(task->table_id_),
                              static_cast<int64_t>(task->column_id_), task->row_key_))) {
      }
    } else if (OB_FAIL(call_model(allocator, model_name, typed_prompt, value, result))) {
      // the group is written with one UPDATE, so a member that could not be answered
      // makes the whole group wait: it is answered again on a later round
      int saved_ret = ret;
      ret = OB_SUCCESS;
      complete = false;
      LOG_INFO("ai column model call failed", "ret", saved_ret, K(model_name), K(value));
      if (OB_FAIL(retry_later(*task, saved_ret))) {
      }
    } else if (OB_FAIL(values.push_back(value))) {
    } else if (OB_FAIL(results.push_back(result))) {
    } else if (OB_FAIL(live.push_back(task))) {
    }
  }
  if (OB_FAIL(ret) || !complete || live.empty()) {
  } else {
    // The row may have changed while the model was answering. Recompute the value
    // expression of every member: if it no longer renders the value the answer was
    // computed from, the whole group is dropped, so that a late answer can never
    // overwrite a newer one.
    bool still_current = true;
    for (int64_t i = 0; OB_SUCC(ret) && still_current && i < live.count(); ++i) {
      const ObAiColumnTaskRow *task = live.at(i);
      ObString model_name;
      ObString value_expr;
      ObString current;
      bool found = false;
      split_ai_column_expr_text(task->expr_text_, model_name, value_expr);
      if (OB_FAIL(render_value(allocator, task->database_name_, task->table_name_,
                               value_expr, task->rowkey_predicate_, current, found))) {
      } else if (!found || 0 != current.compare(values.at(i))) {
        still_current = false;
      }
    }
    if (OB_FAIL(ret) || !still_current) {
    } else if (OB_FAIL(write_task_group(live, results))) {
      LOG_INFO("failed to write ai column task group", "ret", ret);
    }
  }
  return ret;
}

}  // observer
}  // oceanbase
