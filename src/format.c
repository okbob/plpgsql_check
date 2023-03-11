/*-------------------------------------------------------------------------
 *
 * format.c
 *
 *			  error/warning message formatting
 *
 * by Pavel Stehule 2013-2023
 *
 *-------------------------------------------------------------------------
 */

#include <math.h>

#include "plpgsql_check.h"

#include "access/htup_details.h"
#include "mb/pg_wchar.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/xml.h"

static void put_text_line(plpgsql_check_result_info *ri, const char *message, int len);
static const char * error_level_str(int level);
static void init_tag(plpgsql_check_result_info *ri, Oid fn_oid);
static void close_and_save(plpgsql_check_result_info *ri);

static void put_error_text(plpgsql_check_result_info *ri, PLpgSQL_execstate *estate, int sqlerrcode, int lineno,
	const char *message, const char *detail, const char *hint, int level, int position, const char *query, const char *context);

static void format_error_xml(StringInfo str, PLpgSQL_execstate *estate, int sqlerrcode, int lineno,
	const char *message, const char *detail, const char *hint, int level, int position, const char *query, const char *context);

static void format_error_json(StringInfo str, PLpgSQL_execstate *estate, int sqlerrcode, int lineno,
	const char *message, const char *detail, const char *hint, int level, int position, const char *query, const char *context);

static void put_error_tabular(plpgsql_check_result_info *ri, PLpgSQL_execstate *estate, Oid fn_oid, int sqlerrcode, int lineno,
	const char *message, const char *detail, const char *hint, int level, int position, const char *query, const char *context);

/*
 * columns of plpgsql_check_function_table result
 *
 */
#define Natts_result					11

#define Anum_result_functionid			0
#define Anum_result_lineno			1
#define Anum_result_statement			2
#define Anum_result_sqlstate			3
#define Anum_result_message			4
#define Anum_result_detail			5
#define Anum_result_hint			6
#define Anum_result_level			7
#define Anum_result_position			8
#define Anum_result_query			9
#define Anum_result_context			10

/*
 * columns of plpgsql_show_dependency_tb result 
 *
 */
#define Natts_dependency				5

#define Anum_dependency_type			0
#define Anum_dependency_oid				1
#define Anum_dependency_schema			2
#define Anum_dependency_name			3
#define Anum_dependency_params			4

/*
 * columns of plpgsql_profiler_function_tb result
 *
 */
#define Natts_profiler					11

#define Anum_profiler_lineno			0
#define Anum_profiler_stmt_lineno		1
#define Anum_profiler_queryid			2
#define Anum_profiler_cmds_on_row		3
#define Anum_profiler_exec_count		4
#define Anum_profiler_exec_count_err	5
#define Anum_profiler_total_time		6
#define Anum_profiler_avg_time			7
#define Anum_profiler_max_time			8
#define Anum_profiler_processed_rows	9
#define Anum_profiler_source			10

/*
 * columns of plpgsql_profiler_function_statements_tb result
 *
 */
#define Natts_profiler_statements					13

#define Anum_profiler_statements_stmtid				0
#define Anum_profiler_statements_parent_stmtid		1
#define Anum_profiler_statements_parent_note		2
#define Anum_profiler_statements_block_num			3
#define Anum_profiler_statements_lineno				4
#define Anum_profiler_statements_queryid			5
#define Anum_profiler_statements_exec_stmts			6
#define Anum_profiler_statements_exec_stmts_err		7
#define Anum_profiler_statements_total_time			8
#define Anum_profiler_statements_avg_time			9
#define Anum_profiler_statements_max_time			10
#define Anum_profiler_statements_processed_rows		11
#define Anum_profiler_statements_stmtname			12

/*
 * columns of plpgsql_profiler_functions_all_tb result
 *
 */
#define Natts_profiler_functions_all_tb		8

#define Anum_profiler_functions_all_funcoid			0
#define Anum_profiler_functions_all_exec_count		1
#define Anum_profiler_functions_all_exec_count_err	2
#define Anum_profiler_functions_all_total_time		3
#define Anum_profiler_functions_all_avg_time		4
#define Anum_profiler_functions_all_stddev_time		5
#define Anum_profiler_functions_all_min_time		6
#define Anum_profiler_functions_all_max_time		7


#define SET_RESULT_NULL(anum) \
	do { \
		values[(anum)] = (Datum) 0; \
		nulls[(anum)] = true; \
	} while (0)

#define SET_RESULT(anum, value) \
	do { \
		values[(anum)] = (value); \
		nulls[(anum)] = false; \
	} while(0)

#define SET_RESULT_TEXT(anum, str) \
	do { \
		if (str != NULL) \
		{ \
			SET_RESULT((anum), CStringGetTextDatum((str))); \
		} \
		else \
		{ \
			SET_RESULT_NULL(anum); \
		} \
	} while (0)

#define SET_RESULT_INT32(anum, ival)	SET_RESULT((anum), Int32GetDatum((ival)))
#define SET_RESULT_INT64(anum, ival)	SET_RESULT((anum), Int64GetDatum((ival)))
#if PG_VERSION_NUM >= 110000
#define SET_RESULT_QUERYID(anum, ival)	SET_RESULT((anum), UInt64GetDatum((ival)))
#else
#define SET_RESULT_QUERYID(anum, ival)	SET_RESULT((anum), UInt32GetDatum((ival)))
#endif
#define SET_RESULT_OID(anum, oid)		SET_RESULT((anum), ObjectIdGetDatum((oid)))
#define SET_RESULT_FLOAT8(anum, fval)	SET_RESULT((anum), Float8GetDatum(fval))


/*
 * Translate name of format to number of format
 *
 */
int
plpgsql_check_format_num(char *format_str)
{
	int		result;

	char *format_lower_str = lowerstr(format_str);

	if (strcmp(format_lower_str, "text") == 0)
		result = PLPGSQL_CHECK_FORMAT_TEXT;
	else if (strcmp(format_lower_str, "xml") == 0)
		result = PLPGSQL_CHECK_FORMAT_XML;
	else if (strcmp(format_lower_str, "json") == 0)
		result = PLPGSQL_CHECK_FORMAT_JSON;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognize format: \"%s\"",
									 format_str),
			errhint("Only \"text\", \"xml\" and \"json\" formats are supported.")));

	return result;
}

/*
 * Prepare storage and formats for result
 *
 */
void
plpgsql_check_init_ri(plpgsql_check_result_info *ri,
					  int format,
					  ReturnSetInfo *rsinfo)
{
	int			natts;
	MemoryContext	per_query_ctx;
	MemoryContext	oldctx;

	ri->format = format;
	ri->sinfo = NULL;

	switch (format)
	{
		case PLPGSQL_CHECK_FORMAT_TEXT:
		case PLPGSQL_CHECK_FORMAT_XML:
		case PLPGSQL_CHECK_FORMAT_JSON:
			natts = 1;
			break;
		case PLPGSQL_CHECK_FORMAT_TABULAR:
			natts = Natts_result;
			break;
		case PLPGSQL_SHOW_DEPENDENCY_FORMAT_TABULAR:
			natts = Natts_dependency;
			break;
		case PLPGSQL_SHOW_PROFILE_TABULAR:
			natts = Natts_profiler;
			break;
		case PLPGSQL_SHOW_PROFILE_STATEMENTS_TABULAR:
			natts = Natts_profiler_statements;
			break;
		case PLPGSQL_SHOW_PROFILE_FUNCTIONS_ALL_TABULAR:
			natts = Natts_profiler_functions_all_tb;
			break;
		default:
			elog(ERROR, "unknown format %d", format);
	}

	ri->init_tag = format == PLPGSQL_CHECK_FORMAT_XML ||
				   format == PLPGSQL_CHECK_FORMAT_JSON;

	/* need to build tuplestore in query context */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldctx = MemoryContextSwitchTo(per_query_ctx);

	ri->tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
	ri->tuple_store = tuplestore_begin_heap(false, false, work_mem);
	ri->query_ctx = per_query_ctx;

	MemoryContextSwitchTo(oldctx);

	/* simple check of target */
	if (ri->tupdesc->natts != natts)
		elog(ERROR, "unexpected returning columns (%d instead %d)",
			 ri->tupdesc->natts,
			 natts);

	/* prepare rsinfo for this tuplestore result */
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = ri->tuple_store;
	rsinfo->setDesc = ri->tupdesc;
}

/*
 * When result is not empty, finalize result and close tuplestore.
 *
 */
void
plpgsql_check_finalize_ri(plpgsql_check_result_info *ri)
{
	if (ri->sinfo)
	{
		close_and_save(ri);

		pfree(ri->sinfo->data);
		pfree(ri->sinfo);
		ri->sinfo = NULL;
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(ri->tupstore);
}

/*
 * error message processing router
 */
static void
plpgsql_check_put_error_internal(PLpgSQL_checkstate *cstate,
					  int sqlerrcode,
					  int lineno,
					  const char *message,
					  const char *detail,
					  const char *hint,
					  int level,
					  int position,
					  const char *query,
					  const char *context)
{
	plpgsql_check_result_info *ri = cstate->result_info;
	PLpgSQL_execstate *estate = cstate->estate;

	if (context == NULL && estate && estate->err_text)
		context = estate->err_text;

	/* ignore warnings when is not requested */
	if ((level == PLPGSQL_CHECK_WARNING_PERFORMANCE && !cstate->cinfo->performance_warnings) ||
				(level == PLPGSQL_CHECK_WARNING_OTHERS && !cstate->cinfo->other_warnings) ||
				(level == PLPGSQL_CHECK_WARNING_EXTRA && !cstate->cinfo->extra_warnings) ||
				(level == PLPGSQL_CHECK_WARNING_SECURITY && !cstate->cinfo->security_warnings) ||
				(level == PLPGSQL_CHECK_WARNING_COMPATIBILITY && !cstate->cinfo->compatibility_warnings))
		return;

	if ((level == PLPGSQL_CHECK_WARNING_PERFORMANCE && cstate->pragma_vector.disable_performance_warnings) ||
			(level == PLPGSQL_CHECK_WARNING_OTHERS && cstate->pragma_vector.disable_other_warnings) ||
			(level == PLPGSQL_CHECK_WARNING_EXTRA && cstate->pragma_vector.disable_extra_warnings) ||
			(level == PLPGSQL_CHECK_WARNING_SECURITY && cstate->pragma_vector.disable_security_warnings) ||
			(level == PLPGSQL_CHECK_WARNING_COMPATIBILITY && cstate->pragma_vector.disable_compatibility_warnings))
		return;

	if (cstate->pragma_vector.disable_check)
		return;

	if (ri->init_tag)
	{
		init_tag(ri, cstate->cinfo->fn_oid);
		ri->init_tag = false;
	}

	if (ri->tuple_store)
	{
		switch (ri->format)
		{
			case PLPGSQL_CHECK_FORMAT_TABULAR:
				put_error_tabular(ri, estate, cstate->cinfo->fn_oid,
								  sqlerrcode, lineno, message, detail,
								  hint, level, position, query, context);
				break;

			case PLPGSQL_CHECK_FORMAT_TEXT:
				put_error_text(ri, estate,
							   sqlerrcode, lineno, message, detail,
							   hint, level, position, query, context);
				break;

			case PLPGSQL_CHECK_FORMAT_XML:
				format_error_xml(ri->sinfo, estate,
								 sqlerrcode, lineno, message, detail,
								 hint, level, position, query, context);
				break;

			case PLPGSQL_CHECK_FORMAT_JSON:
				format_error_json(ri->sinfo, estate,
								  sqlerrcode, lineno, message, detail,
								  hint, level, position, query, context);
			break;
		}

		/* stop checking if it is necessary */
		if (level == PLPGSQL_CHECK_ERROR && cstate->cinfo->fatal_errors)
			cstate->stop_check = true;

	}
	else
	{
		int elevel;

		/*
		 * when a passive mode is active and fatal_errors is false, then
		 * raise warning everytime.
		 */
		if (!cstate->is_active_mode && !cstate->cinfo->fatal_errors)
			elevel = WARNING;
		else
			elevel = level == PLPGSQL_CHECK_ERROR ? ERROR : WARNING;

		/* use error fields as parameters of postgres exception */
		ereport(elevel,
				(sqlerrcode ? errcode(sqlerrcode) : 0,
				 errmsg_internal("%s", message),
				 (detail != NULL) ? errdetail_internal("%s", detail) : 0,
				 (hint != NULL) ? errhint("%s", hint) : 0,
				 (query != NULL) ? internalerrquery(query) : 0,
				 (position != 0) ? internalerrposition(position) : 0,
				 (context != NULL) ? errcontext("%s", context) : 0));
	}
}

/*
 * store edata
 */
void
plpgsql_check_put_error_edata(PLpgSQL_checkstate *cstate,
							  ErrorData *edata)
{
	plpgsql_check_put_error_internal(cstate,
									 edata->sqlerrcode,
									 edata->lineno,
									 edata->message,
									 edata->detail,
									 edata->hint,
									 PLPGSQL_CHECK_ERROR,
									 edata->internalpos,
									 edata->internalquery,
									 edata->context);
}

void
plpgsql_check_put_error(PLpgSQL_checkstate *cstate,
					  int sqlerrcode,
					  int lineno,
					  const char *message,
					  const char *detail,
					  const char *hint,
					  int level,
					  int position,
					  const char *query,
					  const char *context)
{
	/*
	 * Trapped internal errors has transformed position. The plpgsql_check
	 * errors (and warnings) have to have same transformation for position
	 * to correct display caret (for trapped and reraised, and raised errors)
	 */
	if (position != -1 && query)
		position = pg_mbstrlen_with_len(query, position) + 1;

	plpgsql_check_put_error_internal(cstate,
							sqlerrcode,
							lineno,
							message,
							detail,
							hint,
							level,
							position,
							query,
							context);
}

/*
 * Append text line (StringInfo) to one column tuple store
 *
 */
static void
put_text_line(plpgsql_check_result_info *ri, const char *message, int len)
{
	Datum           value;
	bool            isnull = false;
	HeapTuple       tuple;

	if (len >= 0)
		value = PointerGetDatum(cstring_to_text_with_len(message, len));
	else
		value = PointerGetDatum(cstring_to_text(message));

	tuple = heap_form_tuple(ri->tupdesc, &value, &isnull);
	tuplestore_puttuple(ri->tuple_store, tuple);
}

static const char *
error_level_str(int level)
{
	switch (level)
	{
		case PLPGSQL_CHECK_ERROR:
			return "error";
		case PLPGSQL_CHECK_WARNING_OTHERS:
			return "warning";
		case PLPGSQL_CHECK_WARNING_EXTRA:
			return "warning extra";
		case PLPGSQL_CHECK_WARNING_PERFORMANCE:
			return "performance";
		case PLPGSQL_CHECK_WARNING_SECURITY:
			return "security";
		case PLPGSQL_CHECK_WARNING_COMPATIBILITY:
			return "compatibility";
		default:
			return "???";
	}
}

/*
 * collects errors and warnings in plain text format
 */
static void
put_error_text(plpgsql_check_result_info *ri,
			   PLpgSQL_execstate *estate,
			   int sqlerrcode,
			   int lineno,
			   const char *message,
			   const char *detail,
			   const char *hint,
			   int level,
			   int position,
			   const char *query,
			   const char *context)
{
	StringInfoData  sinfo;
	const char *level_str = error_level_str(level);

	Assert(message != NULL);

	initStringInfo(&sinfo);

	/* lineno should be valid for actual statements */
	if (estate != NULL && estate->err_stmt != NULL && estate->err_stmt->lineno > 0)
		appendStringInfo(&sinfo, "%s:%s:%d:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 estate->err_stmt->lineno,
				 plpgsql_check__stmt_typename_p(estate->err_stmt),
				 message);
	else if (strncmp(message, UNUSED_VARIABLE_TEXT, UNUSED_VARIABLE_TEXT_CHECK_LENGTH) == 0)
	{
		appendStringInfo(&sinfo, "%s:%s:%d:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 lineno,
				 "DECLARE",
				 message);
	}
	else if (strncmp(message, NEVER_READ_VARIABLE_TEXT, NEVER_READ_VARIABLE_TEXT_CHECK_LENGTH) == 0)
	{
		appendStringInfo(&sinfo, "%s:%s:%d:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 lineno,
				 "DECLARE",
				 message);
	}
	else
	{
		appendStringInfo(&sinfo, "%s:%s:%s",
				 level_str,
				 unpack_sql_state(sqlerrcode),
				 message);
	}

	put_text_line(ri, sinfo.data, sinfo.len);
	resetStringInfo(&sinfo);

	if (query != NULL) 
	{
		char           *query_line;	/* pointer to beginning of current line */
		int             line_caret_pos;
		bool            is_first_line = true;
		char           *_query = pstrdup(query);
		char           *ptr;

		ptr = _query;
		query_line = ptr;
		line_caret_pos = position;

		while (*ptr != '\0')
		{
			/* search end of lines and replace '\n' by '\0' */
			if (*ptr == '\n')
			{
				*ptr = '\0';
				if (is_first_line)
				{
					appendStringInfo(&sinfo, "Query: %s", query_line);
					is_first_line = false;
				} else
					appendStringInfo(&sinfo, "       %s", query_line);

				put_text_line(ri, sinfo.data, sinfo.len);
				resetStringInfo(&sinfo);

				if (line_caret_pos > 0 && position == 0)
				{
					appendStringInfo(&sinfo, "--     %*s",
						       line_caret_pos, "^");

					put_text_line(ri, sinfo.data, sinfo.len);
					resetStringInfo(&sinfo);

					line_caret_pos = 0;
				}
				/* store caret position offset for next line */

				if (position > 1)
					line_caret_pos = position - 1;

				/* go to next line */
				query_line = ptr + 1;
			}
			ptr += pg_mblen(ptr);

			if (position > 0)
				position--;
		}

		/* flush last line */
		if (query_line != NULL)
		{
			if (is_first_line)
				appendStringInfo(&sinfo, "Query: %s", query_line);
			else
				appendStringInfo(&sinfo, "       %s", query_line);

			put_text_line(ri, sinfo.data, sinfo.len);
			resetStringInfo(&sinfo);

			if (line_caret_pos > 0 && position == 0)
			{
				appendStringInfo(&sinfo, "--     %*s",
						 line_caret_pos, "^");
				put_text_line(ri, sinfo.data, sinfo.len);
				resetStringInfo(&sinfo);
			}
		}

		pfree(_query);
	}

	if (detail != NULL)
	{
		appendStringInfo(&sinfo, "Detail: %s", detail);
		put_text_line(ri, sinfo.data, sinfo.len);
		resetStringInfo(&sinfo);
	}

	if (hint != NULL)
	{
		appendStringInfo(&sinfo, "Hint: %s", hint);
		put_text_line(ri, sinfo.data, sinfo.len);
		resetStringInfo(&sinfo);
	}

	if (context != NULL) 
	{
		appendStringInfo(&sinfo, "Context: %s", context);
		put_text_line(ri, sinfo.data, sinfo.len);
		resetStringInfo(&sinfo);
	}

	pfree(sinfo.data);
}

/*
 * Initialize StringInfo buffer with top tag
 *
 */
static void
init_tag(plpgsql_check_result_info *ri, Oid fn_oid)
{
	/* XML format requires StringInfo buffer */
	if (ri->format == PLPGSQL_CHECK_FORMAT_XML ||
		ri->format == PLPGSQL_CHECK_FORMAT_JSON)
	{
		if (ri->sinfo != NULL)
			resetStringInfo(ri->sinfo);
		else
		{
			MemoryContext	oldcxt;

			/* StringInfo should be created in some longer life context */
			oldcxt = MemoryContextSwitchTo(ri->query_ctx);

			ri->sinfo = makeStringInfo();

			MemoryContextSwitchTo(oldcxt);
		}

		if (ri->format == PLPGSQL_CHECK_FORMAT_XML)
		{
			/* create a initial tag */
			if (plpgsql_check_regress_test_mode)
				appendStringInfo(ri->sinfo, "<Function>\n");
			else
				appendStringInfo(ri->sinfo, "<Function oid=\"%d\">\n", fn_oid);
		}
		else if (ri->format == PLPGSQL_CHECK_FORMAT_JSON) {
			/* create a initial tag */
			if (plpgsql_check_regress_test_mode)
				appendStringInfo(ri->sinfo, "{ \"issues\":[\n");
			else
				appendStringInfo(ri->sinfo, "{ \"function\":\"%d\",\n\"issues\":[\n", fn_oid);
		}
	}
}

/*
 * Append close tag and store document
 *
 */
static void
close_and_save(plpgsql_check_result_info *ri)
{
	if (ri->format == PLPGSQL_CHECK_FORMAT_XML)
	{
		appendStringInfoString(ri->sinfo, "</Function>");

		put_text_line(ri, ri->sinfo->data, ri->sinfo->len);
	}
	else if (ri->format == PLPGSQL_CHECK_FORMAT_JSON)
	{
		if (ri->sinfo->len > 1 && ri->sinfo->data[ri->sinfo->len -1] == ',') {
			ri->sinfo->data[ri->sinfo->len - 1] = '\n';
		}
		appendStringInfoString(ri->sinfo, "\n]\n}");

		put_text_line(ri, ri->sinfo->data, ri->sinfo->len);
	}
}

/*
 * format_error_xml formats and collects a identifided issues
 */
static void
format_error_xml(StringInfo str,
						  PLpgSQL_execstate *estate,
								 int sqlerrcode,
								 int lineno,
								 const char *message,
								 const char *detail,
								 const char *hint,
								 int level,
								 int position,
								 const char *query,
								 const char *context)
{
	const char *level_str = error_level_str(level);

	Assert(message != NULL);

	/* flush tag */
	appendStringInfoString(str, "  <Issue>\n");

	appendStringInfo(str, "    <Level>%s</Level>\n", level_str);
	appendStringInfo(str, "    <Sqlstate>%s</Sqlstate>\n",
						 unpack_sql_state(sqlerrcode));
	appendStringInfo(str, "    <Message>%s</Message>\n",
							 escape_xml(message));
	if (estate != NULL && estate->err_stmt != NULL)
		appendStringInfo(str, "    <Stmt lineno=\"%d\">%s</Stmt>\n",
				 estate->err_stmt->lineno,
			   plpgsql_check__stmt_typename_p(estate->err_stmt));

	else if (strcmp(message, "unused declared variable") == 0)
		appendStringInfo(str, "    <Stmt lineno=\"%d\">DECLARE</Stmt>\n",
				 lineno);

	if (hint != NULL)
		appendStringInfo(str, "    <Hint>%s</Hint>\n",
								 escape_xml(hint));
	if (detail != NULL)
		appendStringInfo(str, "    <Detail>%s</Detail>\n",
								 escape_xml(detail));
	if (query != NULL)
		appendStringInfo(str, "    <Query position=\"%d\">%s</Query>\n",
							 position, escape_xml(query));

	if (context != NULL)
		appendStringInfo(str, "    <Context>%s</Context>\n",
							 escape_xml(context));

	/* flush closing tag */
	appendStringInfoString(str, "  </Issue>\n");
}

/*
* format_error_json formats and collects a identifided issues
*/
static void
format_error_json(StringInfo str,
	PLpgSQL_execstate *estate,
	int sqlerrcode,
	int lineno,
	const char *message,
	const char *detail,
	const char *hint,
	int level,
	int position,
	const char *query,
	const char *context)
{
	const char *level_str = error_level_str(level);
	StringInfoData sinfo; /*Holds escaped json*/

	Assert(message != NULL);

	initStringInfo(&sinfo);

	/* flush tag */
	appendStringInfoString(str, "  {\n");
	appendStringInfo(str, "    \"level\":\"%s\",\n", level_str);
		
	escape_json(&sinfo, message);
	appendStringInfo(str, "    \"message\":%s,\n", sinfo.data);
	if (estate != NULL && estate->err_stmt != NULL)
		appendStringInfo(str, "    \"statement\":{\n\"lineNumber\":\"%d\",\n\"text\":\"%s\"\n},\n",
			estate->err_stmt->lineno,
			plpgsql_check__stmt_typename_p(estate->err_stmt));

	else if (strcmp(message, "unused declared variable") == 0)
		appendStringInfo(str, "    \"statement\":{\n\"lineNumber\":\"%d\",\n\"text\":\"DECLARE\"\n},",
			lineno);

	if (hint != NULL) {
		resetStringInfo(&sinfo);
		escape_json(&sinfo, hint);
		appendStringInfo(str, "    \"hint\":%s,\n", sinfo.data);
	}
	if (detail != NULL) {
		resetStringInfo(&sinfo);
		escape_json(&sinfo, detail);
		appendStringInfo(str, "    \"detail\":%s,\n", sinfo.data);
	}
	if (query != NULL) {
		resetStringInfo(&sinfo);
		escape_json(&sinfo, query);
		appendStringInfo(str, "    \"query\":{\n\"position\":\"%d\",\n\"text\":%s\n},\n", position, sinfo.data);
	}

	if (context != NULL) {
		resetStringInfo(&sinfo);
		escape_json(&sinfo, context);
		appendStringInfo(str, "    \"context\":%s,\n", sinfo.data);
	}

	/* placing this property last as to avoid a trailing comma*/
	appendStringInfo(str, "    \"sqlState\":\"%s\"\n",	unpack_sql_state(sqlerrcode));

	/* flush closing tag. Needs comman jus in case there is more than one issue. Comma removed in epilog */
	appendStringInfoString(str, "  },");
}

/*
 * store error fields to result tuplestore
 *
 */
static void
put_error_tabular(plpgsql_check_result_info *ri,
				  PLpgSQL_execstate *estate,
				  Oid fn_oid,
				  int sqlerrcode,
				  int lineno,
				  const char *message,
				  const char *detail,
				  const char *hint,
				  int level,
				  int position,
				  const char *query,
				  const char *context)
{
	Datum	values[Natts_result];
	bool	nulls[Natts_result];

	Assert(ri->tuple_store);
	Assert(ri->tupdesc);

	Assert(message != NULL);

	SET_RESULT_OID(Anum_result_functionid, fn_oid);

	/* lineno should be valid */
	if (estate != NULL && estate->err_stmt != NULL && estate->err_stmt->lineno > 0)
	{
		/* use lineno based on err_stmt */
		SET_RESULT_INT32(Anum_result_lineno, estate->err_stmt->lineno);
		SET_RESULT_TEXT(Anum_result_statement, plpgsql_check__stmt_typename_p(estate->err_stmt));
	}
	else if (strncmp(message, UNUSED_VARIABLE_TEXT, UNUSED_VARIABLE_TEXT_CHECK_LENGTH) == 0)
	{
		SET_RESULT_INT32(Anum_result_lineno, lineno);
		SET_RESULT_TEXT(Anum_result_statement, "DECLARE");
	}
	else if (strncmp(message, NEVER_READ_VARIABLE_TEXT, NEVER_READ_VARIABLE_TEXT_CHECK_LENGTH) == 0)
	{
		SET_RESULT_INT32(Anum_result_lineno, lineno);
		SET_RESULT_TEXT(Anum_result_statement, "DECLARE");
	}

	else
	{
		SET_RESULT_NULL(Anum_result_lineno);
		SET_RESULT_NULL(Anum_result_statement);
	} 

	SET_RESULT_TEXT(Anum_result_sqlstate, unpack_sql_state(sqlerrcode));
	SET_RESULT_TEXT(Anum_result_message, message);
	SET_RESULT_TEXT(Anum_result_detail, detail);
	SET_RESULT_TEXT(Anum_result_hint, hint);
	SET_RESULT_TEXT(Anum_result_level, error_level_str(level));

	if (position != 0)
		SET_RESULT_INT32(Anum_result_position, position);
	else
		SET_RESULT_NULL(Anum_result_position);

	SET_RESULT_TEXT(Anum_result_query, query);
	SET_RESULT_TEXT(Anum_result_context, context);

	tuplestore_putvalues(ri->tuple_store, ri->tupdesc, values, nulls);
}

/*
 * Store one output row of dependency view to result tuplestore
 *
 */
void
plpgsql_check_put_dependency(plpgsql_check_result_info *ri,
							 char *type,
							 Oid oid,
							 char *schema,
							 char *name,
							 char *params)
{
	Datum	values[Natts_dependency];
	bool	nulls[Natts_dependency];

	Assert(ri->tuple_store);
	Assert(ri->tupdesc);

	SET_RESULT_TEXT(Anum_dependency_type, type);
	SET_RESULT_OID(Anum_dependency_oid, oid);
	SET_RESULT_TEXT(Anum_dependency_schema, schema);
	SET_RESULT_TEXT(Anum_dependency_name, name);
	SET_RESULT_TEXT(Anum_dependency_params, params);

	tuplestore_putvalues(ri->tuple_store, ri->tupdesc, values, nulls);
}

/*
 * Store one output row of profiler to result tuplestore
 *
 */
void
plpgsql_check_put_profile(plpgsql_check_result_info *ri,
						  Datum queryids_array,
						  int lineno,
						  int stmt_lineno,
						  int cmds_on_row,
						  int64 exec_count,
						  int64 exec_count_err,
						  int64 us_total,
						  Datum max_time_array,
						  Datum processed_rows_array,
						  char *source_row)
{
	Datum	values[Natts_profiler];
	bool	nulls[Natts_profiler];

	Assert(ri->tuple_store);
	Assert(ri->tupdesc);

	SET_RESULT_NULL(Anum_profiler_stmt_lineno);
	SET_RESULT_NULL(Anum_profiler_queryid);
	SET_RESULT_NULL(Anum_profiler_exec_count);
	SET_RESULT_NULL(Anum_profiler_exec_count_err);
	SET_RESULT_NULL(Anum_profiler_total_time);
	SET_RESULT_NULL(Anum_profiler_avg_time);
	SET_RESULT_NULL(Anum_profiler_max_time);
	SET_RESULT_NULL(Anum_profiler_processed_rows);
	SET_RESULT_NULL(Anum_profiler_source);
	SET_RESULT_NULL(Anum_profiler_cmds_on_row);

	SET_RESULT_INT32(Anum_profiler_lineno, lineno);
	SET_RESULT_TEXT(Anum_profiler_source, source_row);

	if (stmt_lineno > 0)
	{
		SET_RESULT_INT32(Anum_profiler_stmt_lineno, stmt_lineno);
		if (queryids_array != (Datum) 0)
			SET_RESULT(Anum_profiler_queryid, queryids_array);
		SET_RESULT_INT32(Anum_profiler_cmds_on_row, cmds_on_row);
		SET_RESULT_INT64(Anum_profiler_exec_count, exec_count);
		SET_RESULT_INT64(Anum_profiler_exec_count_err, exec_count_err);
		SET_RESULT_FLOAT8(Anum_profiler_total_time, us_total / 1000.0);
		SET_RESULT_FLOAT8(Anum_profiler_avg_time, ceil(((float8) us_total) / exec_count) / 1000.0);
		SET_RESULT(Anum_profiler_max_time, max_time_array);
		SET_RESULT(Anum_profiler_processed_rows, processed_rows_array);
	}

	tuplestore_putvalues(ri->tuple_store, ri->tupdesc, values, nulls);
}

/*
 * Store one output row of profiler to result tuplestore in statement 
 * oriented format
 *
 */
void
plpgsql_check_put_profile_statement(plpgsql_check_result_info *ri,
									pc_queryid queryid,
									int stmtid,
									int parent_stmtid,
									const char *parent_note,
									int block_num,
									int lineno,
									int64 exec_stmts,
									int64 exec_stmts_err,
									double total_time,
									double max_time,
									int64 processed_rows,
									char *stmtname)
{
	Datum	values[Natts_profiler_statements];
	bool	nulls[Natts_profiler_statements];

	Assert(ri->tuple_store);
	Assert(ri->tupdesc);

	/* ignore invisible statements */
	if (lineno <= 0)
		return;

	SET_RESULT_INT32(Anum_profiler_statements_stmtid, stmtid);
	SET_RESULT_INT32(Anum_profiler_statements_block_num, block_num);
	SET_RESULT_INT32(Anum_profiler_statements_lineno, lineno);

	if (queryid == NOQUERYID)
		SET_RESULT_NULL(Anum_profiler_statements_queryid);
	else
		SET_RESULT_QUERYID(Anum_profiler_statements_queryid, queryid);

	SET_RESULT_INT64(Anum_profiler_statements_exec_stmts, exec_stmts);
	SET_RESULT_INT64(Anum_profiler_statements_exec_stmts_err, exec_stmts_err);
	SET_RESULT_INT64(Anum_profiler_statements_processed_rows, processed_rows);
	SET_RESULT_FLOAT8(Anum_profiler_statements_total_time, total_time / 1000.0);
	SET_RESULT_FLOAT8(Anum_profiler_statements_total_time, total_time / 1000.0);
	SET_RESULT_FLOAT8(Anum_profiler_statements_max_time, max_time / 1000.0);
	SET_RESULT_TEXT(Anum_profiler_statements_stmtname, stmtname);

	if (parent_note)
		SET_RESULT_TEXT(Anum_profiler_statements_parent_note, parent_note);
	else
		SET_RESULT_NULL(Anum_profiler_statements_parent_note);

	/* set nullable field */
	if (parent_stmtid == -1)
		SET_RESULT_NULL(Anum_profiler_statements_parent_stmtid);
	else
		SET_RESULT_INT32(Anum_profiler_statements_parent_stmtid, parent_stmtid);

	if (exec_stmts > 0)
		SET_RESULT_FLOAT8(Anum_profiler_statements_avg_time, ceil(((float8) total_time) / exec_stmts) / 1000.0);
	else
		SET_RESULT_NULL(Anum_profiler_statements_avg_time);

	tuplestore_putvalues(ri->tuple_store, ri->tupdesc, values, nulls);
}

void
plpgsql_check_put_profiler_functions_all_tb(plpgsql_check_result_info *ri,
											Oid funcoid,
											int64 exec_count,
											int64 exec_count_err,
											double total_time,
											double avg_time,
											double stddev_time,
											double min_time,
											double max_time)
{
	Datum	values[Natts_profiler_functions_all_tb];
	bool	nulls[Natts_profiler_functions_all_tb];

	Assert(ri->tuple_store);
	Assert(ri->tupdesc);

	SET_RESULT_OID(Anum_profiler_functions_all_funcoid, funcoid);
	SET_RESULT_INT64(Anum_profiler_functions_all_exec_count, exec_count);
	SET_RESULT_INT64(Anum_profiler_functions_all_exec_count_err, exec_count_err);
	SET_RESULT_FLOAT8(Anum_profiler_functions_all_total_time, total_time / 1000.0);
	SET_RESULT_FLOAT8(Anum_profiler_functions_all_avg_time, avg_time / 1000.0);
	SET_RESULT_FLOAT8(Anum_profiler_functions_all_stddev_time, stddev_time / 1000.0);
	SET_RESULT_FLOAT8(Anum_profiler_functions_all_min_time, min_time / 1000.0);
	SET_RESULT_FLOAT8(Anum_profiler_functions_all_max_time, max_time / 1000.0);

	tuplestore_putvalues(ri->tuple_store, ri->tupdesc, values, nulls);
}
