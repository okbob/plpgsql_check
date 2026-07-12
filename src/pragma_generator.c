/*-------------------------------------------------------------------------
 *
 * pragma_generator.c
 *
 *			  generator of table pragmas for CREATE TEMP TABLE AS
 *			  statements used in function's body
 *
 * by Pavel Stehule 2013-2026
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "parser/parser.h"
#include "utils/builtins.h"

static void generate_pragmas_stmt_walker(PLpgSQL_stmt *stmt, void *context);
static void process_stmt_query(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt,
							   PLpgSQL_expr *expr, bool is_perform_stmt);
static void process_create_table_as(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt,
									PLpgSQL_expr *expr, CreateTableAsStmt *ctas);
static char *build_table_pragma_def(const char *relname, List *colNames, List *targetList);
static bool is_temp_range_var(RangeVar *rel);
static void check_temp_schema(RangeVar *rel);

/*
 * Returns true, when a range var describes a temporary table. The
 * persistence can be entered explicitly (TEMP or TEMPORARY keywords),
 * or implicitly by using of "pg_temp" schema.
 */
static bool
is_temp_range_var(RangeVar *rel)
{
	if (rel->relpersistence == RELPERSISTENCE_TEMP)
		return true;

	return rel->schemaname && strcmp(rel->schemaname, "pg_temp") == 0;
}

/*
 * Emulates the check of target schema of a temporary table done by
 * RangeVarAdjustRelationPersistence routine. We cannot to use this
 * routine directly, because it requires an existing schema, and we
 * don't want to create the temporary schema as a side effect.
 */
static void
check_temp_schema(RangeVar *rel)
{
	if (rel->relpersistence == RELPERSISTENCE_TEMP &&
		rel->schemaname &&
		strcmp(rel->schemaname, "pg_temp") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot create temporary relation in non-temporary schema")));
}

/*
 * Returns definition of a table (usable by table pragma) derived from
 * target list of an analyzed query. The names of columns can be
 * overwritten by explicitly entered column names from CREATE TABLE AS
 * statement.
 */
static char *
build_table_pragma_def(const char *relname, List *colNames, List *targetList)
{
	StringInfoData def;
	ListCell   *lc;
	ListCell   *colname_item;
	bool		is_first = true;

	initStringInfo(&def);
	appendStringInfo(&def, "%s(", quote_identifier(relname));

	colname_item = list_head(colNames);

	foreach(lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		const char *colname;
		Oid			typid;
		int32		typmod;

		if (tle->resjunk)
			continue;

		if (colname_item)
		{
			colname = strVal(lfirst(colname_item));
			colname_item = lnext(colNames, colname_item);
		}
		else
			colname = tle->resname ? tle->resname : "?column?";

		typid = exprType((Node *) tle->expr);
		typmod = exprTypmod((Node *) tle->expr);

		/* like CREATE TABLE AS does, replace unknown type by text */
		if (typid == UNKNOWNOID)
		{
			typid = TEXTOID;
			typmod = -1;
		}

		if (!is_first)
			appendStringInfoString(&def, ", ");

		appendStringInfo(&def, "%s %s",
						 quote_identifier(colname),
						 format_type_with_typemod(typid, typmod));

		is_first = false;
	}

	/* be consistent with CREATE TABLE AS behaviour */
	if (colname_item)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	appendStringInfoChar(&def, ')');

	return def.data;
}

/*
 * Generates (and returns as a result row) the table pragma for one
 * CREATE TEMP TABLE AS statement. The pragma is applied immediately
 * too, so the temporary table is visible for planning of the following
 * statements.
 */
static void
process_create_table_as(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt,
						PLpgSQL_expr *expr, CreateTableAsStmt *ctas)
{
	CachedPlanSource *plansource;
	CreateTableAsStmt *ctas_analyzed;
	Query	   *query;
	Query	   *inner_query;
	char	   *def;

	/* materialized views cannot be temporary */
	if (ctas->objtype != OBJECT_TABLE)
		return;

	if (!ctas->into || !ctas->into->rel)
		return;

	if (!is_temp_range_var(ctas->into->rel))
		return;

	check_temp_schema(ctas->into->rel);

	/* CREATE TABLE AS EXECUTE is not supported */
	if (ctas->query && IsA(ctas->query, ExecuteStmt))
		return;

	/*
	 * Plan the statement. The parse analysis transforms an inner query of
	 * CREATE TABLE AS statement, so names and types of columns can be read
	 * from the target list without execution.
	 */
	plpgsql_check_expr_prepare_plan(cstate, expr);

	plansource = plpgsql_check_get_plan_source(cstate, expr->plan);
	if (!plansource || !plansource->query_list)
		return;

	query = linitial_node(Query, plansource->query_list);
	if (query->commandType != CMD_UTILITY ||
		!query->utilityStmt ||
		!IsA(query->utilityStmt, CreateTableAsStmt))
		return;

	ctas_analyzed = (CreateTableAsStmt *) query->utilityStmt;
	if (!ctas_analyzed->query || !IsA(ctas_analyzed->query, Query))
		return;

	inner_query = (Query *) ctas_analyzed->query;
	if (inner_query->commandType != CMD_SELECT)
		return;

	def = build_table_pragma_def(ctas->into->rel->relname,
								 ctas->into->colNames,
								 inner_query->targetList);

	plpgsql_check_put_text_line(cstate->result_info,
								psprintf("table: %s", def), -1);

	/* make the temporary table visible for following statements */
	plpgsql_check_pragma_table(cstate, def, stmt->lineno);
}

/*
 * Processes one SQL statement of function's body. When the statement
 * creates a temporary table, then the table pragma is generated for it
 * (and applied). Explicitly written pragmas are applied too, so a
 * manually declared temporary table is visible for planning of the
 * following statements.
 */
static void
process_stmt_query(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt,
				   PLpgSQL_expr *expr, bool is_perform_stmt)
{
	MemoryContext oldCxt;
	ResourceOwner oldowner;

	if (!expr || !expr->query)
		return;

	cstate->estate->err_stmt = stmt;

	oldCxt = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(cstate->check_cxt);

	PG_TRY();
	{
		List	   *parsetree;

		parsetree = raw_parser(expr->query, expr->parseMode);

		if (list_length(parsetree) == 1)
		{
			Node	   *node = linitial_node(RawStmt, parsetree)->stmt;

			if (IsA(node, CreateTableAsStmt))
			{
				process_create_table_as(cstate, stmt, expr,
										(CreateTableAsStmt *) node);
			}
			else if (IsA(node, CreateStmt))
			{
				CreateStmt *cstmt = (CreateStmt *) node;

				/*
				 * The table pragma cannot be generated from CREATE TABLE
				 * statement (column definition list, LIKE clause or OF type
				 * clause), but the check of target schema should be done
				 * every time.
				 */
				if (is_temp_range_var(cstmt->relation))
					check_temp_schema(cstmt->relation);
			}
			else if (IsA(node, SelectStmt))
			{
				(void) plpgsql_check_apply_inline_pragmas(cstate,
														  (SelectStmt *) node,
														  expr->ns,
														  stmt->lineno,
														  is_perform_stmt);
			}
		}

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(cstate->check_cxt);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldCxt);
		CurrentResourceOwner = oldowner;

		if (cstate->cinfo->fatal_errors)
			ReThrowError(edata);

		/* the statement is ignored, and warning is raised instead */
		ereport(WARNING,
				(errcode(edata->sqlerrcode),
				 errmsg("pragma cannot be generated for statement on line %d", stmt->lineno),
				 errdetail("%s", edata->message)));

		FreeErrorData(edata);
	}
	PG_END_TRY();
}

/*
 * The walker of statement tree. The statements are visited in textual
 * order (pre-order). Only static SQL statements are processed - the
 * dynamic (EXECUTE) statements are ignored.
 */
static void
generate_pragmas_stmt_walker(PLpgSQL_stmt *stmt, void *context)
{
	PLpgSQL_checkstate *cstate = (PLpgSQL_checkstate *) context;

	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_EXECSQL:
			process_stmt_query(cstate, stmt,
							   ((PLpgSQL_stmt_execsql *) stmt)->sqlstmt,
							   false);
			break;

		case PLPGSQL_STMT_PERFORM:
			process_stmt_query(cstate, stmt,
							   ((PLpgSQL_stmt_perform *) stmt)->expr,
							   true);
			break;

		default:
			break;
	}

	plch_statement_tree_walker(stmt, generate_pragmas_stmt_walker, NULL, context);
}

/*
 * Scans the function's body and generates (and returns as result rows)
 * table pragmas for all CREATE TEMP TABLE AS statements there.
 */
void
plch_generate_table_pragmas_walk(PLpgSQL_checkstate *cstate, PLpgSQL_function *func)
{
	generate_pragmas_stmt_walker((PLpgSQL_stmt *) func->action, cstate);
}
