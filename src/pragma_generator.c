/*-------------------------------------------------------------------------
 *
 * pragma_generator.c
 *
 *			  generator of table pragmas for CREATE TEMP TABLE
 *			  statements used in function's body
 *
 * by Pavel Stehule 2013-2026
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql_check.h"

#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/rel.h"

static void make_pragma_stmt_walker(PLpgSQL_stmt *stmt, void *context);
static void process_stmt_query(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt,
							   PLpgSQL_expr *expr, bool is_perform_stmt);
static void process_create_table_as(PLpgSQL_checkstate *cstate, PLpgSQL_stmt *stmt,
									PLpgSQL_expr *expr, CreateTableAsStmt *ctas);
static void process_create_table(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
								 CreateStmt *cstmt);
static char *build_table_pragma_def(const char *relname, List *colNames, List *targetList);
static char *build_table_pragma_def_from_tupdesc(const char *relname, TupleDesc tupdesc);
static void emit_pragma_from_relid(PLpgSQL_checkstate *cstate, Oid relid);
static void drop_temp_table(const char *relname);
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
 * Returns definition of a table (usable by table pragma) derived from
 * a tuple descriptor of an existing relation.
 */
static char *
build_table_pragma_def_from_tupdesc(const char *relname, TupleDesc tupdesc)
{
	StringInfoData def;
	bool		is_first = true;
	int			i;

	initStringInfo(&def);
	appendStringInfo(&def, "%s(", quote_identifier(relname));

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		if (!is_first)
			appendStringInfoString(&def, ", ");

		appendStringInfo(&def, "%s %s",
						 quote_identifier(NameStr(att->attname)),
						 format_type_with_typemod(att->atttypid, att->atttypmod));

		is_first = false;
	}

	appendStringInfoChar(&def, ')');

	return def.data;
}

/*
 * Emits the table pragma derived from the structure of an existing
 * relation.
 */
static void
emit_pragma_from_relid(PLpgSQL_checkstate *cstate, Oid relid)
{
	Relation	rel;
	char	   *def;

	rel = table_open(relid, AccessShareLock);

	def = build_table_pragma_def_from_tupdesc(RelationGetRelationName(rel),
											  RelationGetDescr(rel));

	table_close(rel, AccessShareLock);

	plch_put_text_line(cstate->result_info,
					   psprintf("table: %s", def), -1);
}

/*
 * A name collision (usually the pattern CREATE, DROP, CREATE with same
 * name) is an artifact of static scanning - DROP statements are not
 * executed. We want to return pragmas for both definitions, so the
 * previous table is dropped (the drop is reverted by rollback of the
 * check's subtransaction), and the following statements will see the
 * last definition like in runtime.
 */
static void
drop_temp_table(const char *relname)
{
	ereport(WARNING,
			(errcode(ERRCODE_DUPLICATE_TABLE),
			 errmsg("relation \"%s\" already exists", relname),
			 errdetail("The temporary table is replaced by the new definition.")));

	if (SPI_execute(psprintf("DROP TABLE pg_temp.%s", quote_identifier(relname)),
					false, 0) != SPI_OK_UTILITY)
		elog(ERROR, "unexpected SPI result on DROP TABLE execution");
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
	Oid			relid;
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
	plch_expr_prepare_plan(cstate, expr);

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

	relid = RangeVarGetRelid(ctas->into->rel, NoLock, true);
	if (OidIsValid(relid))
	{
		/* like runtime does, the existing table wins */
		if (ctas->if_not_exists)
		{
			emit_pragma_from_relid(cstate, relid);
			return;
		}

		drop_temp_table(ctas->into->rel->relname);
	}

	plch_put_text_line(cstate->result_info,
					   psprintf("table: %s", def), -1);

	/* make the temporary table visible for following statements */
	plpgsql_check_pragma_table(cstate, def, stmt->lineno);
}

/*
 * Generates (and returns as a result row) the table pragma for one
 * CREATE TEMP TABLE statement - the column definition list form, the
 * LIKE clause, the OF type clause, inheritance or partitioning. The
 * original statement is executed - the temporary table is created
 * inside the check's subtransaction, that is always rolled back, so
 * no object survives the check. The execution is the only reliable
 * way how to get the table's structure - the LIKE clause, the OF type
 * clause, inheritance, partitioning or serial columns are expanded by
 * PostgreSQL itself. Unlike the CREATE TABLE AS branch, the pragma is
 * not applied here - the table already exists, and it is visible for
 * planning of the following statements.
 */
static void
process_create_table(PLpgSQL_checkstate *cstate, PLpgSQL_expr *expr,
					 CreateStmt *cstmt)
{
	Oid			relid;

	if (!is_temp_range_var(cstmt->relation))
		return;

	check_temp_schema(cstmt->relation);

	relid = RangeVarGetRelid(cstmt->relation, NoLock, true);
	if (OidIsValid(relid) && !cstmt->if_not_exists)
		drop_temp_table(cstmt->relation->relname);

	if (SPI_execute(expr->query, false, 0) != SPI_OK_UTILITY)
		elog(ERROR, "unexpected SPI result on CREATE TABLE execution");

	relid = RangeVarGetRelid(cstmt->relation, NoLock, true);
	if (!OidIsValid(relid))
		return;

	emit_pragma_from_relid(cstate, relid);
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
				process_create_table(cstate, expr, (CreateStmt *) node);
			}
			else if (IsA(node, SelectStmt))
			{
				(void) plch_apply_inline_pragmas(cstate,
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
make_pragma_stmt_walker(PLpgSQL_stmt *stmt, void *context)
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

	plch_statement_tree_walker(stmt, make_pragma_stmt_walker, NULL, context);
}

/*
 * Scans the function's body and generates (and returns as result rows)
 * table pragmas for all statements there that create temporary tables.
 */
void
plch_make_pragma(PLpgSQL_checkstate *cstate, PLpgSQL_function *func)
{
	make_pragma_stmt_walker((PLpgSQL_stmt *) func->action, cstate);
}
