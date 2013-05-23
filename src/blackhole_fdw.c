

#include "postgres.h"

#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

PG_MODULE_MAGIC;

/*
 * SQL functions
 */
extern Datum blackhole_fdw_handler(PG_FUNCTION_ARGS);
extern Datum blackhole_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(blackhole_fdw_handler);
PG_FUNCTION_INFO_V1(blackhole_fdw_validator);


static void blackholeGetForeignRelSize (PlannerInfo *root,
								   RelOptInfo *baserel,
								   Oid foreigntableid);

static void blackholeGetForeignPaths (PlannerInfo *root,
								 RelOptInfo *baserel,
								 Oid foreigntableid);

static ForeignScan *blackholeGetForeignPlan (PlannerInfo *root,
										RelOptInfo *baserel,
										Oid foreigntableid,
										ForeignPath *best_path,
										List *tlist,
										List *scan_clauses);

static void blackholeBeginForeignScan (ForeignScanState *node,
								  int eflags);

static TupleTableSlot *blackholeIterateForeignScan (ForeignScanState *node);

static void blackholeReScanForeignScan (ForeignScanState *node);

static void blackholeEndForeignScan (ForeignScanState *node);

static void blackholeAddForeignUpdateTargets (Query *parsetree,
										 RangeTblEntry *target_rte,
										 Relation target_relation);

static List *blackholePlanForeignModify (PlannerInfo *root,
									ModifyTable *plan,
									Index resultRelation,
									int subplan_index);

static void blackholeBeginForeignModify (ModifyTableState *mtstate,
									ResultRelInfo *rinfo,
									List *fdw_private,
									int subplan_index,
									int eflags);

static TupleTableSlot *blackholeExecForeignInsert (EState *estate,
											  ResultRelInfo *rinfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);

static TupleTableSlot *blackholeExecForeignUpdate (EState *estate,
											  ResultRelInfo *rinfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);

static TupleTableSlot *blackholeExecForeignDelete (EState *estate,
											  ResultRelInfo *rinfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);

static void blackholeEndForeignModify (EState *estate,
								  ResultRelInfo *rinfo);

static void blackholeExplainForeignScan (ForeignScanState *node,
									struct ExplainState *es);

static void blackholeExplainForeignModify (ModifyTableState *mtstate,
									  ResultRelInfo *rinfo,
									  List *fdw_private,
									  int subplan_index,
									  struct ExplainState *es);

static bool blackholeAnalyzeForeignTable (Relation relation,
									 AcquireSampleRowsFunc *func,
									 BlockNumber *totalpages);

/* structures used by the FDW */

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct blackholeFdwOption
{
    const char *optname;
    Oid         optcontext;     /* Oid of catalog in which option may appear */
};

/* 
 * This is what will be set and stashed away in fdw_private during
 * GetForeignRelSize, and fetch for the subsequent routines.
 */
typedef struct
{
    char       *foo; 
    int         bar;
}   BlackholeFdwPlanState;


Datum
blackhole_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	/* assign the handlers for the FDW */

	/* these are required */
	fdwroutine->GetForeignRelSize = blackholeGetForeignRelSize;
	fdwroutine->GetForeignPaths = blackholeGetForeignPaths;
	fdwroutine->GetForeignPlan = blackholeGetForeignPlan;
	fdwroutine->BeginForeignScan = blackholeBeginForeignScan;
	fdwroutine->IterateForeignScan = blackholeIterateForeignScan;
	fdwroutine->ReScanForeignScan = blackholeReScanForeignScan;
	fdwroutine->EndForeignScan = blackholeEndForeignScan;
	
	/* remainder are optional - use NULL if not required */
	/* support for insert / update / delete */
	fdwroutine->AddForeignUpdateTargets = blackholeAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = blackholePlanForeignModify;
	fdwroutine->BeginForeignModify = blackholeBeginForeignModify;
	fdwroutine->ExecForeignInsert = blackholeExecForeignInsert;
	fdwroutine->ExecForeignUpdate = blackholeExecForeignUpdate;
	fdwroutine->ExecForeignDelete = blackholeExecForeignDelete;
	fdwroutine->EndForeignModify = blackholeEndForeignModify;

	/* support for EXPLAIN */
	fdwroutine->ExplainForeignScan = blackholeExplainForeignScan;
	fdwroutine->ExplainForeignModify = blackholeExplainForeignModify;

	/* support for ANALYSE */
	fdwroutine->AnalyzeForeignTable = blackholeAnalyzeForeignTable;

	PG_RETURN_POINTER(fdwroutine);
}

Datum
blackhole_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));

	/* make sure the options are valid */

	/* no options are supported */

	if (list_length(options_list) > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("invalid options"),
				 errhint("Blackhole FDW doies not support any options")));
	
	PG_RETURN_VOID();
}

static void blackholeGetForeignRelSize (PlannerInfo *root,
								   RelOptInfo *baserel,
								   Oid foreigntableid)
{

  /*
	Obtain relation size estimates for a foreign table. This is called at the 
	beginning of planning for a query that scans a foreign table. root is the 
	planner's global information about the query; baserel is the planner's 
	information about this table; and foreigntableid is the pg_class OID of 
	the foreign table. (foreigntableid could be obtained from the planner 
	data structures, but it's passed explicitly to save effort.)

	This function should update baserel->rows to be the expected number of 
	rows returned by the table scan, after accounting for the filtering done 
	by the restriction quals. The initial value of baserel->rows is just a 
	constant default estimate, which should be replaced if at all possible. 
	The function may also choose to update baserel->width if it can compute 
	a better estimate of the average result row width.
  */

  BlackholeFdwPlanState *fdw_private;

  baserel->rows = 0;

  fdw_private = palloc0(sizeof(BlackholeFdwPlanState));
  baserel->fdw_private = (void *) fdw_private;

  /* initialize reuired state in fdw_private */

}

static void blackholeGetForeignPaths (PlannerInfo *root,
								 RelOptInfo *baserel,
								 Oid foreigntableid)
{
  /*
	Create possible access paths for a scan on a foreign table. This is called 
	during query planning. The parameters are the same as for 
	GetForeignRelSize, which has already been called.

	This function must generate at least one access path (ForeignPath node) for 
	a scan on the foreign table and must call add_path to add each such path to 
	baserel->pathlist. It's recommended to use create_foreignscan_path to build 
	the ForeignPath nodes. The function can generate multiple access paths, 
	e.g., a path which has valid pathkeys to represent a pre-sorted result. 
	Each access path must contain cost estimates, and can contain any 
	FDW-private information that is needed to identify the specific scan 
	method intended.
  */

  /*
  BlackholeFdwPlanState *fdw_private = baserel->fdw_private;
  */

  Cost        startup_cost, total_cost;

  startup_cost = 0;
  total_cost = startup_cost + baserel->rows;

  /* Create a ForeignPath node and add it as only possible path */
  add_path(baserel, (Path *)
		   create_foreignscan_path(root, baserel,
								   baserel->rows,
								   startup_cost,
								   total_cost,
								   NIL,       /* no pathkeys */
								   NULL,      /* no outer rel either */
								   NIL));     /* no fdw_private data */
}



static ForeignScan *blackholeGetForeignPlan (PlannerInfo *root,
										RelOptInfo *baserel,
										Oid foreigntableid,
										ForeignPath *best_path,
										List *tlist,
										List *scan_clauses)
{
  /*
	Create a ForeignScan plan node from the selected foreign access path. This 
	is called at the end of query planning. The parameters are as for 
	GetForeignRelSize, plus the selected ForeignPath (previously produced by 
	GetForeignPaths), the target list to be emitted by the plan node, and 
	the restriction clauses to be enforced by the plan node.

	This function must create and return a ForeignScan plan node; it's 
	recommended to use make_foreignscan to build the ForeignScan node.

  */

	Index	scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check. So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	
	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							NIL);	/* no private state either */
	
}


static void blackholeBeginForeignScan (ForeignScanState *node,
								  int eflags)
{
  /*
	Begin executing a foreign scan. This is called during executor
	startup. It should perform any initialization needed before the scan
	can start, but not start executing the actual scan (that should be
	done upon the first call to IterateForeignScan). The ForeignScanState
	node has already been created, but its fdw_state field is still
	NULL. Information about the table to scan is accessible through the
	ForeignScanState node (in particular, from the underlying ForeignScan
	plan node, which contains any FDW-private information provided by
	GetForeignPlan). eflags contains flag bits describing the executor's
	operating mode for this plan node.

	Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this
	function should not perform any externally-visible actions; it should
	only do the minimum required to make the node state valid for
	ExplainForeignScan and EndForeignScan.

  */
}


static TupleTableSlot *blackholeIterateForeignScan (ForeignScanState *node)
{
  /*
	Fetch one row from the foreign source, returning it in a tuple table
	slot (the node's ScanTupleSlot should be used for this
	purpose). Return NULL if no more rows are available. The tuple table
	slot infrastructure allows either a physical or virtual tuple to be
	returned; in most cases the latter choice is preferable from a
	performance standpoint. Note that this is called in a short-lived
	memory context that will be reset between invocations. Create a memory
	context in BeginForeignScan if you need longer-lived storage, or use
	the es_query_cxt of the node's EState.

	The rows returned must match the column signature of the foreign table
	being scanned. If you choose to optimize away fetching columns that
	are not needed, you should insert nulls in those column positions.

	Note that PostgreSQL's executor doesn't care whether the rows returned
	violate any NOT NULL constraints that were defined on the foreign
	table columns — but the planner does care, and may optimize queries
	incorrectly if NULL values are present in a column declared not to
	contain them. If a NULL value is encountered when the user has
	declared that none should be present, it may be appropriate to raise
	an error (just as you would need to do in the case of a data type
	mismatch).
  */

  
  /*
  BlackholeFdwExecutionState *festate = 
	(BlackholeFdwExecutionState *) node->fdw_state;
  */
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

  ExecClearTuple(slot);

  /* get the next record, if any, and fill in the slot */

  /* then return the slot */
  return slot;
}


static void blackholeReScanForeignScan (ForeignScanState *node)
{
  /*
	Restart the scan from the beginning. Note that any parameters the scan
	depends on may have changed value, so the new scan does not
	necessarily return exactly the same rows.
  */
}


static void blackholeEndForeignScan (ForeignScanState *node)
{
  /*
	End the scan and release resources. It is normally not important to
	release palloc'd memory, but for example open files and connections to
	remote servers should be cleaned up.
  */
}


static void blackholeAddForeignUpdateTargets (Query *parsetree,
										 RangeTblEntry *target_rte,
										 Relation target_relation)
{
  /* 
	 UPDATE and DELETE operations are performed against rows previously
	 fetched by the table-scanning functions. The FDW may need extra
	 information, such as a row ID or the values of primary-key columns, to
	 ensure that it can identify the exact row to update or delete. To
	 support that, this function can add extra hidden, or "junk", target
	 columns to the list of columns that are to be retrieved from the
	 foreign table during an UPDATE or DELETE.

	 To do that, add TargetEntry items to parsetree->targetList, containing
	 expressions for the extra values to be fetched. Each such entry must
	 be marked resjunk = true, and must have a distinct resname that will
	 identify it at execution time. Avoid using names matching ctidN or
	 wholerowN, as the core system can generate junk columns of these
	 names.

	 This function is called in the rewriter, not the planner, so the
	 information available is a bit different from that available to the
	 planning routines. parsetree is the parse tree for the UPDATE or
	 DELETE command, while target_rte and target_relation describe the
	 target foreign table.

	 If the AddForeignUpdateTargets pointer is set to NULL, no extra target
	 expressions are added. (This will make it impossible to implement
	 DELETE operations, though UPDATE may still be feasible if the FDW
	 relies on an unchanging primary key to identify rows.)
  */
}


static List *blackholePlanForeignModify (PlannerInfo *root,
									ModifyTable *plan,
									Index resultRelation,
									int subplan_index)
{
  /*
	Perform any additional planning actions needed for an insert, update,
	or delete on a foreign table. This function generates the FDW-private
	information that will be attached to the ModifyTable plan node that
	performs the update action. This private information must have the
	form of a List, and will be delivered to BeginForeignModify during the
	execution stage.

	root is the planner's global information about the query. plan is the
	ModifyTable plan node, which is complete except for the fdwPrivLists
	field. resultRelation identifies the target foreign table by its
	rangetable index. subplan_index identifies which target of the
	ModifyTable plan node this is, counting from zero; use this if you
	want to index into plan->plans or other substructure of the plan node.

	If the PlanForeignModify pointer is set to NULL, no additional
	plan-time actions are taken, and the fdw_private list delivered to
	BeginForeignModify will be NIL.
  */

  return NULL;
}


static void blackholeBeginForeignModify (ModifyTableState *mtstate,
									ResultRelInfo *rinfo,
									List *fdw_private,
									int subplan_index,
									int eflags)
{
  /*
	Begin executing a foreign table modification operation. This routine
	is called during executor startup. It should perform any
	initialization needed prior to the actual table
	modifications. Subsequently, ExecForeignInsert, ExecForeignUpdate or
	ExecForeignDelete will be called for each tuple to be inserted,
	updated, or deleted.

	mtstate is the overall state of the ModifyTable plan node being
	executed; global data about the plan and execution state is available
	via this structure. rinfo is the ResultRelInfo struct describing the
	target foreign table. (The ri_FdwState field of ResultRelInfo is
	available for the FDW to store any private state it needs for this
	operation.) fdw_private contains the private data generated by
	PlanForeignModify, if any. subplan_index identifies which target of
	the ModifyTable plan node this is. eflags contains flag bits
	describing the executor's operating mode for this plan node.

	Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this
	function should not perform any externally-visible actions; it should
	only do the minimum required to make the node state valid for
	ExplainForeignModify and EndForeignModify.

	If the BeginForeignModify pointer is set to NULL, no action is taken
	during executor startup.
  */
}


static TupleTableSlot *blackholeExecForeignInsert (EState *estate,
											  ResultRelInfo *rinfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot)
{
  /*
	Insert one tuple into the foreign table. estate is global execution
	state for the query. rinfo is the ResultRelInfo struct describing the
	target foreign table. slot contains the tuple to be inserted; it will
	match the rowtype definition of the foreign table. planSlot contains
	the tuple that was generated by the ModifyTable plan node's subplan;
	it differs from slot in possibly containing additional "junk"
	columns. (The planSlot is typically of little interest for INSERT
	cases, but is provided for completeness.)

	The return value is either a slot containing the data that was
	actually inserted (this might differ from the data supplied, for
	example as a result of trigger actions), or NULL if no row was
	actually inserted (again, typically as a result of triggers). The
	passed-in slot can be re-used for this purpose.

	The data in the returned slot is used only if the INSERT query has a
	RETURNING clause. Hence, the FDW could choose to optimize away
	returning some or all columns depending on the contents of the
	RETURNING clause. However, some slot must be returned to indicate
	success, or the query's reported rowcount will be wrong.

	If the ExecForeignInsert pointer is set to NULL, attempts to insert
	into the foreign table will fail with an error message.

  */

  return slot;
}


static TupleTableSlot *blackholeExecForeignUpdate (EState *estate,
											  ResultRelInfo *rinfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot)
{
  /*
	Update one tuple in the foreign table. estate is global execution
	state for the query. rinfo is the ResultRelInfo struct describing the
	target foreign table. slot contains the new data for the tuple; it
	will match the rowtype definition of the foreign table. planSlot
	contains the tuple that was generated by the ModifyTable plan node's
	subplan; it differs from slot in possibly containing additional "junk"
	columns. In particular, any junk columns that were requested by
	AddForeignUpdateTargets will be available from this slot.

	The return value is either a slot containing the row as it was
	actually updated (this might differ from the data supplied, for
	example as a result of trigger actions), or NULL if no row was
	actually updated (again, typically as a result of triggers). The
	passed-in slot can be re-used for this purpose.

	The data in the returned slot is used only if the UPDATE query has a
	RETURNING clause. Hence, the FDW could choose to optimize away
	returning some or all columns depending on the contents of the
	RETURNING clause. However, some slot must be returned to indicate
	success, or the query's reported rowcount will be wrong.

	If the ExecForeignUpdate pointer is set to NULL, attempts to update
	the foreign table will fail with an error message.

  */

  return slot;
}


static TupleTableSlot *blackholeExecForeignDelete (EState *estate,
											  ResultRelInfo *rinfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot)
{
  /*
	Delete one tuple from the foreign table. estate is global execution
	state for the query. rinfo is the ResultRelInfo struct describing the
	target foreign table. slot contains nothing useful upon call, but can
	be used to hold the returned tuple. planSlot contains the tuple that
	was generated by the ModifyTable plan node's subplan; in particular,
	it will carry any junk columns that were requested by
	AddForeignUpdateTargets. The junk column(s) must be used to identify
	the tuple to be deleted.

	The return value is either a slot containing the row that was deleted,
	or NULL if no row was deleted (typically as a result of triggers). The
	passed-in slot can be used to hold the tuple to be returned.

	The data in the returned slot is used only if the DELETE query has a
	RETURNING clause. Hence, the FDW could choose to optimize away
	returning some or all columns depending on the contents of the
	RETURNING clause. However, some slot must be returned to indicate
	success, or the query's reported rowcount will be wrong.

	If the ExecForeignDelete pointer is set to NULL, attempts to delete
	from the foreign table will fail with an error message.
  */

  return slot;
}


static void blackholeEndForeignModify (EState *estate,
								  ResultRelInfo *rinfo)
{
  /*
	End the table update and release resources. It is normally not
	important to release palloc'd memory, but for example open files and
	connections to remote servers should be cleaned up.

	If the EndForeignModify pointer is set to NULL, no action is taken
	during executor shutdown.
  */
}


static void blackholeExplainForeignScan (ForeignScanState *node,
									struct ExplainState *es)
{
  /*
	Print additional EXPLAIN output for a foreign table scan. This
	function can call ExplainPropertyText and related functions to add
	fields to the EXPLAIN output. The flag fields in es can be used to
	determine what to print, and the state of the ForeignScanState node
	can be inspected to provide run-time statistics in the EXPLAIN ANALYZE
	case.

	If the ExplainForeignScan pointer is set to NULL, no additional
	information is printed during EXPLAIN.
  */
}


static void blackholeExplainForeignModify (ModifyTableState *mtstate,
									  ResultRelInfo *rinfo,
									  List *fdw_private,
									  int subplan_index,
									  struct ExplainState *es)
{
  /*
	Print additional EXPLAIN output for a foreign table update. This
	function can call ExplainPropertyText and related functions to add
	fields to the EXPLAIN output. The flag fields in es can be used to
	determine what to print, and the state of the ModifyTableState node
	can be inspected to provide run-time statistics in the EXPLAIN ANALYZE
	case. The first four arguments are the same as for BeginForeignModify.

	If the ExplainForeignModify pointer is set to NULL, no additional
	information is printed during EXPLAIN.
  */
}


static bool blackholeAnalyzeForeignTable (Relation relation,
									 AcquireSampleRowsFunc *func,
									 BlockNumber *totalpages)
{
  /*
	This function is called when ANALYZE is executed on a foreign
	table. If the FDW can collect statistics for this foreign table, it
	should return true, and provide a pointer to a function that will
	collect sample rows from the table in func, plus the estimated size of
	the table in pages in totalpages. Otherwise, return false.

	If the FDW does not support collecting statistics for any tables, the
	AnalyzeForeignTable pointer can be set to NULL.

	If provided, the sample collection function must have the signature

		int
		AcquireSampleRowsFunc (Relation relation, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows,
							   double *totaldeadrows);

	A random sample of up to targrows rows should be collected from the
	table and stored into the caller-provided rows array. The actual
	number of rows collected must be returned. In addition, store
	estimates of the total numbers of live and dead rows in the table into
	the output parameters totalrows and totaldeadrows. (Set totaldeadrows
	to zero if the FDW does not have any concept of dead rows.)
  */

  return false;
}

