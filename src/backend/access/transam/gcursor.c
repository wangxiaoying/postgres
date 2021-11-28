#include "postgres.h"

#include "access/gcursor.h"
#include "access/transam.h"
#include "commands/explain.h"
#include "nodes/nodes.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/snapshot.h"

#define MAX_SERIALIZED_PLAN_LEN 100000 // hardcode max plan size

int CURRENT_PARTITION_ID = -1;

// TODO: need lock
typedef struct GlobalCursors
{
    Datum main_arg;
    int num_part;
    int option;
    slock_t mutex;
    int attached;
    char plan[MAX_SERIALIZED_PLAN_LEN];
} GlobalCursors;

static GlobalCursors *GCurs = NULL;

Size GlobalCursorShmemSize(void)
{
    Size size;
    size = sizeof(GlobalCursors);
    elog(DEBUG1, "init global cursor shared mem with size: %ld", size);
    return size;
}

void GlobalCursorShmemInit(void)
{
    bool foundGCurs;
    GCurs = (GlobalCursors *)ShmemInitStruct("GCurs", GlobalCursorShmemSize(), &foundGCurs);
    memset(GCurs, 0, sizeof(GlobalCursors));
    elog(DEBUG1, "init global cursor, %d", foundGCurs);
}

void ResetGlobalCursor(void)
{
    memset(GCurs, 0, sizeof(GlobalCursors));
    elog(DEBUG1, "reset global cursor!");
    SpinLockInit(&GCurs->mutex);
    GCurs->main_arg = NULL;
    GCurs->attached = 0;
}

void GlobalCursorSetPlan(PlannedStmt *pstmt)
{
    char *pstmt_data;
    int pstmt_len;

    pstmt_data = nodeToString(pstmt);
    pstmt_len = strlen(pstmt_data) + 1;
    if (pstmt_len > MAX_SERIALIZED_PLAN_LEN)
    {
        elog(ERROR, "statement is too long! %d > %d", pstmt_len, MAX_SERIALIZED_PLAN_LEN);
    }
    // elog(DEBUG1, "set serialized plan: %s (%d)", pstmt_data, pstmt_len);
    ExplainState *es = NewExplainState();
    es->analyze = false;
    es->costs = false;
    es->verbose = true;
    es->buffers = false;
    es->timing = false;
    es->summary = false;
    es->format = EXPLAIN_FORMAT_TEXT;
    ExplainBeginOutput(es);
    ExplainOnePlan(pstmt, NULL, es, "", NULL, NULL, NULL, NULL);
    ExplainEndOutput(es);
    Assert(es->indent == 0);
    elog(DEBUG1, "plan: %s", es->str->data);

    memcpy(&GCurs->plan, pstmt_data, pstmt_len);
}

PlannedStmt *GlobalCursorGetPlan(void)
{
    PlannedStmt *pstmt;
    pstmt = (PlannedStmt *)stringToNode(&GCurs->plan);
    return pstmt;
}


inline void GlobalCursorSetOption(int option)
{
    GCurs->option = option;
    elog(DEBUG1, "set option: %d", option);
}

inline int GlobalCursorGetOption(void)
{
    return GCurs->option;
}

inline void GlobalCursorSetNumPartition(int num_part)
{
    GCurs->num_part = num_part;
    elog(DEBUG1, "set number of partition: %d", num_part);
}

inline int GlobalCursorGetNumPartition(void)
{
    return GCurs->num_part;
}

inline void GlobalCursorSetPartitionID(int part_id)
{
    CURRENT_PARTITION_ID = part_id;
}

inline int GlobalCursorGetPartitionID(void)
{
    return CURRENT_PARTITION_ID;
}

inline void GlobalCursorSetMainArg(Datum main_arg)
{
    GCurs->main_arg = main_arg;
}

inline Datum GlobalCursorGetMainArg(void)
{
    return GCurs->main_arg;
}

inline void GlobalCursorLock(void)
{
    SpinLockAcquire(&GCurs->mutex);
}

inline void GlobalCursorUnlock(void)
{
    SpinLockRelease(&GCurs->mutex);
}

// needs to be called while GlobalCursorLock
inline int GlobalCursorGetAttached(void)
{
    Assert(GCurs->attached < GCurs->num_part);
    return GCurs->attached;
}

inline void GlobalCursorIncrementAttached(void)
{
    Assert(GCurs->attached < GCurs->num_part);
    GCurs->attached += 1;
}

Portal InitGlobalCursorPortal(char *portalname)
{
    Portal portal;
    MemoryContext oldContext;
	PlannedStmt *plan;
	char *queryString;

	int part_id = strtol(portalname + strlen(GLOBAL_CURSOR_NAME_PREFIX), NULL, 10);
	if (part_id < 0 || part_id >= GlobalCursorGetNumPartition())
	{
		elog(ERROR, "got partition id %d not within [0, %d)", part_id, GlobalCursorGetNumPartition());
	}
	GlobalCursorSetPartitionID(part_id);
	elog(DEBUG1, "current session global cursor partition: %d", GlobalCursorGetPartitionID());

	plan = GlobalCursorGetPlan();

	/* Create a portal like in CursorOpen */
    elog(DEBUG1, "create global cursor portal: %s", portalname);
	portal = CreatePortal(portalname, false, false);

	oldContext = MemoryContextSwitchTo(portal->portalContext);

	queryString = ""; // hardcode - seems like no need queryString for execution

	PortalDefineQuery(portal,
					  NULL,
					  queryString,
					  CMDTAG_SELECT, /* cursor's query is always a SELECT */
					  list_make1(plan),
					  NULL);

	MemoryContextSwitchTo(oldContext);

	portal->cursorOptions = GlobalCursorGetOption();
	MemoryContextSwitchTo(oldContext);

	PortalStart(portal, NULL, 0, InvalidSnapshot); // hardcode only support params = NULL

	Assert(portal->strategy == PORTAL_ONE_SELECT);

    return portal;
}

bool IsGlobalCursor(char *portalname)
{
    return (strncmp(GLOBAL_CURSOR_NAME_PREFIX, portalname, strlen(GLOBAL_CURSOR_NAME_PREFIX)) == 0);
}