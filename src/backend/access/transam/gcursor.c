#include "postgres.h"

#include "access/gcursor.h"
#include "access/transam.h"
#include "commands/explain.h"
#include "nodes/nodes.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/snapshot.h"

#define MAX_SERIALIZED_PLAN_LEN 8000 // hardcode max plan size

int CURRENT_PARTITION_ID = -1;

typedef struct GlobalCursors
{
    Oid rel_oid;
    int num_part;
    int option;
    char plan[MAX_SERIALIZED_PLAN_LEN];
} GlobalCursors;

static GlobalCursors *GCurs = NULL;
int part_id = -1;

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
}

void GlobalCursorSetPlan(PlannedStmt *pstmt)
{
    char *pstmt_data;
    int pstmt_len;

    GCurs->rel_oid = linitial_oid(pstmt->relationOids);
    elog(DEBUG1, "set initial oid: %d", GCurs->rel_oid);

    pstmt_data = nodeToString(pstmt);
    pstmt_len = strlen(pstmt_data) + 1;
    if (pstmt_len > MAX_SERIALIZED_PLAN_LEN)
    {
        elog(ERROR, "statement is too long! %d > %d", pstmt_len, MAX_SERIALIZED_PLAN_LEN);
    }
    // elog(DEBUG1, "set serialized plan: %s (%d)", pstmt_data, pstmt_len);
    memcpy(&GCurs->plan, pstmt_data, pstmt_len);
}

PlannedStmt *GlobalCursorGetPlan(void)
{
    PlannedStmt *pstmt;
    pstmt = (PlannedStmt *)stringToNode(&GCurs->plan);

    // ExplainState *es = NewExplainState();
    // es->analyze = false;
    // es->costs = false;
    // es->verbose = true;
    // es->buffers = false;
    // es->timing = false;
    // es->summary = false;
    // es->format = EXPLAIN_FORMAT_TEXT;
    // ExplainBeginOutput(es);
    // ExplainOnePlan(pstmt, NULL, es, "", NULL, NULL, NULL, NULL);
    // ExplainEndOutput(es);
    // Assert(es->indent == 0);
    // elog(DEBUG1, "plan: %s", es->str->data);

    return pstmt;
}

inline Oid GlobalCursorGetRelationOid(void)
{
    return GCurs->rel_oid;
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
