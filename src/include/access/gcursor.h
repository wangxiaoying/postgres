#ifndef GCURSOR_H
#define GCURSOR_H

#include "nodes/plannodes.h"

#define GLOBAL_CURSOR_NAME_PREFIX "multi_cursor"

extern Size GlobalCursorShmemSize(void);
extern void GlobalCursorShmemInit(void);
extern void IncrementGlobalCursor(void);
extern void ResetGlobalCursor(void);
extern void GlobalCursorSetPlan(PlannedStmt *pstmt);
extern PlannedStmt *GlobalCursorGetPlan(void);
extern void GlobalCursorSetOption(int);
extern int GlobalCursorGetOption(void);
extern void GlobalCursorSetNumPartition(int);
extern int GlobalCursorGetNumPartition(void);
extern void GlobalCursorSetPartitionID(int);
extern int GlobalCursorGetPartitionID(void);
extern void GlobalCursorSetRelationOid(Oid);
extern Oid GlobalCursorGetRelationOid(void);

#endif