#ifndef GCURSOR_H
#define GCURSOR_H

#include "nodes/plannodes.h"
#include "storage/spin.h"
#include "utils/portal.h"

#define GLOBAL_CURSOR_NAME_PREFIX       "multi_cursor"
#define	GLOBAL_CURSOR_MAGIC		        0x79fb2666
#define GLOBAL_CURSOR_QUEUE_SIZE		65536

typedef struct
{
    slock_t mutex;
} gcursor_shm_header;

extern Size GlobalCursorShmemSize(void);
extern void GlobalCursorShmemInit(void);
extern void ResetGlobalCursor(void);
extern void GlobalCursorSetPlan(PlannedStmt *);
extern PlannedStmt *GlobalCursorGetPlan(void);
extern void GlobalCursorSetOption(int);
extern int GlobalCursorGetOption(void);
extern void GlobalCursorSetNumPartition(int);
extern int GlobalCursorGetNumPartition(void);
extern void GlobalCursorSetPartitionID(int);
extern int GlobalCursorGetPartitionID(void);
extern Datum GlobalCursorGetMainArg(void);
extern void GlobalCursorSetMainArg(Datum);
extern void GlobalCursorLock(void);
extern void GlobalCursorUnlock(void);
extern void GlobalCursorIncrementAttached(void);
extern int GlobalCursorGetAttached(void);
extern Portal InitGlobalCursorPortal(char *);
extern bool IsGlobalCursor(char *);

#endif