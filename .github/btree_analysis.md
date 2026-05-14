# B-Tree Index Build Analysis

## Overview

PostgreSQL builds B-Tree indexes using an **external merge sort** followed by bottom-up page construction. This approach handles datasets much larger than memory by spilling sorted runs to disk and merging them.

All core build logic lives in `src/backend/access/nbtree/nbtsort.c`.

---

## Step-by-Step Build Procedure (entry: `btbuild`)

### Phase 1: Initialization — `btbuild` (line 415)

Entry point called by the index AM framework.

```
btbuild(Relation heap, Relation index, IndexInfo *indexInfo)
```

1. Initializes `BTBuildState` — the top-level build tracking struct
2. Decides **parallel vs serial** build:
   - If `indexInfo->ii_ParallelWorkers > 0` and conditions are met → parallel
   - Otherwise → serial
3. Calls `_bt_spools_heapscan()` to scan heap and fill sort spools
4. Calls `_bt_leafbuild()` to sort and construct pages
5. Returns `IndexBuildResult` with tuple counts

#### Key structs:

```c
typedef struct BTBuildState
{
    bool        isunique;          // unique index?
    bool        nulls_not_distinct;
    bool        havedead;          // any dead tuples seen?
    Relation    heap;
    BTSpool    *spool;             // main sort spool
    BTSpool    *spool2;            // dead tuples spool (unique indexes only)
    double      indtuples;         // count of indexed tuples
    // ... parallel build fields ...
} BTBuildState;

typedef struct BTSpool
{
    Tuplesortstate *sortstate;     // tuplesort state
    Relation        heap;
    Relation        index;
    bool            isunique;
    bool            nulls_not_distinct;
} BTSpool;
```

### Phase 2: Heap Scan & Spool — `_bt_spools_heapscan` (line 487)

Scans the entire heap and feeds index tuples into tuplesort.

1. Creates `BTSpool` with `tuplesort_begin_index_btree()`
   - Memory budget: `maintenance_work_mem`
2. For **unique indexes**: creates a second spool (`spool2`) for dead tuples
3. For **parallel builds**: launches workers via `_bt_begin_parallel()`
4. Calls `table_index_build_scan()` which scans the heap and invokes the callback for each tuple

### Phase 3: Per-Tuple Callback — `_bt_build_callback` (line 701)

Called once per heap tuple during the scan.

```c
static void
_bt_build_callback(Relation index, ItemPointer tid, Datum *values,
                   bool *isnull, bool tupleIsAlive, void *state)
```

1. Forms an index tuple from the heap tuple's key values
2. Routes the tuple:
   - **Dead tuple + unique index** → `spool2` (separate sort for dead tuples)
   - **All others** → `spool` (main sort)
3. Calls `tuplesort_putindextuplevalues()` to add to the appropriate spool

### Phase 4: Sort & Build — `_bt_leafbuild` (line 660)

Orchestrates the sort completion and page construction.

1. Calls `tuplesort_performsort()` on the main spool
   - If data fits in memory: in-memory quicksort
   - If spilled to disk: merges sorted runs (see [Tuplesort section](#the-role-of-dumptuples-tuplesorttc2339))
2. If `spool2` exists, sorts that too
3. Initializes `BTWriteState`:
   ```c
   typedef struct BTWriteState
   {
       Relation        index;
       BulkWriteState *bulkstate;    // bulk write handle (bypasses buffer cache)
       BTScanInsert    inskey;       // insertion scankey for comparisons
   } BTWriteState;
   ```
4. Calls `_bt_load()` to read sorted tuples and build the tree

### Phase 5: Load Sorted Tuples — `_bt_load` (line 1259)

Reads tuples from the sorted spool(s) and builds B-Tree pages bottom-up.

**Three load strategies** (decided here):

| Strategy | Condition | Description |
|----------|-----------|-------------|
| **Merge** | `spool2 != NULL` (unique index with dead tuples) | Merge-reads from both spools, interleaving live and dead tuples in sort order |
| **Dedup** | Non-unique index, `deduplicate=true` | Reads from main spool, combines duplicate keys into posting list tuples |
| **Plain** | Default | Reads tuples one-by-one from main spool |

Core loop (plain path shown):
```c
while ((itup = tuplesort_getindextuple(btspool->sortstate, true)) != NULL)
{
    if (state == NULL)
        state = _bt_pagestate(wstate, 0);   // init first leaf page

    _bt_buildadd(wstate, state, itup, 0);   // add tuple to current page
}

_bt_uppershutdown(wstate, state);    // finalize rightmost pages + metapage
smgr_bulk_finish(wstate->bulkstate); // flush bulk write buffer
```

### Phase 6: Page Initialization — `_bt_pagestate` (line 770)

Creates a new `BTPageState` for a given tree level.

```c
static BTPageState *
_bt_pagestate(BTWriteState *wstate, uint32 level)
```

1. Allocates a new buffer via `smgr_bulk_get_buf()`
2. Initializes the page with `_bt_blnewpage()` (sets opaque flags: leaf/non-leaf, etc.)
3. Sets `btps_full` — the page fullness threshold:
   - **Leaf pages**: Respects `fillfactor` (default 90%), leaves room for future inserts
   - **Internal pages**: Packed as tightly as possible (no fillfactor slack)
4. Returns the `BTPageState`

### Phase 7: Add Tuple to Page — `_bt_buildadd` (line 908)

The core page-building function. Called once per sorted tuple.

```c
static void
_bt_buildadd(BTWriteState *wstate, BTPageState *state,
             IndexTuple itup, Size truncextra)
```

**Page full check** — two conditions trigger a new page:
1. **Hard limit**: Tuple physically doesn't fit (`pgspc < itupsz`)
2. **Soft limit**: Page usage exceeds `btps_full` threshold and we have more than one item

**When page is full:**
1. **Truncate** the separator key (suffix truncation) — removes unnecessary trailing attributes to save space in parent pages
2. **Link** the old page → new page (set `btpo_next`)
3. **Write the full page to disk**: `_bt_blwritepage(wstate, obuf, oblkno)`
4. **Propagate** the separator key up to the parent level:
   ```c
   // Create parent level on-demand if needed
   if (state->btps_next == NULL)
       state->btps_next = _bt_pagestate(wstate, state->btps_level + 1);
   // Insert downlink into parent
   BTreeTupleSetDownLink(oitup, oblkno);
   _bt_buildadd(wstate, state->btps_next, oitup, 0);  // recursive!
   ```
5. **Start** a new page and insert the current tuple there

**Key optimization — suffix truncation:**
When splitting a page, the separator key for the parent is truncated to the minimal distinguishing prefix. This saves space in internal pages significantly.

### Phase 8: Finalization — `_bt_uppershutdown` (line 1187)

Called once after all tuples are loaded. Handles only the **rightmost (last) page** at each level.

```c
static void
_bt_uppershutdown(BTWriteState *wstate, BTPageState *state)
```

1. **Walks up** the `BTPageState` linked list (leaf → internal → ... → root)
2. For each level's final page:
   - If **topmost**: marks as `BTP_ROOT`
   - Otherwise: inserts its low key into parent via `_bt_buildadd()` (may trigger further parent page writes)
   - Calls `_bt_slideleft()` to fix rightmost page format
   - **Writes** the page via `_bt_blwritepage()`
3. **Writes the metapage** pointing to root block, making the index valid

---

## Complete Call Graph

```
btbuild()
 ├─ _bt_spools_heapscan()
 │   ├─ tuplesort_begin_index_btree()        # create main spool
 │   ├─ tuplesort_begin_index_btree()        # create spool2 (unique only)
 │   ├─ [_bt_begin_parallel()]               # launch workers (parallel only)
 │   └─ table_index_build_scan()
 │       └─ _bt_build_callback()             # called per heap tuple
 │           └─ tuplesort_putindextuplevalues()
 │               └─ [dumptuples()]           # if LACKMEM → sort & spill run to disk
 │
 ├─ _bt_leafbuild()
 │   ├─ tuplesort_performsort()              # complete the sort
 │   │   └─ [mergeruns()]                    # merge sorted runs if spilled
 │   └─ _bt_load()
 │       ├─ tuplesort_getindextuple()        # read next sorted tuple
 │       ├─ _bt_pagestate()                  # init page for level 0 (leaf)
 │       │   └─ _bt_blnewpage()
 │       ├─ _bt_buildadd()                   # add tuple to page (LOOP)
 │       │   ├─ _bt_truncate()              # suffix truncation on split
 │       │   ├─ _bt_blwritepage()           # write full page to disk
 │       │   │   └─ smgr_bulk_write()
 │       │   ├─ _bt_pagestate()             # create parent level (on-demand)
 │       │   └─ _bt_buildadd() [recursive]  # insert downlink into parent
 │       │
 │       ├─ _bt_uppershutdown()              # finalize last pages
 │       │   ├─ _bt_buildadd()              # insert final downlinks
 │       │   ├─ _bt_slideleft()             # fix rightmost pages
 │       │   ├─ _bt_blwritepage()           # write last page per level
 │       │   └─ _bt_blwritepage()           # write metapage
 │       └─ smgr_bulk_finish()               # flush bulk write buffer
 │
 └─ return IndexBuildResult
```

---

## Parallel Build

When `indexInfo->ii_ParallelWorkers > 0`, the build uses multiple workers:

1. **Leader** calls `_bt_begin_parallel()`:
   - Allocates shared memory (`BTShared` struct) via DSM
   - Launches worker processes
   - Each worker gets its own `BTSpool` and performs a parallel heap scan
2. **Workers** each:
   - Scan a portion of the heap (coordinated via `_bt_parallel_heapscan`)
   - Sort their portion independently
   - Write sorted runs to shared temp files
3. **Leader** calls `_bt_leafbuild()`:
   - Merges all workers' sorted outputs
   - Builds the final B-Tree pages (single-threaded)

---

## The Role of `dumptuples` (tuplesort.c:2339)

`dumptuples` is the **run-building** step of the external sort algorithm. It is called when the in-memory tuple buffer is full (memory budget exhausted).

### What it does:

1. **Quicksort** the accumulated tuples in `memtuples[]` via `tuplesort_sort_memtuples(state)`
2. **Write** each sorted tuple to a `LogicalTape` on disk via `WRITETUP(state, destTape, stup)`
3. **Free** tuple memory via `MemoryContextReset()` and `FREEMEM(state, tupleMem)`
4. **Advance** to the next tape via `selectnewtape()` for the next run

Each invocation produces one **sorted run** on a logical tape. After all input is consumed, `mergeruns()` performs a polyphase merge of these runs into a single sorted stream.

## Disk Spill Mechanism

### Are sorted tuples spilled to disk?

**Yes**, when `maintenance_work_mem` is exhausted.

### I/O stack:

```
dumptuples()
  → WRITETUP() macro
      → writetup_index()           [tuplesortvariants.c]
          → LogicalTapeWrite()     [logtape.c]
              → BufFileWrite()     [buffile.c]
                  → FileWrite()    Physical I/O to pgsql_tmp/
```

### Storage structures:

| Abstraction    | Purpose                                          |
|----------------|--------------------------------------------------|
| `LogicalTapeSet` | Manages N logical tapes over one physical temp file |
| `LogicalTape`    | One tape = one sorted run                        |
| `BufFile`        | Buffered I/O abstraction over temp files         |

Temp files are created in `$PGDATA/base/pgsql_tmp/`.

## Spill Threshold

### Memory budget

The sort uses **`maintenance_work_mem`** (not `work_mem`) as the memory budget. This is typically much larger (default 64MB vs 4MB for `work_mem`).

```c
// In tuplesort_begin_common():
state->allowedMem = maintenance_work_mem * (int64) 1024;
state->availMem = state->allowedMem;
```

### Memory tracking

Each tuple insertion calls:
```c
USEMEM(state, GetMemoryChunkSpace(tuple));  // availMem -= size
```

### Spill trigger

```c
#define LACKMEM(state)  ((state)->availMem < 0 && !(state)->slabAllocatorUsed)
```

The threshold is **negative** (not exactly 0) — it allows a small overshoot before triggering a spill.

### Three spill events:

1. **First LACKMEM** (`tuplesort_puttuple_common:1279`): Transitions state from `TSS_INITIAL` → `TSS_BUILDRUNS`, calls `inittapes()` to initialize disk I/O, then `dumptuples()`.
2. **Periodic during ingestion** (`tuplesort_puttuple_common:1315`): Each subsequent LACKMEM during `TSS_BUILDRUNS` triggers another `dumptuples()` call.
3. **Final flush** (`tuplesort_performsort:1459`): `dumptuples(state, true)` to flush any remaining in-memory tuples, then `mergeruns()`.

## State Machine

```
TSS_INITIAL          All tuples fit in memory (quicksort only)
    │
    │ LACKMEM → inittapes() + dumptuples()
    ▼
TSS_BUILDRUNS        Accumulate tuples, spill sorted runs to tapes
    │
    │ tuplesort_performsort() → mergeruns()
    ▼
TSS_SORTEDONTAPE     Single sorted stream on tape (or TSS_FINALMERGE for lazy merge)
    │
    │ tuplesort_getindextuple()
    ▼
Tuples consumed by _bt_load() to build B-Tree pages
```

## No-Spill Fast Path

If the entire dataset fits in `maintenance_work_mem`, no disk I/O occurs:
- State stays `TSS_INITIAL`
- `tuplesort_performsort()` does an in-memory quicksort
- `_bt_load()` reads directly from the sorted `memtuples[]` array

## Page Writing: `_bt_buildadd` vs `_bt_uppershutdown`

### When are leaf pages written to disk?

Leaf pages (and internal pages) are written to disk **progressively during the loading phase** by `_bt_buildadd`, not deferred until the end. `_bt_uppershutdown` only finalizes the last page on each level.

### `_bt_buildadd` (nbtsort.c:~1000)

Called once per sorted tuple by `_bt_load`. When the current page fills up:

```c
// When page is full:
_bt_blwritepage(wstate, obuf, oblkno);  // Write full page to disk immediately
// Then allocate a new page and continue loading
```

This means **the bulk of disk I/O happens here**, during tuple loading.

### `_bt_uppershutdown` (nbtsort.c:1187)

Called once at the end of `_bt_load`, after all tuples have been processed. It handles only the **final rightmost page** at each level:

1. **Walks up** the `BTPageState` linked list (one node per level: leaf → internal → root)
2. For each level's last page:
   - If topmost: marks it as `BTP_ROOT`
   - Otherwise: inserts its low key into the parent level via `_bt_buildadd` (which may itself trigger a page write at the parent level)
   - Calls `_bt_slideleft()` to fix the rightmost page format
   - Writes the page via `_bt_blwritepage()`
3. **Writes the metapage** pointing to the root block, making the index valid

### `_bt_blwritepage` (nbtsort.c:759)

Thin wrapper around `smgr_bulk_write()`:

```c
static void
_bt_blwritepage(BTWriteState *wstate, BulkWriteBuffer buf, BlockNumber blkno)
{
    smgr_bulk_write(wstate->bulkstate, blkno, buf, true);
}
```

### `BTPageState` — per-level build state (nbtsort.c:231)

```c
typedef struct BTPageState
{
    BulkWriteBuffer btps_buf;       // current page being built
    BlockNumber     btps_blkno;     // block number for this page
    IndexTuple      btps_lowkey;    // page's strict lower bound pivot
    OffsetNumber    btps_lastoff;   // last item offset loaded
    Size            btps_lastextra; // last item's extra posting list space
    uint32          btps_level;     // tree level (0 = leaf)
    Size            btps_full;      // page-full threshold
    struct BTPageState *btps_next;  // link to parent level's state
} BTPageState;
```

The `btps_next` pointer forms a linked list from leaf level up to the current root level. New levels are created on-demand when internal pages fill up.

### Write timeline summary

| Phase | What's written to disk |
|-------|----------------------|
| `_bt_buildadd` loop | Every **full** leaf & internal page (the bulk of I/O) |
| `_bt_uppershutdown` | Only the **last rightmost page** per level + metapage |

By the time `_bt_uppershutdown` runs, all leaf pages except the rightmost one are already on disk.

## Key Source Files

| File | Purpose |
|------|---------|
| `src/backend/access/nbtree/nbtsort.c` | B-Tree build orchestration: `btbuild`, `_bt_spools_heapscan`, `_bt_leafbuild`, `_bt_load` |
| `src/backend/utils/sort/tuplesort.c` | Sort framework: state machine, `dumptuples`, `mergeruns`, `performsort` |
| `src/backend/utils/sort/tuplesortvariants.c` | Type-specific sort callbacks: `writetup_index`, `comparetup_index` |
| `src/backend/utils/sort/logtape.c` | Logical tape abstraction over temp files |
| `src/include/utils/tuplesort.h` | Public API and state definitions |
