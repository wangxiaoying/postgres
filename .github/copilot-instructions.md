# Copilot Instructions for PostgreSQL

This is the PostgreSQL 17.x source tree — an advanced object-relational database management system written primarily in C and Perl.

## Build Commands

PostgreSQL supports both GNU Make and Meson build systems.

### GNU Make (traditional)

```bash
# Configure (one-time)
./configure --prefix=$(pwd)/build_debug --enable-debug --enable-cassert --enable-tap-tests

# Build
make -j$(nproc)              # server and client binaries
make world-bin -j$(nproc)    # also builds contrib modules

# Install
make install
make install-world           # includes contrib and docs

# Clean
make clean                   # keep configure state
make distclean               # remove all generated files
```

### Meson

```bash
meson setup builddir -Dcassert=true -Dtap_tests=enabled
meson compile -C builddir
meson install -C builddir
meson test -C builddir
```

**Important:** Meson and in-tree Make builds cannot coexist — Meson refuses to build if `src/include/pg_config.h` exists from a Make configure.

## Test Commands

### Regression tests (SQL-based)

```bash
make check                          # run core regression tests against temp install
make installcheck                   # run against running server
make check-world                    # all tests (regression + isolation + TAP + contrib)

# Single regression test (e.g., the "select" test)
make -C src/test/regress check EXTRA_TESTS=select
```

Test SQL files live in `src/test/regress/sql/`, expected output in `src/test/regress/expected/`.

### TAP tests (Perl-based)

```bash
# Run all TAP tests for a subsystem
make -C src/test/recovery check

# Single TAP test
make -C src/test/recovery check PROVE_TESTS="t/001_stream_rep.pl"

# Contrib module tests
make -C contrib/hstore check
```

Useful environment variables:
- `PROVE_FLAGS='--verbose'` — verbose TAP output
- `PG_TEST_NOCLEAN=1` — preserve temp directories after test

### Isolation tests (concurrency)

```bash
make -C src/test/isolation check
```

## Architecture

### Query processing pipeline

```
SQL text → Parser → Parse tree → Analyzer → Query tree → Rewriter → Rewritten query
→ Optimizer/Planner → Plan tree → Executor → Results
```

Each stage has a corresponding `src/backend/` subdirectory: `parser/`, `rewrite/`, `optimizer/`, `executor/`.

### Key backend subsystems (`src/backend/`)

| Directory | Purpose |
|-----------|---------|
| `parser/` | SQL lexer (`scan.l`), grammar (`gram.y`), parse analysis |
| `optimizer/` | Query planning, path generation, cost estimation, GEQO |
| `executor/` | Plan node execution — one `node*.c` per executor node type |
| `access/` | Table access methods (heap) and index AMs (btree, hash, gist, gin, brin, spgist) |
| `storage/` | Buffer manager, lock manager, virtual fd, WAL, smgr |
| `catalog/` | System catalog management and code generation |
| `commands/` | DDL/utility command implementations |
| `tcop/` | Top-level query dispatch (`postgres.c` is the main backend loop) |
| `nodes/` | Node infrastructure — copy, equal, serialize/deserialize |
| `utils/adt/` | Built-in data type implementations |
| `utils/cache/` | System catalog caches (catcache, relcache, syscache) |
| `replication/` | Physical and logical replication |

### Other important source directories

- `src/include/` — All header files, mirroring `src/backend/` structure
- `src/bin/` — Client tools (psql, pg_dump, initdb, pg_ctl, pgbench, etc.)
- `src/interfaces/libpq/` — Client C library
- `src/pl/` — Procedural languages (PL/pgSQL, PL/Perl, PL/Python, PL/Tcl)
- `contrib/` — ~60 bundled extensions
- `src/test/` — Test infrastructure and suites
- `src/common/` — Code shared between frontend and backend

## Code Conventions

### Formatting

- **Indentation:** Tabs, 4-space tab width (enforced via `.editorconfig`)
- **Line length:** 79 characters for C, 78 for Perl
- **Formatter:** Run `src/tools/pgindent/pgindent .` before committing C changes
- **Perl formatter:** `perltidy --profile=src/tools/pgindent/perltidyrc`
- **perltidy version:** Must be exactly 20230309 for consistent output
- Typedef names used by pgindent are maintained in `src/tools/pgindent/typedefs.list` — add new typedefs there

### File header comment

Every source file starts with this block:

```c
/*-------------------------------------------------------------------------
 *
 * filename.c
 *    Brief description
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/path/to/filename.c
 *
 *-------------------------------------------------------------------------
 */
```

### Include ordering

- Backend `.c` files: `#include "postgres.h"` must be the **first** include
- Frontend `.c` files: `#include "postgres_fe.h"` instead
- Then project headers as `#include "path/header.h"` (relative to `src/include/`)

### Memory management

PostgreSQL uses a **memory context** system (`palloc`/`pfree`), not raw `malloc`/`free`:

```c
void *palloc(Size size);       // allocate in CurrentMemoryContext
void *palloc0(Size size);      // allocate zeroed
void pfree(void *pointer);
char *pstrdup(const char *in);
```

Type-safe macros: `palloc_object(type)`, `palloc_array(type, count)`.

Memory contexts form a tree — destroying a parent frees all children. Switch contexts with `MemoryContextSwitchTo()`.

### Error reporting

Use `ereport()` (structured) over `elog()` (printf-style) for user-facing errors:

```c
ereport(ERROR,
    errcode(ERRCODE_UNDEFINED_TABLE),
    errmsg("relation \"%s\" does not exist", relname));
```

Error levels: `DEBUG5`–`DEBUG1`, `LOG`, `INFO`, `NOTICE`, `WARNING`, `ERROR` (aborts transaction), `FATAL` (kills backend), `PANIC` (kills all backends).

### SQL-callable C functions

Functions exposed to SQL use the V1 calling convention:

```c
#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;                        // required once per shared library

PG_FUNCTION_INFO_V1(my_function);       // register function
Datum
my_function(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);
    PG_RETURN_INT32(arg + 1);
}
```

### Node system

All plan/parse tree structures inherit from `Node` via a `NodeTag` first field. Nodes are created with `makeNode(TypeName)`. Key generated files (do NOT edit by hand):

- `src/include/nodes/nodetags.h` — generated by `src/backend/nodes/gen_node_support.pl`
- `src/backend/nodes/{copyfuncs,equalfuncs,outfuncs,readfuncs}.c` — also generated

To add a new node type, define the struct in the appropriate header with `pg_node_attr()` annotations and re-run the generator.

### System catalogs and code generation

Catalog definitions live in `src/include/catalog/pg_*.h` with initial data in companion `.dat` files (e.g., `pg_proc.dat`). Several Perl scripts generate C code from these:

| Script | Generates |
|--------|-----------|
| `src/backend/catalog/genbki.pl` | `postgres.bki`, catalog `_d.h` headers with OID macros |
| `src/backend/utils/Gen_fmgrtab.pl` | `fmgroids.h`, `fmgrprotos.h`, `fmgrtab.c` |
| `src/backend/nodes/gen_node_support.pl` | `nodetags.h`, copy/equal/out/read funcs |

When adding built-in functions, add entries to `pg_proc.dat` rather than writing registration code by hand.

### Extension/contrib module structure

```
contrib/myext/
├── Makefile             # Uses PGXS build infrastructure
├── meson.build          # Meson build definition
├── myext.control        # Extension metadata (name, version, relocatable)
├── myext--1.0.sql       # SQL install script
├── myext--1.0--1.1.sql  # Migration script
├── myext.c              # C source
└── expected/            # Regression test expected output
```

### Hook system

PostgreSQL provides function-pointer hooks for extensions to intercept core behavior without modifying source (e.g., `ExecutorStart_hook`, `planner_hook`, `ProcessUtility_hook`). Extensions set these hooks in `_PG_init()`.
