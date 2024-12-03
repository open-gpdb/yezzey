

#ifndef YEZZEY_XVACUUM_H
#define YEZZEY_XVACUUM_H

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC int yezzey_delete_chunk_internal(const char *external_chunk_path);
EXTERNC int yezzey_vacuum_garbage_internal(int segindx, bool confirm,
                                           bool crazyDrop);

EXTERNC int yezzey_vacuum_garbage_relation_internal(Relation rel, int segindx, bool confirm,
                                           bool crazyDrop);

EXTERNC int yezzey_vacuum_garbage_relation_internal_oid(Oid reloid, int segindx, bool confirm,
                                           bool crazyDrop);

EXTERNC int yezzey_delete_obsolete_chunks_internal(Relation rel, int segindx);
EXTERNC int yezzey_collect_obsolete_chunks_internal(Relation rel, int segindx);

#endif /* YEZZEY_XVACUUM_H */