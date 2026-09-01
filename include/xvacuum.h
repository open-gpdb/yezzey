

#ifndef YEZZEY_XVACUUM_H
#define YEZZEY_XVACUUM_H

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC void yezzey_delete_chunk_internal(const char *external_chunk_path);
EXTERNC void yezzey_vacuum_garbage_internal(int segindx, bool confirm,
                                            bool crazyDrop);

EXTERNC void yezzey_vacuum_garbage_tablespace_internal(Oid tablespace,
                                                       int segindx,
                                                       bool confirm,
                                                       bool crazyDrop);

EXTERNC void yezzey_vacuum_garbage_relation_internal(Relation rel, int segindx,
                                                     bool confirm,
                                                     bool crazyDrop);

EXTERNC void yezzey_vacuum_garbage_relation_internal_oid(Oid reloid,
                                                         int segindx,
                                                         bool confirm,
                                                         bool crazyDrop);

EXTERNC void yezzey_collect_obsolete_internal(int segindx, const char *dbname,
                                              Oid nspoid, Oid dboid);
EXTERNC void yezzey_delele_obsolete_internal(int segindx, bool crazy_drop,
                                             const char *dbname, Oid nspoid,
                                             Oid dboid);
#endif /* YEZZEY_XVACUUM_H */
