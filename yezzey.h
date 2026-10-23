/*-------------------------------------------------------------------------
 *
 * yezzey.h
 *        GP/PG Extention to offload cold data segments to external storage
 *
 * PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *                yezzey.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef YEZZEY_H
#define YEZZEY_H

#include "postgres.h"

#include "gucs.h"
#include "ystat.h"

void yezzey_prepare(void);
void yezzey_finish(void);

void yezzey_offload_relation_internal(Oid reloid, bool remove_locally,
                                      const char *external_path);
void yezzey_load_relation_internal(Oid reloid);

const f_smgr_ao *smgrao_yezzey(void);

extern Datum yezzey_stat_get_external_storage_usage(PG_FUNCTION_ARGS);

void _PG_init(void);

#endif /* YEZZEY_H */
