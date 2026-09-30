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

int loadFileFromExternalStorage(RelFileNode rnode, BackendId backend,
                                ForkNumber forkNum, BlockNumber blkno);

void yezzey_init(void);

/*
 * SMGR - related functions
 */

void yezzey_close(SMgrRelation reln, ForkNumber forkNum);
void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo);

void yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum,
                      bool isRedo);

bool yezzey_exists(SMgrRelation reln, ForkNumber forkNum);

void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo,
                   char relstorage);

void yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                   char *buffer, bool skipFsync);
void yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber blockNum);
void yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                 char *buffer);
void yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                  char *buffer, bool skipFsync);

void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum,
                      BlockNumber blockNum, BlockNumber nBlocks);

BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum);
void yezzey_truncate(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber nBlocks);
void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum);

BlockNumber yezzey_mdnblocks(SMgrRelation reln, ForkNumber forknum);

extern void yezzey_pre_ckpt(void);
extern void yezzey_sync(void);
extern void yezzey_post_ckpt(void);

const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode);

const f_smgr_ao *smgrao_yezzey(void);
void smgr_init_yezzey(void);

extern Datum yezzey_stat_get_external_storage_usage(PG_FUNCTION_ARGS);

void _PG_init(void);

extern Oid runningRewriteSpcOidHint;

#endif /* YEZZEY_H */
