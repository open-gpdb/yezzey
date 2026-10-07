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
void yezzey_open(SMgrRelation reln);

void yezzey_close(SMgrRelation reln, ForkNumber forkNum);
void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo);

void yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum,
                      bool isRedo);

bool yezzey_exists(SMgrRelation reln, ForkNumber forkNum);

void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo);

void yezzey_unlink_ao(RelFileNodeBackend rnode, ForkNumber forkNum,
                      bool isRedo);

void yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
#if PG_VERSION_NUM >= 160000
                   const void *buffer, bool skipFsync);
#else
                   char *buffer, bool skipFsync);
#endif
#if PG_VERSION_NUM >= 160000
void yezzey_zeroextend(SMgrRelation reln, ForkNumber forkNum,
                       BlockNumber blockNum, int nBlocks, bool skipFsync);
#endif
bool yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber blockNum);
void yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
#if PG_VERSION_NUM >= 160000
                 void *buffer);
#else
                 char *buffer);
#endif
void yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
#if PG_VERSION_NUM >= 160000
                  const void *buffer, bool skipFsync);
#else
                  char *buffer, bool skipFsync);
#endif

void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum,
                      BlockNumber blockNum, BlockNumber nBlocks);

BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum);
void yezzey_truncate(SMgrRelation reln, ForkNumber forkNum,
#if PG_VERSION_NUM >= 160000
                     BlockNumber old_blocks, BlockNumber nBlocks);
#else
                     BlockNumber nBlocks);
#endif
void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum);

BlockNumber yezzey_mdnblocks(SMgrRelation reln, ForkNumber forknum);

void smgr_yezzey(SMgrRelation reln, BackendId backend, SMgrImpl which,
                 Relation rel);

void smgr_init_yezzey(void);

extern Datum yezzey_stat_get_external_storage_usage(PG_FUNCTION_ARGS);

void _PG_init(void);

#endif /* YEZZEY_H */
