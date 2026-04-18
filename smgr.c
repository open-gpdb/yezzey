
#include "postgres.h"

// For GpIdentity
#include "c.h"
#include "cdb/cdbvars.h"

/* for IsModernYezzey */
#include "ygpver.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#include "catalog/pg_tablespace.h"

#if IsModernYezzey
#include "access/aomd.h"
#endif

#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/shmem.h"
#include "storage/smgr.h"

#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "utils/elog.h"
#include "utils/snapmgr.h"

#include "miscadmin.h"

#include "proxy.h"
#include "storage.h"
#include "yezzey.h"
#include "yezzey_meta.h"

/*
 * Construct external storage filepath.
 *
 * Assepts initialized StringInfoData as its first param
 */

static void constructExtenrnalStorageFilepath(StringInfoData *path,
                                              RelFileNode rnode,
                                              BackendId backend,
                                              ForkNumber forkNum,
                                              BlockNumber blkno) {
  char *relpath;
  BlockNumber blockNum;

  relpath = relpathbackend(rnode, backend, forkNum);

  appendStringInfoString(path, relpath);
  blockNum = blkno / ((BlockNumber)RELSEG_SIZE);

  if (blockNum > 0)
    appendStringInfo(path, ".%u", blockNum);

  pfree(relpath);
}

/* TODO: remove, or use external_storage.h funcs */
int loadFileFromExternalStorage(RelFileNode rnode, BackendId backend,
                                ForkNumber forkNum, BlockNumber blkno) {
  StringInfoData path;
  initStringInfo(&path);

  constructExtenrnalStorageFilepath(&path, rnode, backend, forkNum, blkno);

  return 0;
}

static bool yezzeyCheatRelfilenode(RelFileNodeBackend *rnode) {
#if IsGreenplum6
  return false;
#endif
  if (rnode->node.spcNode == YEZZEYTABLESPACE_OID) {
    rnode->node.spcNode = DEFAULTTABLESPACE_OID;
    return true;
  }
  return false;
}

static void yezzeyRevertCheatRelfilenode(RelFileNodeBackend *rnode,
                                         bool cheat) {
#if IsGreenplum6
  return;
#endif
  if (cheat) {
    rnode->node.spcNode = YEZZEYTABLESPACE_OID;
  }
}

void yezzey_init(void) {
  elog(yezzey_log_level, "[YEZZEY_SMGR] init called");
  mdinit();
}

#if IsModernYezzey
void yezzey_open(SMgrRelation reln) {
  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);
  mdopen(reln);
  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
}
#endif

#if IsModernYezzey
void yezzey_close(SMgrRelation reln, ForkNumber forkNum) {
  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);
  mdclose(reln, forkNum);
  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
}
#else
void yezzey_close(SMgrRelation reln, ForkNumber forkNum) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdclose(reln, forkNum);
}
#endif

#if IsModernYezzey
void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo) {
  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);

  mdcreate(reln, forkNum, isRedo);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
}
#else
void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdcreate(reln, forkNum, isRedo);
}
#endif

void yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum,
                      bool isRedo) {
  bool cheat = yezzeyCheatRelfilenode(&rnode);

  mdcreate_ao(rnode, segmentFileNum, isRedo);

  yezzeyRevertCheatRelfilenode(&rnode, cheat);
}

#if IsModernYezzey
bool yezzey_exists(SMgrRelation reln, ForkNumber forkNum) {
  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);

  bool ret = mdexists(reln, forkNum);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);

  return ret;
}
#else
bool yezzey_exists(SMgrRelation reln, ForkNumber forkNum) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return true;
  }

  return mdexists(reln, forkNum);
}
#endif

#if IsModernYezzey
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
#else
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo,
                   char relstorage)
#endif
{
  bool cheat = yezzeyCheatRelfilenode(&rnode);

#if IsModernYezzey
  mdunlink(rnode, forkNum, isRedo);
#else
  if (rnode.node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */

    return;
  }

  mdunlink(rnode, forkNum, isRedo, relstorage);
#endif

  yezzeyRevertCheatRelfilenode(&rnode, cheat);
}

#if IsModernYezzey
void yezzey_unlink_ao(RelFileNodeBackend rnode, ForkNumber forkNum,
                      bool isRedo) {

  bool cheat = yezzeyCheatRelfilenode(&rnode);
  mdunlink_ao(rnode, forkNum, isRedo);

  yezzeyRevertCheatRelfilenode(&rnode, cheat);
}
#endif

void yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                   char *buffer, bool skipFsync) {

  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);
#if IsGreenplum6
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }
#endif

  mdextend(reln, forkNum, blockNum, buffer, skipFsync);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
}

#if PG_VERSION_NUM >= 130000
bool
#else
void
#endif
yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
#if IsGreenplum6
  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);

  mdprefetch(reln, forkNum, blockNum);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
#else

  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);

  bool ret = mdprefetch(reln, forkNum, blockNum);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);

  return ret;
#endif
}

void yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                 char *buffer) {
#if IsGreenplum6
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }
#endif

  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);

  mdread(reln, forkNum, blockNum, buffer);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
}

void yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                  char *buffer, bool skipFsync) {
#if IsGreenplum6
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }
#endif

  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);

  mdwrite(reln, forkNum, blockNum, buffer, skipFsync);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
}

void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum,
                      BlockNumber blockNum, BlockNumber nBlocks) {
#if IsGreenplum6
  /*do nothing */
#else

  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);
  mdwriteback(reln, forkNum, blockNum, nBlocks);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
#endif
}

BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum) {

#if IsGreenplum6
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return 0;
  }
#endif

  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);

  BlockNumber n = mdnblocks(reln, forkNum);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);

  return n;
}

BlockNumber yezzey_mdnblocks(SMgrRelation reln, ForkNumber forknum) {
  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);
  BlockNumber n = mdnblocks(reln, forknum);
  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
  return n;
}

void yezzey_truncate(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber nBlocks) {
#if IsGreenplum6
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }
#endif

  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);
  mdtruncate(reln, forkNum, nBlocks);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
}

void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum) {
#if IsGreenplum6
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }
#endif

  bool cheat = yezzeyCheatRelfilenode(&reln->smgr_rnode);

  mdimmedsync(reln, forkNum);

  yezzeyRevertCheatRelfilenode(&reln->smgr_rnode, cheat);
}

#if IsGreenplum6
void yezzey_pre_ckpt(void) { (void)mdpreckpt(); }

void yezzey_sync(void) { (void)mdsync(); }

void yezzey_post_ckpt(void) { (void)mdpostckpt(); }

#endif

#if IsGreenplum6
static const struct f_smgr yezzey_smgr = {
    .smgr_init = yezzey_init,
    .smgr_shutdown = NULL,
    .smgr_close = yezzey_close,
    .smgr_create = yezzey_create,
    .smgr_create_ao = yezzey_create_ao,
    .smgr_exists = yezzey_exists,
    .smgr_unlink = yezzey_unlink,
    .smgr_extend = yezzey_extend,
    .smgr_prefetch = yezzey_prefetch,
    .smgr_read = yezzey_read,
    .smgr_write = yezzey_write,
    .smgr_writeback = yezzey_writeback,
    .smgr_nblocks = yezzey_nblocks,
    .smgr_truncate = yezzey_truncate,
    .smgr_immedsync = yezzey_immedsync,
    .smgr_pre_ckpt = yezzey_pre_ckpt,
    .smgr_sync = yezzey_sync,
    .smgr_post_ckpt = yezzey_post_ckpt,
};
#else

static const f_smgr yezzey_smgrsw[] = {
    /* magnetic disk */
    {
        .smgr_init = yezzey_init,
        .smgr_open = yezzey_open,
        .smgr_shutdown = NULL,
        .smgr_close = yezzey_close,
        .smgr_create = yezzey_create,
        .smgr_exists = yezzey_exists,
        .smgr_unlink = yezzey_unlink,
        .smgr_extend = yezzey_extend,
        .smgr_prefetch = yezzey_prefetch,
        .smgr_read = yezzey_read,
        .smgr_write = yezzey_write,
        .smgr_writeback = yezzey_writeback,
        .smgr_nblocks = yezzey_mdnblocks,
        .smgr_truncate = yezzey_truncate,
        .smgr_immedsync = yezzey_immedsync,
    },
    /*
     * Relation files that are different from heap, characterised by:
     *     1. variable blocksize
     *     2. block numbers are not consecutive
     *     3. shared buffers are not used
     * Append-optimized relation files currently fall in this category.
     */
    {
        .smgr_init = NULL,
        .smgr_open = yezzey_open,
        .smgr_shutdown = NULL,
        .smgr_close = yezzey_close,
        .smgr_create = yezzey_create,
        .smgr_exists = yezzey_exists,
        .smgr_unlink = yezzey_unlink_ao,
        .smgr_extend = yezzey_extend,
        .smgr_prefetch = yezzey_prefetch,
        .smgr_read = yezzey_read,
        .smgr_write = yezzey_write,
        .smgr_writeback = yezzey_writeback,
        .smgr_nblocks = yezzey_nblocks,
        .smgr_truncate = yezzey_truncate,
        .smgr_immedsync = yezzey_immedsync,
    }};
#endif

static const struct f_smgr_ao yezzey_smgr_ao = {
#if IsModernYezzey
    .smgr_create_ao = yezzey_create_ao,
#endif
    .smgr_FileClose = yezzey_FileClose,
    .smgr_AORelOpenSegFile = yezzey_AORelOpenSegFile,
#if IsModernYezzey
    .smgr_AORelOpenSegFileXlog = yezzey_AORelOpenSegFileXlog,
#endif
    .smgr_FileWrite = yezzey_FileWrite,
    .smgr_FileRead = yezzey_FileRead,
    .smgr_FileSync = yezzey_FileSync,
    .smgr_FileTruncate = yezzey_FileTruncate,
#if !IsModernYezzey
    .smgr_NonVirtualCurSeek = yezzey_NonVirtualCurSeek,
    .smgr_FileSeek = yezzey_FileSeek,
#else
    .smgr_FileDiskSize = yezzey_FileDiskSize,
    .smgr_FileSize = yezzey_FileSize,
#endif
};

#if IsGreenplum6
const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode) {
  return &yezzey_smgr;
}
#elif IsGreenplum7
const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode,
                          SMgrImpl which) {
  return &yezzey_smgrsw[which];
}
#else
void smgr_yezzey(SMgrRelation reln, BackendId backend, SMgrImpl which,
                 Relation rel) {
  reln->smgr = &yezzey_smgrsw[which];
  reln->smgr_ao = &yezzey_smgr_ao;
}
#endif

#if IsGreenplum6
const f_smgr_ao *smgrao_yezzey(void) { return &yezzey_smgr_ao; }
#endif

void smgr_init_yezzey(void) {
#if IsGreenplum6
  smgr_init_standard();
#endif
  yezzey_init();
}
