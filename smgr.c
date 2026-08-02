
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
                                              YezzeyLocator rnode,
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
int loadFileFromExternalStorage(YezzeyLocator rnode, BackendId backend,
                                ForkNumber forkNum, BlockNumber blkno) {
  StringInfoData path;
  initStringInfo(&path);

  constructExtenrnalStorageFilepath(&path, rnode, backend, forkNum, blkno);

  return 0;
}

static void yezzeyCheatRelfilenode(YezzeyLocatorBackend *rnode) {
#if IsGreenplum6
  rnode->node.spcNode = runningRewriteSpcOidHint ? runningRewriteSpcOidHint
                                                 : DEFAULTTABLESPACE_OID;
#else
  rnode->node.spcNode = DEFAULTTABLESPACE_OID;
#endif
}

static void yezzeyRevertCheatRelfilenode(YezzeyLocatorBackend *rnode) {
  rnode->node.spcNode = YEZZEYTABLESPACE_OID;
}

void yezzey_init(void) {
  elog(yezzey_log_level, "[YEZZEY_SMGR] init called");
  mdinit();
}

#define IsYezzeyOperateSpc(spc) ((spc) == YEZZEYTABLESPACE_OID)

#if IsModernYezzey
void yezzey_open(SMgrRelation reln) {
  if (IsYezzeyOperateSpc(YezzeySMGRLocator(reln).node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdopen(reln);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdopen(reln);
  }
}
#endif

void yezzey_close(SMgrRelation reln, ForkNumber forkNum) {

  if (IsYezzeyOperateSpc(YezzeySMGRLocator(reln).node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdclose(reln, forkNum);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdclose(reln, forkNum);
  }
}

void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo) {
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdcreate(reln, forkNum, isRedo);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdcreate(reln, forkNum, isRedo);
  }
}

void yezzey_create_ao(YezzeyLocatorBackend rnode, int32 segmentFileNum,
                      bool isRedo) {
  if (IsYezzeyOperateSpc(rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&rnode);
    PG_TRY();
    {
      mdcreate_ao(rnode, segmentFileNum, isRedo);
      yezzeyRevertCheatRelfilenode(&rnode);
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&rnode);
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdcreate_ao(rnode, segmentFileNum, isRedo);
  }
}

bool yezzey_exists(SMgrRelation reln, ForkNumber forkNum) {

  bool ret;
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {

      ret = mdexists(reln, forkNum);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    ret = mdexists(reln, forkNum);
  }

  return ret;
}

#if IsModernYezzey
void yezzey_unlink(YezzeyLocatorBackend rnode, ForkNumber forkNum, bool isRedo)
#else
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo,
                   char relstorage)
#endif
{

  if (IsYezzeyOperateSpc(rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&rnode);
    PG_TRY();
    {

#if IsModernYezzey
      mdunlink(rnode, forkNum, isRedo);
#else
      mdunlink(rnode, forkNum, isRedo, relstorage);
#endif
      yezzeyRevertCheatRelfilenode(&rnode);
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&rnode);
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
#if IsModernYezzey
    mdunlink(rnode, forkNum, isRedo);
#else
    mdunlink(rnode, forkNum, isRedo, relstorage);
#endif
  }
}

#if IsModernYezzey
void yezzey_unlink_ao(YezzeyLocatorBackend rnode, ForkNumber forkNum,
                      bool isRedo) {
  if (IsYezzeyOperateSpc(rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&rnode);
    PG_TRY();
    {
      mdunlink_ao(rnode, forkNum, isRedo);
      yezzeyRevertCheatRelfilenode(&rnode);
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&rnode);
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdunlink_ao(rnode, forkNum, isRedo);
  }
}
#endif

void yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                   char *buffer, bool skipFsync) {
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdextend(reln, forkNum, blockNum, buffer, skipFsync);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdextend(reln, forkNum, blockNum, buffer, skipFsync);
  }
}

#if PG_VERSION_NUM >= 130000
bool
#else
void
#endif
yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{

#if IsModernYezzey
  bool ret;
#endif
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {
    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
#if IsGreenplum6
      mdprefetch(reln, forkNum, blockNum);
#else
      ret = mdprefetch(reln, forkNum, blockNum);
#endif
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {

#if IsGreenplum6
    mdprefetch(reln, forkNum, blockNum);
#else
    ret = mdprefetch(reln, forkNum, blockNum);
#endif
  }

#if IsModernYezzey
  return ret;
#endif
}

void yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                 char *buffer) {

  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {
    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdread(reln, forkNum, blockNum, buffer);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdread(reln, forkNum, blockNum, buffer);
  }
}

void yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                  char *buffer, bool skipFsync) {

  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
  }
}

void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum,
                      BlockNumber blockNum, BlockNumber nBlocks) {
#if IsGreenplum6
  /*do nothing */
#else
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdwriteback(reln, forkNum, blockNum, nBlocks);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdwriteback(reln, forkNum, blockNum, nBlocks);
  }
#endif
}

BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum) {
  BlockNumber n;
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      n = mdnblocks(reln, forkNum);
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    n = mdnblocks(reln, forkNum);
  }

  return n;
}

BlockNumber yezzey_mdnblocks(SMgrRelation reln, ForkNumber forknum) {
  BlockNumber n;

  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      n = mdnblocks(reln, forknum);

      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    n = mdnblocks(reln, forknum);
  }

  return n;
}

void yezzey_truncate(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber nBlocks) {
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdtruncate(reln, forkNum, nBlocks);

      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdtruncate(reln, forkNum, nBlocks);
  }
}

void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum) {

  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    PG_TRY();
    {
      mdimmedsync(reln, forkNum);

      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(YezzeySMGRLocator(reln)));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdimmedsync(reln, forkNum);
  }
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

#if IsModernYezzey
#define MAX_YEZZEY_SMGR_ID 3
#else
#define MAX_YEZZEY_SMGR_ID 2
#endif

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
    },
#define YEZZEY_IGNORE_SMGR_ID 2
};
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
  if (which >= MAX_YEZZEY_SMGR_ID) {
    elog(ERROR, "corrutped smgr which ID: %d", which);
  }
  reln->smgr_ao = &yezzey_smgr_ao;
  /* This is basically only PAX case for now, do not overwrite */
  if (which == YEZZEY_IGNORE_SMGR_ID) {
    return;
  }
  reln->smgr = &yezzey_smgrsw[which];
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
