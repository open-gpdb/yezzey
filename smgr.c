
#include "postgres.h"

// For GpIdentity
#include "c.h"
#include "cdb/cdbvars.h"

#include "catalog/pg_tablespace.h"

#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/shmem.h"
#include "storage/smgr.h"

#include "access/xact.h"

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

static void yezzeyCheatRelfilenode(RelFileNodeBackend *rnode) {
  rnode->node.spcNode = runningRewriteSpcOidHint ? runningRewriteSpcOidHint
                                                 : DEFAULTTABLESPACE_OID;
}

static void yezzeyRevertCheatRelfilenode(RelFileNodeBackend *rnode) {
  rnode->node.spcNode = YEZZEYTABLESPACE_OID;
}

void yezzey_init(void) {
  elog(yezzey_log_level, "[YEZZEY_SMGR] init called");
  mdinit();
}

#define IsYezzeyOperateSpc(spc) ((spc) == YEZZEYTABLESPACE_OID)

void yezzey_close(SMgrRelation reln, ForkNumber forkNum) {

  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      mdclose(reln, forkNum);
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdclose(reln, forkNum);
  }
}

void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo) {
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      mdcreate(reln, forkNum, isRedo);
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdcreate(reln, forkNum, isRedo);
  }
}

void yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum,
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

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {

      ret = mdexists(reln, forkNum);
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    ret = mdexists(reln, forkNum);
  }

  return ret;
}

void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo,
                   char relstorage) {

  if (IsYezzeyOperateSpc(rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&rnode);
    PG_TRY();
    {
      mdunlink(rnode, forkNum, isRedo, relstorage);
      yezzeyRevertCheatRelfilenode(&rnode);
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&rnode);
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdunlink(rnode, forkNum, isRedo, relstorage);
  }
}

void yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                   char *buffer, bool skipFsync) {
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      mdextend(reln, forkNum, blockNum, buffer, skipFsync);
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdextend(reln, forkNum, blockNum, buffer, skipFsync);
  }
}

void yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber blockNum) {
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {
    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      mdprefetch(reln, forkNum, blockNum);
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {

    mdprefetch(reln, forkNum, blockNum);
  }
}

void yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                 char *buffer) {

  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {
    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      mdread(reln, forkNum, blockNum, buffer);
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
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

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
  }
}

void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum,
                      BlockNumber blockNum, BlockNumber nBlocks) {}

BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum) {
  BlockNumber n;
  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      n = mdnblocks(reln, forkNum);
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
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

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      n = mdnblocks(reln, forknum);

      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
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

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      mdtruncate(reln, forkNum, nBlocks);

      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdtruncate(reln, forkNum, nBlocks);
  }
}

void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum) {

  if (IsYezzeyOperateSpc(reln->smgr_rnode.node.spcNode)) {

    yezzeyCheatRelfilenode(&(reln->smgr_rnode));
    PG_TRY();
    {
      mdimmedsync(reln, forkNum);

      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
    }
    PG_CATCH();
    {
      yezzeyRevertCheatRelfilenode(&(reln->smgr_rnode));
      PG_RE_THROW();
    }
    PG_END_TRY();
  } else {
    mdimmedsync(reln, forkNum);
  }
}

void yezzey_pre_ckpt(void) { (void)mdpreckpt(); }

void yezzey_sync(void) { (void)mdsync(); }

void yezzey_post_ckpt(void) { (void)mdpostckpt(); }

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

static const struct f_smgr_ao yezzey_smgr_ao = {
    .smgr_FileClose = yezzey_FileClose,
    .smgr_AORelOpenSegFile = yezzey_AORelOpenSegFile,
    .smgr_FileWrite = yezzey_FileWrite,
    .smgr_FileRead = yezzey_FileRead,
    .smgr_FileSync = yezzey_FileSync,
    .smgr_FileTruncate = yezzey_FileTruncate,
    .smgr_NonVirtualCurSeek = yezzey_NonVirtualCurSeek,
    .smgr_FileSeek = yezzey_FileSeek,
};

const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode) {
  return &yezzey_smgr;
}

const f_smgr_ao *smgrao_yezzey(void) { return &yezzey_smgr_ao; }

void smgr_init_yezzey(void) {
  smgr_init_standard();
  yezzey_init();
}
