#include "postgres.h"
#include "proxy.h"
#include "yezzey.h"

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

const f_smgr_ao *smgrao_yezzey(void) { return &yezzey_smgr_ao; }
