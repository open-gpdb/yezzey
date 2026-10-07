#ifndef YEZZEY_PROXY_H
#define YEZZEY_PROXY_H

#include "pg.h"

#include "gucs.h"
#include "storage.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

typedef File SMGRFile;

EXTERNC int64 yezzey_NonVirtualCurSeek(SMGRFile file);
EXTERNC void yezzey_FileClose(SMGRFile file);
EXTERNC int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence);

EXTERNC int yezzey_FileSync(SMGRFile file, uint32 wait_event_info);

EXTERNC File yezzey_AORelOpenSegFile(Oid reloid, const char *fileName,
                                     int fileFlags);
EXTERNC File yezzey_AORelOpenSegFileXlog(RelFileNode node,
                                         int32 segmentFileNum, int fileFlags);

#if PG_VERSION_NUM >= 160000
EXTERNC int yezzey_FileWrite(SMGRFile file, const void *buffer, size_t amount,
                             off_t offset, uint32 wait_event_info);
EXTERNC int yezzey_FileRead(SMGRFile file, void *buffer, size_t amount,
                            off_t offset, uint32 wait_event_info);
#else
EXTERNC int yezzey_FileWrite(SMGRFile file, char *buffer, int amount,
                             off_t offset, uint32 wait_event_info);
EXTERNC int yezzey_FileRead(SMGRFile file, char *buffer, int amount,
                            off_t offset, uint32 wait_event_info);
#endif

EXTERNC int yezzey_FileTruncate(SMGRFile file, int64 offset,
                                uint32 wait_event_info);

EXTERNC off_t yezzey_FileDiskSize(File file);

EXTERNC off_t yezzey_FileSize(File file);

#endif /* YEZZEY_PROXY_H */
