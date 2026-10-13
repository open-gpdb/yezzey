#ifndef YEZZEY_PROXY_H
#define YEZZEY_PROXY_H

#include "pg.h"

#include "gucs.h"
#include "storage.h"

#include "relfilelocator.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC int64 yezzey_NonVirtualCurSeek(File file);
EXTERNC void yezzey_FileClose(File file);
EXTERNC int64 yezzey_FileSeek(File file, int64 offset, int whence);

EXTERNC int yezzey_FileSync(File file, uint32 wait_event_info);

EXTERNC File yezzey_AORelOpenSegFile(Oid reloid, const char *fileName,
                                     int fileFlags);
EXTERNC File yezzey_AORelOpenSegFileXlog(YezzeyLocator node,
                                         int32 segmentFileNum, int fileFlags);

#if PG_VERSION_NUM >= 160000
EXTERNC int yezzey_FileWrite(File file, const void *buffer, size_t amount,
                             off_t offset, uint32 wait_event_info);
EXTERNC int yezzey_FileRead(File file, void *buffer, size_t amount,
                            off_t offset, uint32 wait_event_info);
#else
EXTERNC int yezzey_FileWrite(File file, char *buffer, int amount, off_t offset,
                             uint32 wait_event_info);
EXTERNC int yezzey_FileRead(File file, char *buffer, int amount, off_t offset,
                            uint32 wait_event_info);
#endif

EXTERNC int yezzey_FileTruncate(File file, int64 offset,
                                uint32 wait_event_info);

EXTERNC off_t yezzey_FileDiskSize(File file);

EXTERNC off_t yezzey_FileSize(File file);

#endif /* YEZZEY_PROXY_H */
