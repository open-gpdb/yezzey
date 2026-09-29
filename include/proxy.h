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

#ifndef OPENGPDB
typedef File SMGRFile;
#endif

EXTERNC int64 yezzey_NonVirtualCurSeek(SMGRFile file);
EXTERNC void yezzey_FileClose(SMGRFile file);
EXTERNC int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence);

#ifdef OPENGPDB
EXTERNC int yezzey_FileSync(SMGRFile file);
#else
EXTERNC int yezzey_FileSync(SMGRFile file, uint32 wait_event_info);
#endif

#ifdef OPENGPDB
EXTERNC SMGRFile yezzey_AORelOpenSegFile(Oid reloid, const char *nspname,
                                         const char *relname, FileName fName,
                                         int fileFlags, int fileMode,
                                         int64 modcount);
#else
EXTERNC File yezzey_AORelOpenSegFile(Oid reloid, const char *fileName,
                                     int fileFlags);
EXTERNC File yezzey_AORelOpenSegFileXlog(YezzeyLocator node,
                                         int32 segmentFileNum, int fileFlags);
#endif

#ifdef OPENGPDB
EXTERNC int yezzey_FileWrite(SMGRFile file, char *buffer, int amount);
EXTERNC int yezzey_FileRead(SMGRFile file, char *buffer, int amount);
#else
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
#endif

#ifdef OPENGPDB
EXTERNC int yezzey_FileTruncate(SMGRFile file, int64 offset);
#else
EXTERNC int yezzey_FileTruncate(SMGRFile file, int64 offset,
                                uint32 wait_event_info);
#endif

#ifndef OPENGPDB
EXTERNC off_t yezzey_FileDiskSize(File file);

EXTERNC off_t yezzey_FileSize(File file);
#endif

#endif /* YEZZEY_PROXY_H */
