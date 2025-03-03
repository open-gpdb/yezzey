#pragma once

#include "pg.h"
#include "virtual_index.h"
#include "ygpver.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#if IsModernYezzey
#define YEZZEY_AUX_NAMESPACE 8001
#define YEZZEYTABLESPACE_OID 8555
#endif

EXTERNC void YezzeyUpdateMetadataRelations(
    Oid yandexoid /*yezzey auxiliary index oid*/, Oid reloid,
    Oid relfilenodeOid, int64_t blkno, int64_t offset_start,
    int64_t offset_finish, bool encrypted, bool kek, int32_t reused,
    int64_t modcount, XLogRecPtr lsn, const char *x_path /* external path */,
    const char *md5);