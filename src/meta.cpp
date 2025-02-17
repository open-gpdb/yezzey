#include "yezzey_meta.h"

void YezzeyUpdateMetadataRelations(
    Oid yandexoid /*yezzey auxiliary index oid*/, Oid reloid,
    Oid relfilenodeOid, int64_t blkno, int64_t offset_start,
    int64_t offset_finish, bool encrypted, bool kek, int32_t reused, int64_t modcount,
    XLogRecPtr lsn, const char *x_path /* external path */, const char *md5) {
  YezzeyVirtualIndexInsert(yandexoid, reloid, relfilenodeOid, blkno,
                           offset_start, offset_finish, int32_t(YEZZEY_IS_ENC * encrypted + YEZZEY_ENC_KEK * kek), reused,
                           modcount, lsn, x_path);
  /* TODO: update yezzey relfilemap */
}