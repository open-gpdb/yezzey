#include "yezzey_meta.h"

void YezzeyUpdateMetadataRelations(
    Oid yandexoid /*yezzey auxiliary index oid*/, Oid reloid,
    Oid relfilenodeOid, int64_t blkno, int64_t offset_start,
    int64_t offset_finish, bool encrypted, bool kek, int32_t reused, int64_t modcount,
    XLogRecPtr lsn, const char *x_path /* external path */, const char *md5) {
  int32_t flags = 0;
  if (encrypted)
    flags |= YEZZEY_IS_ENC;
  if (kek)
    flags |= YEZZEY_ENC_KEK;
  YezzeyVirtualIndexInsert(yandexoid, reloid, relfilenodeOid, blkno,
                           offset_start, offset_finish, flags, reused,
                           modcount, lsn, x_path);
  /* TODO: update yezzey relfilemap */
}