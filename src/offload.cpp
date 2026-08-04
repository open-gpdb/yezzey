
/*
 *
 * file: src/offload.cpp
 */

#include "offload.h"
#include "gucs.h"

#include "relfilelocator.h"
#include "storage.h"

/*
 * yezzey_offload_relation_internal_rel: do the offloading job
 * aorel should be locked in AccessExclusiveLock
 */
void yezzey_offload_relation_internal_rel(Relation aorel, bool remove_locally,
                                          const char *external_storage_path) {
  int total_segfiles;
  FileSegInfo **segfile_array;
  AOCSFileSegInfo **segfile_array_cs;

  auto nvp = aorel->rd_att->natts;

#if IsModernYezzey
  Oid segrelid;
#endif

  /*
   * Relation segments named base/DBOID/YezzeyGetRelFileLocator(aorel).*
   */

#if IsModernYezzey
  elog(yezzey_log_level, "offloading relation %s, relnode %u",
       RelationGetRelationName(aorel),
       YezzeyGetRelNode(YezzeyGetRelFileLocator(aorel)));
#else
  elog(yezzey_log_level, "offloading relation %s, relnode %d",
       RelationGetRelationName(aorel),
       YezzeyGetRelNode(YezzeyGetRelFileLocator(aorel)));
#endif

  /* for now, we locked relation */

  /* GetAllFileSegInfo_pg_aoseg_rel */

  /* acquire snapshot for aoseg table lookup */
  auto appendOnlyMetaDataSnapshot = SnapshotSelf;

  if (RelationIsAoRows(aorel)) {
    /* Get information about all the file segments we need to scan */
#if IsModernYezzey
    segfile_array = GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot,
                                      &total_segfiles, &segrelid);
#else
    segfile_array =
        GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);
#endif

    for (int i = 0; i < total_segfiles; i++) {
      auto segno = segfile_array[i]->segno;
      auto modcount = segfile_array[i]->modcount;
      auto logicalEof = segfile_array[i]->eof;

      elog(yezzey_log_level,
           "offloading segment no %d, modcount %ld up to logial eof %ld", segno,
           modcount, logicalEof);

      yezzey_offload_relation_internal_rel(aorel, true, NULL);
      /* segment if offloaded */
    }

    if (segfile_array) {
      FreeAllSegFileInfo(segfile_array, total_segfiles);
      pfree(segfile_array);
    }
  } else if (RelationIsAoCols(aorel)) {
    /* ao columns, relstorage == 'c' */
#if IsGreenplum6
    segfile_array_cs = GetAllAOCSFileSegInfo(aorel, appendOnlyMetaDataSnapshot,
                                             &total_segfiles);
#else
    segfile_array_cs = GetAllAOCSFileSegInfo(aorel, appendOnlyMetaDataSnapshot,
                                             &total_segfiles, &segrelid);
#endif
    for (int inat = 0; inat < nvp; ++inat) {
      for (int i = 0; i < total_segfiles; i++) {
        auto segno = segfile_array_cs[i]->segno;
        /* in AOCS case actual *segno* differs from segfile_array_cs[i]->segno
         * whis is logical number of segment. On physical level, each logical
         * segno (segfile_array_cs[i]->segno) is represented by
         * AOTupleId_MultiplierSegmentFileNum in storage (1 file per attribute)
         */
        auto pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
        auto modcount = segfile_array_cs[i]->modcount;
        auto logicalEof = segfile_array_cs[i]->vpinfo.entry[inat].eof;
        elog(yezzey_ao_log_level,
             "offloading cs segment no %d, pseudosegno %d, modcount %ld, up to "
             "eof %ld",
             segno, pseudosegno, modcount, logicalEof);

        offloadRelationSegment(aorel, pseudosegno, modcount, logicalEof,
                               external_storage_path);
        /* segment if offloaded */
      }
    }

    if (segfile_array_cs) {
      FreeAllAOCSSegFileInfo(segfile_array_cs, total_segfiles);
      pfree(segfile_array_cs);
    }
  } else {
    elog(ERROR, "wrong relation storage type, not AO/AOCS");
  }

  /* insert entry in relocate table, is no any */

  /* cleanup */
}

/*
 * yezzey_offload_relation_internal:
 * offloads relation segments data to external storage.
 * if remove_locally is true,
 * issues ATExecSetTableSpace(tablespace shange to virtual (yezzey) tablespace)
 * which will result in local-storage files drops (on both primary and mirror
 * segments)
 */
void yezzey_offload_relation_internal(Oid reloid, bool remove_locally,
                                      const char *external_storage_path) {
  Relation aorel;
  /* need sanity checks */

  /*  This mode guarantees that the holder is the only transaction accessing the
   * table in any way. we need to be sure, thar no other transaction either
   * reads or write to given relation because we are going to delete relation
   * from local storage
   */
  aorel = relation_open(reloid, AccessExclusiveLock);
  RelationOpenSmgr(aorel);

  yezzey_offload_relation_internal_rel(aorel, remove_locally,
                                       external_storage_path);

  relation_close(aorel, NoLock);
}
