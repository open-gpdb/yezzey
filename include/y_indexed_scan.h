#pragma once

#include "pg.h"

/* Struct for heap-or-index scans of system tables */
typedef struct YScanDescData
{
	Relation	heap_rel;		/* catalog being scanned */
	Relation	irel;			/* NULL if doing heap scan */
	HeapScanDesc scan;			/* only valid in heap-scan case */
	IndexScanDesc iscan;		/* only valid in index-scan case */
	Snapshot	snapshot;		/* snapshot to unregister at end of scan */
}	YScanDescData;


typedef YScanDescData* YScanDesc;

extern YScanDesc y_table_beginscan(Relation heapRelation,
				   Oid indexId,
				   bool indexOK,
				   Snapshot snapshot,
				   int nkeys, ScanKey key);
extern HeapTuple y_table_getnext(YScanDesc sysscan);
extern void y_table_endscan(YScanDesc sysscan);