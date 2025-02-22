#ifndef YEZZEY_HEAP_API_H
#define YEZZEY_HEAP_API_H

#include "pg.h"

#if PG_VERSION_NUM >= 120000
#define yezzey_relation_open table_open
#define yezzey_systable_beginscan table_beginscan
#define yezzey_beginscan table_beginscan
#define yezzey_endscan table_endscan

/* catalog */

#define yezzey_beginscan_catalog table_beginscan_catalog

#else
#define yezzey_relation_open heap_open
#define yezzey_systable_beginscan systable_beginscan
#define yezzey_beginscan heap_beginscan
#define yezzey_endscan heap_endscan

/* catalog */
#define yezzey_beginscan_catalog heap_beginscan_catalog 

#endif

#endif /* YEZZEY_HEAP_API_H */