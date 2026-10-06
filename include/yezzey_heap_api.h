#ifndef YEZZEY_HEAP_API_H
#define YEZZEY_HEAP_API_H

#include "pg.h"

#define yezzey_relation_open table_open
#define yezzey_relation_close table_close
#define yezzey_beginscan table_beginscan
#define yezzey_endscan table_endscan

/* catalog */

#define yezzey_systable_beginscan systable_beginscan
#define yezzey_systable_getnext systable_getnext
#define yezzey_systable_endscan systable_endscan

#endif /* YEZZEY_HEAP_API_H */
