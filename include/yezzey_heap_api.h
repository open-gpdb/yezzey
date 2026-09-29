#ifndef YEZZEY_HEAP_API_H
#define YEZZEY_HEAP_API_H

#include "pg.h"

#define yezzey_relation_open heap_open
#define yezzey_heap_getnext heap_getnext
#define yezzey_relation_close heap_close
#define yezzey_beginscan heap_beginscan
#define yezzey_endscan heap_endscan

/* catalog */
#define yezzey_systable_beginscan systable_beginscan
#define yezzey_systable_getnext systable_getnext
#define yezzey_systable_endscan systable_endscan


#endif /* YEZZEY_HEAP_API_H */
