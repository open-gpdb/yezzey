

#ifndef YEZZEY_XVACUUM_H
#define YEZZEY_XVACUUM_H

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC int yezzey_delete_chunk_internal(const char *external_chunk_path, int segindx);

#endif /* YEZZEY_XVACUUM_H */