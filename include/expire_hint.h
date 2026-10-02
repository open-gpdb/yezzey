#pragma once

#include "pg.h"

#ifdef __cplusplus

#include <string>
#include <vector>

#include "chunkinfo.h"

#endif

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#define YEZZEY_EXPIRE_HINT_RELATION 8550
#define YEZZEY_EXPIRE_HINT_IDX_RELATION 8551

/* ----------------
 *		compiler constants for pg_database
 * ----------------
 */

typedef struct {
  XLogRecPtr lsn; /* Chunk lsn */
  text x_path;    /* external path */
} FormData_yezzey_expire_hint;

typedef FormData_yezzey_expire_hint *Form_yezzey_expire_hint;

#define Natts_yezzey_expire_hint 2

#define Anum_yezzey_expire_hint_lsn 1
/* variable-len params should go last */
#define Anum_yezzey_expire_hint_x_path 2

#ifdef __cplusplus
void YezzeyCreateExpireHint();

void YezzeyCreateExpireHintIdx();
#else
#endif
