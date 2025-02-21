#pragma once

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC void YezzeyBinaryUpgrade(void);

EXTERNC void YezzeyInitMetadata(void);

EXTERNC void YezzeyBinaryUpgrade183(void);