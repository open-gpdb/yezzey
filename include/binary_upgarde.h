#pragma once

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif


EXTERNC void YezzeyBinaryUpgrade(void);