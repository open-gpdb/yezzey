#pragma once

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC void YezzeyInitMetadata(void);

EXTERNC void YezzeyBinaryUpdate183(void);