

#include "init.h"

#include "offload_policy.h"

void YezzeyInitMetadata(void) {
  (void)YezzeyCreateOffloadPolicyRelation();
  (void)YezzeyCreateVirtualIndex();
}

void YezzeyBinaryUpdate183(void)
{
  (void)YezzeyCreateVirtualIndexIdx();
}