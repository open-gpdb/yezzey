#pragma once

#include "pg.h"

#if PG_VERSION_NUM >= 16000
#define YezzeyLocator RelFileLocator
#define YezzeyGetRelFileLocator(rel) (rel->rd_locator)
#define YezzeyGetRelSpcOid(rnode) (rnode.spcOid)
#define YezzeyGetRelDbOid(rnode) (rnode.dbOid)
#define YezzeyGetRelNode(rnode) (rnode.relNumber)
#else
#define YezzeyLocator RelFileNode
#define YezzeyGetRelFileLocator(rel) (rel->rd_node)
#define YezzeyGetRelSpcOid(rnode) (rnode.spcNode)
#define YezzeyGetRelDbOid(rnode) (rnode.dbNode)
#define YezzeyGetRelNode(rnode)  (rnode.relNode)
#endif