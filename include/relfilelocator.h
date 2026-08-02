#pragma once

#include "pg.h"

#if PG_VERSION_NUM >= 160000
#define YezzeyLocator RelFileLocator
#define YezzeyLocatorBackend RelFileLocatorBackend
#define YezzeyGetRelFileLocator(rel) (rel->rd_locator)
#define YezzeySMGRLocator(smgr) (smgr->smgr_rlocator)
#define YezzeyGetRelSpcOid(rnode) (rnode.spcOid)
#define YezzeyGetRelDbOid(rnode) (rnode.dbOid)
#define YezzeyGetRelNode(rnode) (rnode.relNumber)
#else
#define YezzeyLocator RelFileNode
#define YezzeyLocatorBackend RelFileNodeBackend
#define YezzeyGetRelFileLocator(rel) (rel->rd_node)
#define YezzeySMGRLocator(smgr) (smgr->smgr_rnode)
#define YezzeyGetRelSpcOid(rnode) (rnode.spcNode)
#define YezzeyGetRelDbOid(rnode) (rnode.dbNode)
#define YezzeyGetRelNode(rnode)  (rnode.relNode)
#endif