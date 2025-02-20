#pragma once

#include "io_adv.h"
#include "msgproto.h"
#include "unistd.h"
#include <memory>
#include <string>
#include <vector>

class YProxyConnector {
public:
  explicit YProxyConnector(std::shared_ptr<IOadv> adv, ssize_t segindx);
  virtual ~YProxyConnector();
  virtual bool close();

protected:
  virtual int prepareYproxyConnection();
  std::shared_ptr<IOadv> adv_{nullptr};
  ssize_t segindx_{0};

  int client_fd_{-1};
};

struct storageChunkMeta {
  int64_t chunkSize;
  std::string chunkName;
};


/* in yproxy.cpp */
extern std::vector<char> CommonCostructCopyDoneRequest();

extern int commonWriteFull(int client_fd_, const std::vector<char> &msg);

extern int commonReadRFQResponce(int client_fd_);