#pragma once

#include "chunkinfo.h"
#include "io_adv.h"
#include "msgproto.h"
#include "yproxy_connector.h"
#include <memory>
#include <vector>
/* reader using yproxy */
class YProxyReader : YProxyConnector {
public:
  friend class ExternalWriter;
  explicit YProxyReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                        std::vector<ChunkInfo> order);
  ~YProxyReader();

public:
  virtual bool read(char *buffer, size_t *amount);

  virtual bool empty();

  virtual bool close();

protected:
  /* prepare connection for chunk reading */
  std::vector<char> ConstructCatRequest(const ChunkInfo &ci, size_t start_off);
  std::vector<char> ConstructCatRequestOld(const ChunkInfo &ci,
                                           size_t start_off);
  virtual int prepareYproxyConnection(const ChunkInfo &ci, size_t start_off);

private:
  uint64_t order_ptr_{0};
  const std::vector<ChunkInfo> order_;
  int64_t current_chunk_remaining_bytes_{0};
  int64_t current_chunk_offset_{0};

  int current_retry{0};
  int retry_limit{1};
};
