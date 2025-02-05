#pragma once

#include "yproxy_connector.h"

// list external storage using yproxy
class YProxyLister : YProxyConnector {
public:
  explicit YProxyLister(std::shared_ptr<IOadv> adv, ssize_t segindx);

  virtual ~YProxyLister();

  virtual std::vector<storageChunkMeta> list_relation_chunks();
  virtual std::vector<std::string> list_chunk_names();

  virtual bool close();

protected:
  std::vector<char> ConstructListRequest(std::string fileName);

  virtual int prepareYproxyConnection();

  struct message {
    char type;
    std::vector<char> content;
    int retCode;
  };
  message readMessage();
  std::vector<storageChunkMeta> readObjectMetaBody(std::vector<char> *body);
};
