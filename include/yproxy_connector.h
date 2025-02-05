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


static std::vector<char> CommonCostructCommandCompleteRequest() {
  MsgBuilder builder = MsgBuilder().fieldProto().endDescription();

  return builder.addProto(MessageTypeCommandComplete).get();
}

static int commonWriteFull(int client_fd_, const std::vector<char> &msg) {
  int len = msg.size();
  int sync_offset = 0;
  while (len > 0) {
    auto rc = ::write(client_fd_, msg.data() + sync_offset, len);

    if (rc <= 0) {
      // handle
      return -1;
    }
    len -= rc;
    sync_offset += rc;
  }
  return 0;
}

static int commonReadRFQResponce(int client_fd_) {
  int len = MSG_HEADER_SIZE;
  char buffer[len];
  // try to read small number of bytes in one op
  // if failed, give up
  int rc = ::read(client_fd_, buffer, len);
  if (rc != len) {
    // handle
    return -1;
  }

  uint64_t msgLen = 0;
  for (int i = 0; i < 8; i++) {
    msgLen <<= 8;
    msgLen += uint8_t(buffer[i]);
  }

  if (msgLen != MSG_HEADER_SIZE + PROTO_HEADER_SIZE) {
    // protocol violation
    return 1;
  }

  // substract header
  msgLen -= len;

  char data[msgLen];
  rc = ::read(client_fd_, data, msgLen);
  if (rc < 0) {
    return -1;
  }
  if (uint64_t(rc) != msgLen) {
    // handle
    return -1;
  }

  if (data[0] != MessageTypeReadyForQuery) {
    return 2;
  }
  return 0;
}