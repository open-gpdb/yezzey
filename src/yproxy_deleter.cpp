#include "yproxy_deleter.h"

YProxyDeleter::YProxyDeleter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                             bool confirm)
    : YProxyConnector(adv, segindx), garbage_cleanup_(true), confirm_(confirm) {
}

YProxyDeleter::YProxyDeleter(std::shared_ptr<IOadv> adv)
    : YProxyConnector(adv, -1), garbage_cleanup_(false), confirm_(true) {}

YProxyDeleter::~YProxyDeleter() { close(); }

bool YProxyDeleter::deleteChunk(const std::string &chunkName) {
  if (client_fd_ == -1) {
    if (prepareYproxyConnection() == -1) {
      // Throw here?
      close();
      return false;
    }
  }

  // TODO: split to chunks
  auto msg = ConstructDeleteRequest(chunkName);

  if (commonWriteFull(client_fd_, msg) == -1) {
    close();
    return false;
  }
  // *amount does not need to change in case of successfull write

  msg = CommonCostructCommandCompleteRequest();
  // signal that current chunk is full
  if (commonWriteFull(client_fd_, msg) == -1) {
    close();
    return false;
  }
  // wait for responce
  if (commonReadRFQResponce(client_fd_) != 0) {
    close();
    return false;
  }

  return true;
}

/*
        Name    string
        Port    uint64
        Segnum  uint64
        Confirm bool
        Garbage bool
*/
std::vector<char>
YProxyDeleter::ConstructDeleteRequest(std::string fileName) {
  std::vector<char> buff(MSG_HEADER_SIZE + PROTO_HEADER_SIZE + fileName.size() +
                             1 + OFFSET_SZ + OFFSET_SZ,
                         0);
  buff[8] = MessageTypeDelete;
  /* confirm */
  buff[9] = confirm_;
  /* garbage */
  buff[10] = garbage_cleanup_;

  uint64_t len = buff.size();

  uint64_t cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, fileName.c_str(),
          fileName.size());

  uint64_t port = PostPortNumber;

  int off = MSG_HEADER_SIZE + PROTO_HEADER_SIZE + fileName.size() + 1;

  for (ssize_t i = off + 7; i >= off; --i) {
    buff[i] = port & ((1 << 8) - 1);
    port >>= 8;
  }

  off += OFFSET_SZ;

  uint64_t segId = segindx_;
  for (ssize_t i = off + 7; i >= off; --i) {
    buff[i] = segId & ((1 << 8) - 1);
    segId >>= 8;
  }

  off += OFFSET_SZ;
  return buff;
}

int YProxyDeleter::prepareYproxyConnection() {
  // open unix data socket
  return YProxyConnector::prepareYproxyConnection();
}

bool YProxyDeleter::close() { return YProxyConnector::close(); }
