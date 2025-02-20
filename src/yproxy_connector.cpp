#include "yproxy_connector.h"

YProxyConnector::YProxyConnector(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : adv_(adv), segindx_(segindx), client_fd_(-1) {}

YProxyConnector::~YProxyConnector() { close(); }
bool YProxyConnector::close() {
  if (client_fd_ != -1) {
    ::close(client_fd_);
    client_fd_ = -1;
  }
  return true;
}

int YProxyConnector::prepareYproxyConnection() {
  // open unix data socket

  client_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (client_fd_ == -1) {
    elog(WARNING, "failed to create unix socket, errno: %m");
    return -1;
  }

  struct sockaddr_un addr;
  /* Bind socket to socket name. */

  memset(&addr, 0, sizeof(addr));

  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, adv_->yproxy_socket.c_str(),
          sizeof(addr.sun_path) - 1);

  auto ret =
      ::connect(client_fd_, (const struct sockaddr *)&addr, sizeof(addr));

  if (ret == -1) {
    elog(WARNING,
         "failed to acquire connection to unix socket on %s, errno: %m",
         adv_->yproxy_socket.c_str());
    return -1;
  }
  return 0;
}

int commonReadRFQResponce(int client_fd_) {
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
    return -1;
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
    return -1;
  }
  return 0;
}


int commonWriteFull(int client_fd_, const std::vector<char> &msg) {
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


std::vector<char> CommonCostructCopyDoneRequest() {
  MsgBuilder builder = MsgBuilder().fieldProto().endDescription();

  return builder.addProto(MessageTypeCopyDone).get();
}
