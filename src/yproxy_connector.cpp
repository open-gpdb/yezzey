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
