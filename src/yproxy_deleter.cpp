#include "yproxy_deleter.h"

YProxyDeleter::YProxyDeleter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                             bool confirm)
    : YProxyConnector(adv, segindx), garbage_cleanup_(true), confirm_(confirm) {
}

YProxyDeleter::YProxyDeleter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                             bool confirm, bool crazyDrop)
    : YProxyConnector(adv, segindx), garbage_cleanup_(true), confirm_(confirm),
      crazy_drop_(crazyDrop) {}

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
std::vector<char> YProxyDeleter::ConstructDeleteRequest(std::string fileName) {

  MsgBuilder builder = MsgBuilder()
                           .fieldProto()
                           .fieldString(fileName.size())
                           .fieldUInt64()
                           .fieldUInt64()
                           .endDescription();

  builder.addProto(MessageTypeDelete, confirm_, garbage_cleanup_, crazy_drop_)
      .addString(fileName)
      .addUInt64(PostPortNumber)
      .addUInt64(segindx_);

  return builder.get();
}

int YProxyDeleter::prepareYproxyConnection() {
  // open unix data socket
  return YProxyConnector::prepareYproxyConnection();
}

bool YProxyDeleter::close() { return YProxyConnector::close(); }
