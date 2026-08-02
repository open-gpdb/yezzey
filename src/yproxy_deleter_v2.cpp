#include "yproxy_deleter_v2.h"
#include "scope_guard.h"

YProxyDeleterV2::YProxyDeleterV2(std::shared_ptr<IOadv> adv, ssize_t segindx,
                                 std::string dbname, bool crazy_drop_)
    : YProxyConnector(adv, segindx), crazy_drop_(crazy_drop_), dbname_(dbname) {
}

YProxyDeleterV2::YProxyDeleterV2(std::shared_ptr<IOadv> adv, ssize_t segindx,
                                 std::string dbname)
    : YProxyConnector(adv, segindx), crazy_drop_(false), dbname_(dbname) {}

YProxyDeleterV2::~YProxyDeleterV2() { close(); }

bool YProxyDeleterV2::Delete(const std::string &chunkName) {
  auto connGuard = makeScopeGuard([this] { this->close(); });

  if (client_fd_ == -1) {
    if (prepareYproxyConnection() == -1) {
      return false;
    }
  }

  // TODO: split to chunks
  const auto msg = ConstructDeleteRequest(chunkName);

  if (commonWriteFull(client_fd_, msg) == -1) {
    return false;
  }
  // wait for responce
  if (commonReadRFQResponce(client_fd_) != 0) {
    return false;
  }

  connGuard.dismiss();
  return true;
}

bool YProxyDeleterV2::Collect(const std::string &chunkName) {
  auto connGuard = makeScopeGuard([this] { this->close(); });

  if (client_fd_ == -1) {
    if (prepareYproxyConnection() == -1) {
      return false;
    }
  }

  // TODO: split to chunks
  const auto msg = ConstructCollectRequest(chunkName);

  if (commonWriteFull(client_fd_, msg) == -1) {
    return false;
  }

  // wait for responce
  if (commonReadRFQResponce(client_fd_) != 0) {
    return false;
  }

  connGuard.dismiss();
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
YProxyDeleterV2::ConstructDeleteRequest(const std::string fileName) {

  MsgBuilder builder = MsgBuilder()
                           .fieldProto()
                           .fieldUInt64()
                           .fieldUInt64()
                           .fieldString(dbname_.size())
                           .fieldString(fileName.size())
                           .endDescription();

  builder.addProto(MessageTypeDeleteObsolete, crazy_drop_)
      .addUInt64(segindx_)
      .addUInt64(PostPortNumber)
      .addString(dbname_)
      .addString(fileName);

  return builder.get();
}
std::vector<char>
YProxyDeleterV2::ConstructCollectRequest(const std::string fileName) {

  MsgBuilder builder = MsgBuilder()
                           .fieldProto()
                           .fieldUInt64()
                           .fieldUInt64()
                           .fieldString(dbname_.size())
                           .fieldString(fileName.size())
                           .endDescription();

  builder.addProto(MessageTypeCollectObsolete)
      .addUInt64(segindx_)
      .addUInt64(PostPortNumber)
      .addString(dbname_)
      .addString(fileName);

  return builder.get();
}

int YProxyDeleterV2::prepareYproxyConnection() {
  // open unix data socket
  return YProxyConnector::prepareYproxyConnection();
}

bool YProxyDeleterV2::close() { return YProxyConnector::close(); }
