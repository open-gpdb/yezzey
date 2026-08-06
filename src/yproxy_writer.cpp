#include "yproxy_writer.h"
#include "url.h"

std::string YProxyWriter::createXPath() {
  return craftStorageUnPrefixedPath(adv_, segindx_, modcount_,
                                    insertion_rec_ptr_);
}

YProxyWriter::YProxyWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           ssize_t modcount, const std::string &storage_path)
    : YProxyConnector(adv, segindx), modcount_(modcount),
      insertion_rec_ptr_(yezzeyGetXStorageInsertLsn()),
      storage_path_(createXPath()) {}

YProxyWriter::~YProxyWriter() { close(); }

// complete external storage interaction.
// TBD: smgr_FileSync() here ?
bool YProxyWriter::close() {
  if (client_fd_ == -1) {
    return true;
  }
  const auto msg = CommonCostructCopyDoneRequest();

  // signal that current chunk is full
  if (commonWriteFull(client_fd_, msg) == -1) {
    ::close(client_fd_);
    client_fd_ = -1;
    return false;
  }

  if (readPutCompleteResponce(client_fd_) != 0) {
    ::close(client_fd_);
    client_fd_ = -1;
    // TODO: handle
    return false;
  }

  // wait for responce
  if (commonReadRFQResponce(client_fd_) != 0) {
    ::close(client_fd_);
    client_fd_ = -1;
    // some error, handle
    return false;
  }
  ::close(client_fd_);
  client_fd_ = -1;
  return true;
}

bool YProxyWriter::write(const char *buffer, size_t *amount) {
  if (client_fd_ == -1) {
    if (prepareYproxyConnection() == -1) {
      // Throw here?
      return false;
    }
  }

  // TODO: split to chunks
  const auto msg = ConstructCopyDataRequest(buffer, *amount);

  if (commonWriteFull(client_fd_, msg) == -1) {
    // Be tidy
    ::close(client_fd_);
    client_fd_ = -1;
    *amount = 0;
    return false;
  }
  // *amount does not need to change in case of successfull write

  return true;
}

// Initialize extental storage access guts
int YProxyWriter::prepareYproxyConnection() {
  const auto rb = YProxyConnector::prepareYproxyConnection();
  if (rb != 0) {
    return rb;
  }

  const auto msg = ConstructPutRequest(storage_path_);

  if (commonWriteFull(client_fd_, msg) == -1) {
    // Be tidy
    ::close(client_fd_);
    client_fd_ = -1;
    return -1;
  }

  return 0;
}

int YProxyWriter::readPutCompleteResponce(int client_fd_) {
  const auto len = MSG_HEADER_SIZE;
  char buffer[len];
  // try to read small number of bytes in one go
  // if failed, give up
  const auto rc = commonReadFull(client_fd_, buffer, len);
  if (rc != 0) {
    // handle
    return -1;
  }

  uint64_t msgLen = 0;
  for (int i = 0; i < 8; i++) {
    msgLen <<= 8;
    msgLen += uint8_t(buffer[i]);
  }

  if (msgLen != MSG_HEADER_SIZE + PROTO_HEADER_SIZE + 2) {
    // protocol violation
    return -1;
  }

  // substract header
  msgLen -= len;

  char data[msgLen];
  const auto rc2 = commonReadFull(client_fd_, data, msgLen);
  if (rc2 != 0) {
    return -1;
  }

  if (data[0] != MessageTypePutComplete) {
    return -1;
  }
  const uint16_t kv = uint8_t(data[4]) + (1 << 8) * uint16_t(data[5]);
  key_version = kv;

  return 0;
}

std::vector<char>
YProxyWriter::ConstructPutRequest(const std::string &fileName) {
  const uint64_t settingsCnt = 4;

  const std::vector<std::pair<std::string, std::string>> settings = {
      {"StorageClass", adv_->storage_class},
      {"MultipartChunksize", std::to_string(adv_->multipart_chunksize)},
      {"MultipartUpload", adv_->multipart_upload ? "1" : "0"},
      {"TableSpace", adv_->tableSpace},
  };
  MsgBuilder builder =
      MsgBuilder().fieldProto().fieldString(fileName.size()).fieldUInt64();

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.fieldString(settings[j].first.size())
        .fieldString(settings[j].second.size());
  }
  builder.endDescription();

  builder
      .addProto(MessageTypePutV3,
                adv_->use_gpg_crypto ? EncryptRequest : NoEncryptRequest)
      .addString(fileName)
      .addUInt64(settingsCnt);

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.addString(settings[j].first).addString(settings[j].second);
  }

  return builder.get();
}

std::vector<char> YProxyWriter::ConstructCopyDataRequest(const char *buffer,
                                                         size_t amount) {

  MsgBuilder builder = MsgBuilder()
                           .fieldProto()
                           .fieldUInt64()
                           .fieldBytes(amount)
                           .endDescription();

  builder.addProto(MessageTypeCopyData)
      .addUInt64(amount)
      .addBytes(buffer, amount);

  return builder.get();
}
