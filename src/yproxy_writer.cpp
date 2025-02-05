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
  auto msg = CommonCostructCommandCompleteRequest();

  // signal that current chunk is full
  if (commonWriteFull(client_fd_, msg) == -1) {
    ::close(client_fd_);
    client_fd_ = -1;
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
  auto msg = ConstructCopyDataRequest(buffer, *amount);

  if (commonWriteFull(client_fd_, msg) == -1) {
    *amount = 0;
    return false;
  }
  // *amount does not need to change in case of successfull write

  return true;
}

int YProxyWriter::prepareYproxyConnection() {
  // open unix data socket
  int rb = YProxyConnector::prepareYproxyConnection();
  if (rb != 0) {
    return rb;
  }

  auto msg = ConstructPutRequest(storage_path_);

  if (commonWriteFull(client_fd_, msg) == -1) {
    return -1;
  }

  return 0;
}

std::vector<char> YProxyWriter::ConstructPutRequestOld(std::string fileName) {
  uint64_t settingsCnt = 4;
  uint64_t settingsMsgSpace = 0;

  std::vector<std::pair<std::string, std::string>> settings = {
      {"StorageClass", adv_->storage_class},
      {"MultipartChunksize", std::to_string(adv_->multipart_chunksize)},
      {"MultipartUpload", adv_->multipart_upload ? "1" : "0"},
      {"TableSpace", adv_->tableSpace},
  };

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    settingsMsgSpace += settings[j].first.size() + 1;
    settingsMsgSpace += settings[j].second.size() + 1;
  }

  std::vector<char> buff(MSG_HEADER_SIZE + PROTO_HEADER_SIZE + fileName.size() +
                             1 + MSG_HEADER_SIZE + settingsMsgSpace,
                         0);

  uint64_t len = buff.size();

  uint64_t cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  buff[8] = MessageTypePutV2;

  if (adv_->use_gpg_crypto) {
    buff[9] = EncryptRequest;
  } else {
    buff[9] = NoEncryptRequest;
  }

  strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, fileName.c_str(),
          fileName.size());
  /* no need to set null byte */

  uint64_t settings_offset =
      MSG_HEADER_SIZE + PROTO_HEADER_SIZE + fileName.size() + 1;

  cp = settingsCnt;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[settings_offset + i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  settings_offset += MSG_HEADER_SIZE;

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    strncpy(buff.data() + settings_offset, settings[j].first.c_str(),
            settings[j].first.size());
    settings_offset += settings[j].first.size() + 1;

    strncpy(buff.data() + settings_offset, settings[j].second.c_str(),
            settings[j].second.size());
    settings_offset += settings[j].second.size() + 1;
  }

  return buff;
}

std::vector<char> YProxyWriter::ConstructPutRequest(std::string fileName) {
  uint64_t settingsCnt = 4;

  std::vector<std::pair<std::string, std::string>> settings = {
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
      .addProto(MessageTypePutV2,
                adv_->use_gpg_crypto ? EncryptRequest : NoEncryptRequest)
      .addString(fileName)
      .addUInt64(settingsCnt);

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.addString(settings[j].first).addString(settings[j].second);
  }

  return builder.get();
}

std::vector<char> YProxyWriter::ConstructCopyDataRequestOld(const char *buffer,
                                                            size_t amount) {
  std::vector<char> buff(MSG_HEADER_SIZE + PROTO_HEADER_SIZE + 8 + amount, 0);
  buff[8] = MessageTypeCopyData;
  uint64_t len = buff.size();

  memcpy(buff.data() + 20, buffer, amount);

  uint64_t cp = amount;
  for (ssize_t i = 19; i >= 12; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  return buff;
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