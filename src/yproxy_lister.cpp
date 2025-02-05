#include "yproxy_lister.h"
#include "url.h"

/*
 *
 *  Yproxy Lister
 */

YProxyLister::YProxyLister(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : YProxyConnector(adv, segindx) {}

YProxyLister::~YProxyLister() { close(); }

bool YProxyLister::close() { return YProxyConnector::close(); }

int YProxyLister::prepareYproxyConnection() {
  // open unix data socket
  return YProxyConnector::prepareYproxyConnection();
}

std::vector<storageChunkMeta> YProxyLister::list_relation_chunks() {
  std::vector<storageChunkMeta> res;
  auto ret = prepareYproxyConnection();
  if (ret != 0) {
    // throw?
    return res;
  }

  auto msg = ConstructListRequest(yezzey_block_db_file_path(
      adv_->nspname, adv_->relname, adv_->coords_, segindx_));
  size_t rc = ::write(client_fd_, msg.data(), msg.size());
  if (rc <= 0) {
    // throw?
    return res;
  }

  std::vector<storageChunkMeta> meta;
  while (true) {
    auto message = readMessage();
    switch (message.type) {
    case MessageTypeObjectMeta:
      meta = readObjectMetaBody(&message.content);
      res.insert(res.end(), meta.begin(), meta.end());
      break;
    case MessageTypeReadyForQuery:
      return res;

    default:
      // throw?
      return res;
    }
  }
}

std::vector<std::string> YProxyLister::list_chunk_names() {
  auto chunk_meta = list_relation_chunks();
  std::vector<std::string> res(chunk_meta.size());

  for (size_t i = 0; i < chunk_meta.size(); i++) {
    res[i] = chunk_meta[i].chunkName;
  }
  return res;
}

std::vector<char> YProxyLister::ConstructListRequestOld(std::string fileName) {
  std::vector<char> buff(
      MSG_HEADER_SIZE + PROTO_HEADER_SIZE + fileName.size() + 1, 0);
  buff[8] = MessageTypeList;
  uint64_t len = buff.size();

  strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, fileName.c_str(),
          fileName.size());

  uint64_t cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  return buff;
}

std::vector<char> YProxyLister::ConstructListRequest(std::string fileName) {
  MsgBuilder builder =
      MsgBuilder().fieldProto().fieldString(fileName.size()).endDescription();

  builder.addProto(MessageTypeList).addString(fileName);

  return builder.get();
}

YProxyLister::message YProxyLister::readMessage() {
  YProxyLister::message res;
  size_t len = MSG_HEADER_SIZE;
  char buffer[len];
  // try to read small number of bytes in one op
  // if failed, give up
  int rc = ::read(client_fd_, buffer, len);
  if (rc < 0 || (size_t)rc != len) {
    // handle
    res.retCode = -1;
    return res;
  }

  uint64_t msgLen = 0;
  for (int i = 0; i < 8; i++) {
    msgLen <<= 8;
    msgLen += uint8_t(buffer[i]);
  }

  // substract header
  msgLen -= len;

  char data[msgLen];
  rc = ::read(client_fd_, data, msgLen);

  if (rc < 0) {
    // handle
    res.retCode = -1;
    return res;
  }

  if ((uint64_t)rc != msgLen) {
    // handle
    res.retCode = -1;
    return res;
  }

  res.type = data[0];
  res.content = std::vector<char>(data, data + msgLen);
  return res;
}

std::vector<storageChunkMeta>
YProxyLister::readObjectMetaBody(std::vector<char> *body) {
  std::vector<storageChunkMeta> res;
  size_t i = PROTO_HEADER_SIZE;
  while (i < body->size()) {
    std::vector<char> buff;
    while (body->at(i) != 0 && i < body->size()) {
      buff.push_back(body->at(i));
      i++;
    }
    i++;
    std::string path(buff.begin(), buff.end());
    if (body->size() - i < 8) {
      // throw?
      return res;
    }
    int64_t size = 0;
    for (size_t j = i; j < i + 8; j++) {
      size <<= 8;
      size += uint8_t(body->at(j));
    }
    i += 8;

    storageChunkMeta meta;
    meta.chunkName = path;
    meta.chunkSize = size;
    res.push_back(meta);
  }

  return res;
}
