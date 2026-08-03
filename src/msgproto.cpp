#include "msgproto.h"

MsgBuilder::MsgBuilder() : length(UINT64_SZ), cursor(0), data() {}

ssize_t MsgBuilder::putChar(char c, ssize_t padding) {
  data.at(padding) = c;
  return CHAR_SZ;
}

ssize_t MsgBuilder::putUInt64(uint64_t val, ssize_t padding) {
  uint64_t cp = val;
  for (ssize_t i = UINT64_SZ - 1; i >= 0; --i) {
    data[i + padding] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }
  return UINT64_SZ;
}

ssize_t MsgBuilder::putString(std::string val, ssize_t padding) {
  strncpy(data.data() + padding, val.c_str(), val.size());
  return val.size() + CHAR_SZ;
}
ssize_t MsgBuilder::putProto(char farg, char sarg, char targ, char larg,
                             ssize_t padding) {
  padding += this->putChar(farg, padding);
  padding += this->putChar(sarg, padding);
  padding += this->putChar(targ, padding);
  padding += this->putChar(larg, padding);
  return PROTO_HEADER_SIZE;
}
ssize_t MsgBuilder::putBytes(const char *bytes, ssize_t len, ssize_t padding) {
  memcpy(data.data() + padding, bytes, len);
  return len;
}
MsgBuilder &MsgBuilder::fieldMessageType() {
  length += UINT64_SZ;
  return *this;
}

MsgBuilder &MsgBuilder::fieldUInt64() {
  length += UINT64_SZ;
  return *this;
}

MsgBuilder &MsgBuilder::fieldString(ssize_t len) {
  length += len + CHAR_SZ;
  return *this;
}

MsgBuilder &MsgBuilder::fieldProto() {
  length += PROTO_HEADER_SIZE;
  return *this;
}
MsgBuilder &MsgBuilder::fieldBytes(ssize_t len) {
  length += len;
  return *this;
}
MsgBuilder &MsgBuilder::endDescription() {
  data = std::vector<char>(length, 0);
  cursor += this->putUInt64(length, cursor);
  return *this;
}

MsgBuilder &MsgBuilder::addMessageType(char message_type) {
  this->putChar(message_type, cursor);
  cursor += UINT64_SZ;
  return *this;
}
MsgBuilder &MsgBuilder::addUInt64(uint64_t val) {
  cursor += this->putUInt64(val, cursor);
  return *this;
}
MsgBuilder &MsgBuilder::addString(std::string str) {
  cursor += this->putString(str, cursor);
  return *this;
}

MsgBuilder &MsgBuilder::addProto(char farg, char sarg, char targ, char larg) {
  cursor += this->putProto(farg, sarg, targ, larg, cursor);
  return *this;
}
MsgBuilder &MsgBuilder::addProto(char farg, char sarg, char targ) {
  cursor += this->putProto(farg, sarg, targ, 0, cursor);
  return *this;
}
MsgBuilder &MsgBuilder::addProto(char farg, char sarg) {
  cursor += this->putProto(farg, sarg, 0, 0, cursor);
  return *this;
}
MsgBuilder &MsgBuilder::addProto(char farg) {
  cursor += this->putProto(farg, 0, 0, 0, cursor);
  return *this;
}
MsgBuilder &MsgBuilder::addBytes(const char *bytes, ssize_t len) {
  cursor += this->putBytes(bytes, len, cursor);
  return *this;
}

std::vector<char> MsgBuilder::get() { return data; }