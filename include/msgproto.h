#pragma once

#include <cstring>
#include <string>
#include <vector>

const char DecryptRequest = 1;
const char NoDecryptRequest = 0;
const char UseKEK = 1;
const char NoUseKEK = 0;
const char ExtendedMessage = 1;
const char EncryptRequest = 1;
const char NoEncryptRequest = 0;
const char MessageTypeCat = 42;
const char MessageTypeCatV2 = 54;
const char MessageTypePut = 43;
const char MessageTypePutV2 = 53;
const char MessageTypePutV3 = 56;
const char MessageTypeCommandComplete = 44;
const char MessageTypeReadyForQuery = 45;
const char MessageTypeCopyData = 46;
const char MessageTypeList = 48;
const char MessageTypeObjectMeta = 49;
const char MessageTypePutComplete = 57;

const char MessageTypeDelete = 47;

const size_t MSG_HEADER_SIZE = 8;
const size_t PROTO_HEADER_SIZE = 4;
const size_t OFFSET_SZ = 8;
const size_t FLAG_SZ = 1;
const size_t CHAR_SZ = 1;
const size_t UINT64_SZ = 8;

class MsgBuilder {
public:
  explicit MsgBuilder();
  // description
  MsgBuilder &fieldMessageType();
  MsgBuilder &fieldUInt64();
  MsgBuilder &fieldString(ssize_t len);
  MsgBuilder &fieldProto();
  MsgBuilder &fieldBytes(ssize_t len);
  MsgBuilder &endDescription();
  // place values
  MsgBuilder &addMessageType(char message_type);
  MsgBuilder &addUInt64(uint64_t val);
  MsgBuilder &addString(std::string str);
  MsgBuilder &addBytes(const char *bytes, ssize_t len);
  MsgBuilder &addProto(char farg);
  MsgBuilder &addProto(char farg, char sarg);
  MsgBuilder &addProto(char farg, char sarg, char targ);
  MsgBuilder &addProto(char farg, char sarg, char targ, char larg);
  std::vector<char> get();

protected:
  ssize_t putUInt64(uint64_t val, ssize_t padding);
  ssize_t putString(std::string val, ssize_t padding);
  ssize_t putChar(char c, ssize_t padding);
  ssize_t putProto(char farg, char sarg, char targ, char larg, ssize_t cursor);
  ssize_t putBytes(const char *bytes, ssize_t len, ssize_t padding);
  ssize_t length;
  ssize_t cursor;
  std::vector<char> data;
};