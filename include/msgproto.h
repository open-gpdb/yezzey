#pragma once

#include <string>
#include <vector>
#include <cstring>

class MsgBuilder{
public:
    explicit MsgBuilder();
    // description
    MsgBuilder& fieldMessageType();
    MsgBuilder& fieldUInt64();
    MsgBuilder& fieldString(ssize_t val);
    MsgBuilder& endDescription();
    // place values
    MsgBuilder& addMessageType(char message_type);
    MsgBuilder& addUInt64(uint64_t val);
    MsgBuilder& addString(std::string str);
    std::vector<char> get();
protected:
    ssize_t putUInt64(uint64_t val, ssize_t padding);
    ssize_t putString(std::string val, ssize_t padding);
    ssize_t putChar(char c, ssize_t padding);
    ssize_t length;
    ssize_t cursor;
    std::vector<char> data;
};