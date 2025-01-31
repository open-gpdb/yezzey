#include "msgproto.h"

const ssize_t CHAR_SZ = 1;
const ssize_t UINT64_SZ = 8;



MsgBuilder::MsgBuilder() : length(UINT64_SZ),cursor(0), data() {}

ssize_t MsgBuilder::putChar(char c,ssize_t padding){
    data.at(padding) = c;
    return CHAR_SZ;
}

ssize_t MsgBuilder::putUInt64(uint64_t val, ssize_t padding){
    uint64_t cp = val;
    for (ssize_t i = UINT64_SZ-1; i >= 0; --i) {
        data.at(i) = cp & ((1 << 8) - 1);
        cp >>= 8;
    }
    return UINT64_SZ;
}

ssize_t MsgBuilder::putString(std::string val,ssize_t padding){
    strncpy(&data[padding], val.c_str(),val.size());
    return val.size() + CHAR_SZ;
}

MsgBuilder& MsgBuilder::fieldMessageType(){
    length += UINT64_SZ;
}

MsgBuilder& MsgBuilder::fieldUInt64(){
    length += UINT64_SZ;
}

MsgBuilder& MsgBuilder::fieldString(ssize_t val){
    length += val + CHAR_SZ;
}

MsgBuilder& MsgBuilder::endDescription(){
    data = std::vector<char>(length);
    cursor += this->putUInt64(length,cursor);
}


MsgBuilder& MsgBuilder::addMessageType(char message_type){
    this->putChar(message_type,cursor);
    cursor += UINT64_SZ;
    return *this;
}
MsgBuilder& MsgBuilder::addUInt64(uint64_t val){
    cursor+= this->putUInt64(val,cursor);
}
MsgBuilder& MsgBuilder::addString(std::string str){
    cursor+= this->putString(str,cursor);
}
std::vector<char> MsgBuilder::get(){
    return data;
}