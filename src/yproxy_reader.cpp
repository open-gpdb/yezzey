#include "yproxy_reader.h"

const int kDefaultRetryLimit = 100;

YProxyReader::YProxyReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           const std::vector<ChunkInfo> order)
    : YProxyConnector(adv, segindx), order_ptr_(0), order_(order),
      current_chunk_remaining_bytes_(0), current_retry(0),
      retry_limit(kDefaultRetryLimit) {}

YProxyReader::~YProxyReader() { close(); }

bool YProxyReader::close() { return YProxyConnector::close(); }

std::vector<char> YProxyReader::ConstructCatRequest(const ChunkInfo &ci,
                                                       size_t start_off) {

  uint64_t settingsCnt = 1;
  uint64_t settingsMsgSpace = 0;

  std::vector<std::pair<std::string, std::string>> settings = {
      {"TableSpace", adv_->tableSpace},
  };

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    settingsMsgSpace += settings[j].first.size() + 1;
    settingsMsgSpace += settings[j].second.size() + 1;
  }

  std::vector<char> buff(MSG_HEADER_SIZE + PROTO_HEADER_SIZE +
                             ci.x_path.size() + 1 + OFFSET_SZ +
                             MSG_HEADER_SIZE + settingsMsgSpace,
                         0);
  buff[8] = MessageTypeCatV2;
  if (ci.enc) {
    buff[9] = DecryptRequest;
  } else {
    buff[9] = NoDecryptRequest;
  }

  if (start_off != 0) {
    buff[10] = ExtendedMessage;
  }
  uint64_t len = buff.size();

  strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, ci.x_path.c_str(),
          ci.x_path.size());

  uint64_t cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  cp = start_off;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[MSG_HEADER_SIZE + PROTO_HEADER_SIZE + ci.x_path.size() + 1 + i] =
        cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  uint64_t settings_offset =
      MSG_HEADER_SIZE + PROTO_HEADER_SIZE + ci.x_path.size() + 1 + OFFSET_SZ;

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

int YProxyReader::prepareYproxyConnection(const ChunkInfo &ci,
                                          size_t start_off) {
  int rb = YProxyConnector::prepareYproxyConnection();
  if (rb != 0) {
    return rb;
  }

  auto msg = ConstructCatRequest(ci, start_off);

  size_t rc = ::write(client_fd_, msg.data(), msg.size());

  if (rc <= 0) {
    // handle
    return -1;
  }

  /* reset retry count */
  this->current_retry = 0;

  // now we are ready to read our request data
  return client_fd_;
}

bool YProxyReader::read(char *buffer, size_t *amount) {
  // preparing done, read data

  while (1) {
    if (current_chunk_remaining_bytes_ == 0) {
      // no more data to read
      if (order_ptr_ == order_.size()) {
        *amount = 0;
        return false;
      }

      // close previous read socket, if any
      if (!this->close()) {
        // wtf?
        return false;
      }
      auto rc = this->prepareYproxyConnection(order_[order_ptr_], 0);
      if (rc < 0) {
        continue;
      }
      current_chunk_offset_ = 0;
      current_chunk_remaining_bytes_ = order_[order_ptr_].size;
    }

    auto rc = ::read(client_fd_, buffer, *amount);
    if (rc <= 0) {
      elog(WARNING, "reacquiring connection on offset %lu",
           current_chunk_offset_);

      if (++this->current_retry < this->retry_limit) {
        auto rrc = this->prepareYproxyConnection(order_[order_ptr_],
                                                 current_chunk_offset_);
        if (rrc < 0) {
          sleep(1);
          continue;
        }
      } else {
        // error, and we are out of retries.
        *amount = rc;
        return false;
      }
      continue;
    }
    // what if rc > current_chunk_remaining_bytes_ ?
    current_chunk_remaining_bytes_ -= rc;
    current_chunk_offset_ += rc;
    if (current_chunk_remaining_bytes_ == 0) {
      ++order_ptr_;
    }
    *amount = rc;

    return true;
  }
}

bool YProxyReader::empty() {
  return order_ptr_ == order_.size() && current_chunk_remaining_bytes_ <= 0;
};
