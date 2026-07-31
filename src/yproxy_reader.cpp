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
  std::vector<std::pair<std::string, std::string>> settings = {
      {"TableSpace", adv_->tableSpace},
  };

  MsgBuilder builder = MsgBuilder()
                           .fieldProto()
                           .fieldString(ci.x_path.size())
                           .fieldUInt64() // offset
                           .fieldUInt64();

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.fieldString(settings[j].first.size())
        .fieldString(settings[j].second.size());
  }
  builder.endDescription();

  builder
      .addProto(MessageTypeCatV2, ci.enc ? DecryptRequest : NoDecryptRequest,
                ci.kek ? UseKEK : NoUseKEK)
      .addString(ci.x_path)
      .addUInt64(start_off)
      .addUInt64(settingsCnt);

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.addString(settings[j].first).addString(settings[j].second);
  }

  return builder.get();
}

int YProxyReader::prepareYproxyConnection(const ChunkInfo &ci,
                                          size_t start_off) {
  int rb = YProxyConnector::prepareYproxyConnection();
  if (rb != 0) {
    return rb;
  }

  auto msg = ConstructCatRequest(ci, start_off);

  ssize_t rc = ::write(client_fd_, msg.data(), msg.size());

  if (rc <= 0) {
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
    CHECK_FOR_INTERRUPTS();
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

      ::close(client_fd_);
      client_fd_ = -1;

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
    if (current_chunk_remaining_bytes_ < rc) {
      ereport(ERROR, (errmsg_internal("yproxy returned too much data: received "
                                      "%ld while expected <= %ld",
                                      rc, current_chunk_remaining_bytes_)));
    }
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
