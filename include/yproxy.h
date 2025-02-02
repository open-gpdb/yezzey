
#pragma once

#include "chunkinfo.h"
#include "io_adv.h"
#include <memory>
#include <string>
#include <vector>

class YProxyConnector {
public:
  explicit YProxyConnector(std::shared_ptr<IOadv> adv, ssize_t segindx);
  virtual ~YProxyConnector();
  virtual bool close();

protected:
  virtual int prepareYproxyConnection();
  std::shared_ptr<IOadv> adv_{nullptr};
  ssize_t segindx_{0};

  int client_fd_{-1};
};

struct storageChunkMeta {
  int64_t chunkSize;
  std::string chunkName;
};

/* reader using yproxy */
class YProxyReader : YProxyConnector {
public:
  friend class ExternalWriter;
  explicit YProxyReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                        std::vector<ChunkInfo> order);
  ~YProxyReader();

public:
  virtual bool read(char *buffer, size_t *amount);

  virtual bool empty();

  virtual bool close();

protected:
  /* prepare connection for chunk reading */
  std::vector<char> ConstructCatRequest(const ChunkInfo &ci, size_t start_off);
  virtual int prepareYproxyConnection(const ChunkInfo &ci, size_t start_off);

private:
  uint64_t order_ptr_{0};
  const std::vector<ChunkInfo> order_;
  int64_t current_chunk_remaining_bytes_{0};
  int64_t current_chunk_offset_{0};

  int current_retry{0};
  int retry_limit{1};
};

// Write into external storage using yproxy
class YProxyWriter : YProxyConnector {
public:
  explicit YProxyWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                        ssize_t modcount, const std::string &storage_path);

  virtual ~YProxyWriter();

  virtual bool write(const char *buffer, size_t *amount);

  virtual bool close();

protected:
  /* prepare connection for chunk reading */
  std::vector<char> ConstructPutRequest(std::string fileName);
  std::vector<char> ConstructCopyDataRequest(const char *buffer, size_t amount);
  virtual int prepareYproxyConnection();

private:
  std::string createXPath();

  ssize_t modcount_;
  XLogRecPtr insertion_rec_ptr_;
  std::string storage_path_;

public:
  std::string getExternalStoragePath() { return storage_path_; }

  XLogRecPtr getInsertionStorageLsn() { return insertion_rec_ptr_; }
};

/* Delete specified file from external storage, bypassing all sanity checks */
class YProxyDeleter : YProxyConnector {
public:
  /*
   * Direct delete dispatch, appliable from MASTER
   */
  explicit YProxyDeleter(std::shared_ptr<IOadv> adv);
  /*
   * For segments execution, claanup garbage workhorse
   */
  explicit YProxyDeleter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                         bool confirm_);

  virtual ~YProxyDeleter();

  virtual bool deleteChunk(const std::string &chunkName);

  virtual bool close();

protected:
  /* prepare connection for chunk reading */
  std::vector<char> ConstructDeleteRequest(std::string fileName);
  virtual int prepareYproxyConnection();

private:
  bool garbage_cleanup_{false};
  bool confirm_{false};
};

// list external storage using yproxy
class YProxyLister : YProxyConnector {
public:
  explicit YProxyLister(std::shared_ptr<IOadv> adv, ssize_t segindx);

  virtual ~YProxyLister();

  virtual std::vector<storageChunkMeta> list_relation_chunks();
  virtual std::vector<std::string> list_chunk_names();

  virtual bool close();

protected:
  std::vector<char> ConstructListRequest(std::string fileName);
  virtual int prepareYproxyConnection();

  struct message {
    char type;
    std::vector<char> content;
    int retCode;
  };
  message readMessage();
  std::vector<storageChunkMeta> readObjectMetaBody(std::vector<char> *body);
};
