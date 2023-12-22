
#pragma once

#include "io_adv.h"
#include "yreader.h"
#include <memory>
#include <string>
#include <vector>


/* reader using yproxy */
class YProxyReader : public YReader {
public:
  friend class ExternalWriter;
  explicit YProxyReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                        std::vector<ChunkInfo> order);
  ~YProxyReader();

public:
  virtual bool close();
  virtual bool read(char *buffer, size_t *amount);

  virtual bool empty();

public:

protected:
  /* prepare connection for chunk reading */ 
  int prepareYproxyConnection(const ChunkInfo & ci);
  std::vector<char> ConstructCatRequest(const ChunkInfo & ci);

private:
  std::shared_ptr<IOadv> adv_{nullptr};
  ssize_t segindx_{0};
  int64_t order_ptr_{0};
  int64_t current_chunk_remaining_bytes_{0};
  const std::vector<ChunkInfo> order_;

  int client_fd_{-1};
};

// write to external storage, using gpwriter.
// encrypt all data with gpg
class YProxyWriter : public YWriter {

public:
  explicit YProxyWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                      ssize_t modcount, const std::string &storage_path);

  virtual ~YProxyWriter();

  virtual bool close();

  virtual bool write(const char *buffer, size_t *amount);

protected:
  /* prepare connection for chunk reading */ 
  int prepareYproxyConnection();
  std::vector<char> ConstructPutRequest(const char *buffer, size_t amount);
  std::vector<char> CostructCommandCompleteRequest();
  int readRFQResponce();

private:
  std::string createXPath();


  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
  ssize_t modcount_;
  XLogRecPtr insertion_rec_ptr_;
  std::string storage_path_;

  int client_fd_{-1};

public:
  std::string getExternalStoragePath() { return storage_path_; }

  XLogRecPtr getInsertionStorageLsn() { return insertion_rec_ptr_; }
};