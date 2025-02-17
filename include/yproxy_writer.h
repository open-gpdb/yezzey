#pragma once

#include "msgproto.h"
#include "yproxy_connector.h"
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

  int readPutCompleteResponce(int client_fd_);

  ssize_t modcount_;
  XLogRecPtr insertion_rec_ptr_;
  std::string storage_path_;
  uint16_t key_version;

public:
  std::string getExternalStoragePath() { return storage_path_; }

  XLogRecPtr getInsertionStorageLsn() { return insertion_rec_ptr_; }

  bool getUseKEK() { return key_version == 2; }
};
