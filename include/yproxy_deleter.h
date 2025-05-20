#pragma once

#include "yproxy_connector.h"

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

  /*
   * For segments execution, claanup garbage workhorse
   */
  explicit YProxyDeleter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                         bool confirm_, bool crazy_drop_);

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
  bool crazy_drop_{false};
};
