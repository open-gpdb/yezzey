#pragma once

#include "yproxy_connector.h"

/* Collect and delete obsolete files */
class YProxyDeleterV2 : YProxyConnector {
public:
  /*
   * Direct delete dispatch, appliable from MASTER
   */
  explicit YProxyDeleterV2(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           std::string dbname);
  /*
   * For segments execution, claanup garbage workhorse
   */
  explicit YProxyDeleterV2(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           std::string dbname, bool crazy_drop);

  virtual ~YProxyDeleterV2();

  virtual bool Collect(const std::string &chunkName);
  virtual bool Delete(const std::string &chunkName);
  virtual bool close();

protected:
  /* prepare connection for chunk reading */
  std::vector<char> ConstructDeleteRequest(const std::string &fileName);
  std::vector<char> ConstructCollectRequest(const std::string &fileName);

  virtual int prepareYproxyConnection();

private:
  bool crazy_drop_{false};
  std::string dbname_;
};