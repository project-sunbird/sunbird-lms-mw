package org.sunbird.user.dao;

import java.util.List;
import org.sunbird.common.models.response.Response;
import org.sunbird.models.user.org.UserOrg;

public interface UserOrgDao {
  Response updateUserOrg(UserOrg userOrg);

  Response createUserOrg(UserOrg userOrg);

  void deleteUserOrgs(List<String> ids);

  void updateUserOrgById(UserOrg userOrg);
}
