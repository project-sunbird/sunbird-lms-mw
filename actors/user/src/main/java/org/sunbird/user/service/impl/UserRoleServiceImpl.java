package org.sunbird.user.service.impl;

import java.util.List;
import java.util.Map;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.user.service.UserRoleService;

public class UserRoleServiceImpl implements UserRoleService {

  @Override
  public String validateRoles(List<String> roleList) {
    Map<String, Object> roleMap = DataCacheHandler.getRoleMap();
    if (null != roleMap && !roleMap.isEmpty()) {
      for (String role : roleList) {
        if (null == roleMap.get(role.trim())) {
          return role + " is not a valid role.";
        }
      }
    }
    return JsonKey.SUCCESS;
  }
}
