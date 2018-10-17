package org.sunbird.user.service;

import java.util.Map;
import org.sunbird.models.user.User;

public interface UserService {

  public Map<String, Object> getUserByUserIdAndOrgId(String userId, String orgId);

  public User validateUserId(String userId);
}
