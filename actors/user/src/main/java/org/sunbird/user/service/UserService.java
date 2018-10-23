package org.sunbird.user.service;

import java.util.Map;
import org.sunbird.common.request.Request;
import org.sunbird.models.user.User;

public interface UserService {

  public Map<String, Object> getUserByUserIdAndOrgId(String userId, String orgId);

  public User validateUserIdAndGetUserIfPresent(String userId);

  void validateUserId(Request request);
}
