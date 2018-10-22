package org.sunbird.user.service;

import java.util.Map;
import org.sunbird.common.request.Request;
import org.sunbird.models.user.User;

public interface UserService {

  public Map<String, Object> getUserByUserIdAndOrgId(String userId, String orgId);

  public User validateUserIdAndGetUserIfPresent(String userId);

  public void generateTeleEventForUser(
      Map<String, Object> requestMap, String userId, String objectType);

  void validateUserId(Request request);
}
