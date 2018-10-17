package org.sunbird.user.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.impl.DefaultDecryptionServiceImpl;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;
import org.sunbird.user.dao.UserDao;
import org.sunbird.user.dao.impl.UserDaoImpl;
import org.sunbird.user.service.UserService;

public class UserServiceImpl implements UserService {

  private static DecryptionService decryptionService = new DefaultDecryptionServiceImpl();
  private static UserDao userDao = UserDaoImpl.getInstance();
  private static UserService userService = null;

  public static UserService getInstance() {
    if (userService == null) {
      userService = new UserServiceImpl();
    }
    return userService;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> getUserByUserIdAndOrgId(String userId, String orgId) {
    Map<String, Object> filters = new HashMap<>();
    filters.put(StringFormatter.joinByDot(JsonKey.ORGANISATIONS, JsonKey.ORGANISATION_ID), orgId);
    filters.put(StringFormatter.joinByDot(JsonKey.ORGANISATIONS, JsonKey.USER_ID), userId);
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.FILTERS, filters);
    SearchDTO searchDto = Util.createSearchDto(map);
    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDto,
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName());
    List<Map<String, Object>> userMapList = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
    if (CollectionUtils.isNotEmpty(userMapList)) {
      Map<String, Object> userMap = userMapList.get(0);
      return decryptionService.decryptData(userMap);
    } else {
      return Collections.EMPTY_MAP;
    }
  }

  @Override
  public User validateUserId(String userId) {
    User user = userDao.getUserById(userId);
    if (null == user) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    return user;
  }
}
