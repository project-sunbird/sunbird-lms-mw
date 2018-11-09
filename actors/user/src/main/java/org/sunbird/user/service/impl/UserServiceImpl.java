package org.sunbird.user.service.impl;

import akka.actor.ActorRef;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.impl.DefaultDecryptionServiceImpl;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;
import org.sunbird.models.user.User;
import org.sunbird.user.dao.UserDao;
import org.sunbird.user.dao.UserExternalIdentityDao;
import org.sunbird.user.dao.impl.UserDaoImpl;
import org.sunbird.user.dao.impl.UserExternalIdentityDaoImpl;
import org.sunbird.user.service.UserService;

public class UserServiceImpl implements UserService {

  private static DecryptionService decryptionService = new DefaultDecryptionServiceImpl();
  private static UserDao userDao = UserDaoImpl.getInstance();
  private static UserService userService = null;
  private UserExternalIdentityDao userExtIdentityDao = new UserExternalIdentityDaoImpl();

  public static UserService getInstance() {
    if (userService == null) {
      userService = new UserServiceImpl();
    }
    return userService;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> esGetUserOrg(String userId, String orgId) {
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
  public User getUserById(String userId) {
    User user = userDao.getUserById(userId);
    if (null == user) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    return user;
  }

  @Override
  public void validateUserId(Request request) {
    String ctxtUserId = (String) request.getContext().get(JsonKey.USER_ID);
    String userId = userExtIdentityDao.getUserId(request);

    if ((!StringUtils.isBlank(userId)) && (!userId.equals(ctxtUserId))) {
      throw new ProjectCommonException(
          ResponseCode.unAuthorized.getErrorCode(),
          ResponseCode.unAuthorized.getErrorMessage(),
          ResponseCode.UNAUTHORIZED.getResponseCode());
    }
  }

  @Override
  public void syncUserProfile(
      String userId, Map<String, Object> userDataMap, Map<String, Object> userPrivateDataMap) {
    ElasticSearchUtil.createData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.userprofilevisibility.getTypeName(),
        userId,
        userPrivateDataMap);
    ElasticSearchUtil.createData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        userId,
        userDataMap);
  }

  @Override
  public Map<String, Object> esGetPublicUserProfileById(String userId) {
    Map<String, Object> esResult =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId);
    if (esResult == null || esResult.size() == 0) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    return esResult;
  }

  @Override
  public Map<String, Object> esGetPrivateUserProfileById(String userId) {
    return ElasticSearchUtil.getDataByIdentifier(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.userprofilevisibility.getTypeName(),
        userId);
  }

  @Override
  public String getActiveCustodianOrgId(Map<String, Object> userMap, ActorRef actorRef) {
    String custodianOrgId = "";
    try {
      SystemSettingClient client = SystemSettingClientImpl.getInstance();
      SystemSetting systemSetting =
          client.getSystemSettingByField(actorRef, JsonKey.CUSTODIAN_ORG_ID);
      if (null != systemSetting && StringUtils.isNotBlank(systemSetting.getValue())) {
        custodianOrgId = systemSetting.getValue();
      }
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserUtil:getActiveCustodianOrgId: Exception occurred while fetching custodian org ID from system setting.",
          ex);
      ProjectCommonException.throwServerErrorException(
          ResponseCode.errorSystemSettingNotFound,
          ProjectUtil.formatMessage(
              ResponseCode.errorSystemSettingNotFound.getErrorMessage(), JsonKey.CUSTODIAN_ORG_ID));
    }
    Map<String, Object> custodianOrg = null;
    if (StringUtils.isNotBlank(custodianOrgId)) {
      custodianOrg =
          ElasticSearchUtil.getDataByIdentifier(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName(),
              custodianOrgId);
      if (MapUtils.isNotEmpty(custodianOrg)) {

        if (null != custodianOrg.get(JsonKey.STATUS)) {
          int status = (int) custodianOrg.get(JsonKey.STATUS);
          if (1 != status) {
            ProjectCommonException.throwClientErrorException(
                ResponseCode.errorInactiveCustodianOrg);
          }
        } else {
          ProjectCommonException.throwClientErrorException(ResponseCode.errorInactiveCustodianOrg);
        }
      } else {
        ProjectCommonException.throwServerErrorException(
            ResponseCode.errorSystemSettingNotFound,
            ProjectUtil.formatMessage(
                ResponseCode.errorSystemSettingNotFound.getErrorMessage(),
                JsonKey.CUSTODIAN_ORG_ID));
      }
    } else {
      ProjectCommonException.throwServerErrorException(
          ResponseCode.errorSystemSettingNotFound,
          ProjectUtil.formatMessage(
              ResponseCode.errorSystemSettingNotFound.getErrorMessage(), JsonKey.CUSTODIAN_ORG_ID));
    }
    userMap.put(JsonKey.ROOT_ORG_ID, custodianOrgId);
    userMap.put(JsonKey.CHANNEL, custodianOrg.get(JsonKey.CHANNEL));
    return custodianOrgId;
  }
}
