package org.sunbird.user.actors.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

public class AuthenticationHelper {

  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);

  public static void validateWithUserId(Request reqObj) {
    if (null != reqObj.getContext().get(JsonKey.IS_AUTH_REQ)
        && Boolean.parseBoolean((String) reqObj.getContext().get(JsonKey.IS_AUTH_REQ))) {
      String ctxtUserId = (String) reqObj.getContext().get(JsonKey.USER_ID);
      String userId = getUserIdFromExtIdAndProvider(reqObj);
      if ((!StringUtils.isBlank(userId)) && (!userId.equals(ctxtUserId))) {
        throw new ProjectCommonException(
            ResponseCode.unAuthorized.getErrorCode(),
            ResponseCode.unAuthorized.getErrorMessage(),
            ResponseCode.UNAUTHORIZED.getResponseCode());
      }
    }
  }

  private static String getUserIdFromExtIdAndProvider(Request reqObj) {
    String userId = "";
    if (null != reqObj.getRequest().get(JsonKey.USER_ID)) {
      userId = (String) reqObj.getRequest().get(JsonKey.USER_ID);
    } else {
      userId = (String) reqObj.getRequest().get(JsonKey.ID);
    }
    if (StringUtils.isBlank(userId)) {
      String extId = (String) reqObj.getRequest().get(JsonKey.EXTERNAL_ID);
      String provider = (String) reqObj.getRequest().get(JsonKey.EXTERNAL_ID_PROVIDER);
      String idType = (String) reqObj.getRequest().get(JsonKey.EXTERNAL_ID_TYPE);
      Map<String, Object> user = getUserFromExternalId(extId, provider, idType);

      if (user != null && !user.isEmpty()) {
        userId = (String) user.get(JsonKey.ID);
      } else {
        throw new ProjectCommonException(
            ResponseCode.invalidParameter.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.invalidParameter.getErrorMessage(),
                StringFormatter.joinByAnd(
                    StringFormatter.joinByComma(JsonKey.EXTERNAL_ID, JsonKey.EXTERNAL_ID_TYPE),
                    JsonKey.EXTERNAL_ID_PROVIDER)),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    return userId;
  }

  @SuppressWarnings({"unchecked"})
  public static Map<String, Object> getUserFromExternalId(
      String extId, String provider, String idType) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> user = null;
    Map<String, Object> externalIdReq = new HashMap<>();
    externalIdReq.put(JsonKey.PROVIDER, provider.toLowerCase());
    externalIdReq.put(JsonKey.ID_TYPE, idType.toLowerCase());
    externalIdReq.put(JsonKey.EXTERNAL_ID, getEncryptedData(extId.toLowerCase()));
    Response response =
        cassandraOperation.getRecordsByCompositeKey(
            usrDbInfo.getKeySpace(), JsonKey.USR_EXT_IDNT_TABLE, externalIdReq);

    List<Map<String, Object>> userRecordList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

    if (CollectionUtils.isNotEmpty(userRecordList)) {
      Map<String, Object> userExtIdRecord = userRecordList.get(0);
      Response res =
          cassandraOperation.getRecordById(
              usrDbInfo.getKeySpace(),
              usrDbInfo.getTableName(),
              (String) userExtIdRecord.get(JsonKey.USER_ID));
      if (CollectionUtils.isNotEmpty((List<Map<String, Object>>) res.get(JsonKey.RESPONSE))) {
        // user exist
        user = ((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0);
      }
    }
    return user;
  }

  public static String getEncryptedData(String value) {
    try {
      return encryptionService.encryptData(value);
    } catch (Exception e) {
      throw new ProjectCommonException(
          ResponseCode.userDataEncryptionError.getErrorCode(),
          ResponseCode.userDataEncryptionError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  public static Map<String, Object> getClientAccessTokenDetail(String clientId) {
    Util.DbInfo clientDbInfo = Util.dbInfoMap.get(JsonKey.CLIENT_INFO_DB);
    Map<String, Object> response = null;
    Map<String, Object> propertyMap = new HashMap<>();
    propertyMap.put(JsonKey.ID, clientId);
    try {
      Response clientResponse =
          cassandraOperation.getRecordById(
              clientDbInfo.getKeySpace(), clientDbInfo.getTableName(), clientId);
      if (null != clientResponse && !clientResponse.getResult().isEmpty()) {
        List<Map<String, Object>> dataList =
            (List<Map<String, Object>>) clientResponse.getResult().get(JsonKey.RESPONSE);
        response = dataList.get(0);
      }
    } catch (Exception e) {
      ProjectLogger.log("Validating client token failed due to : ", e);
    }
    return response;
  }

  public static Map<String, Object> getUserDetail(String userId) {
    Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> response = null;
    try {
      Response userResponse =
          cassandraOperation.getRecordById(
              userDbInfo.getKeySpace(), userDbInfo.getTableName(), userId);
      if (null != userResponse && !userResponse.getResult().isEmpty()) {
        List<Map<String, Object>> dataList =
            (List<Map<String, Object>>) userResponse.getResult().get(JsonKey.RESPONSE);
        response = dataList.get(0);
      }
    } catch (Exception e) {
      ProjectLogger.log("fetching user for id " + userId + " failed due to : ", e);
    }
    return response;
  }

  public static Map<String, Object> getOrgDetail(String orgId) {
    Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Map<String, Object> response = null;
    try {
      Response userResponse =
          cassandraOperation.getRecordById(
              userDbInfo.getKeySpace(), userDbInfo.getTableName(), orgId);
      if (null != userResponse && !userResponse.getResult().isEmpty()) {
        List<Map<String, Object>> dataList =
            (List<Map<String, Object>>) userResponse.getResult().get(JsonKey.RESPONSE);
        response = dataList.get(0);
      }
    } catch (Exception e) {
      ProjectLogger.log("fetching user for id " + orgId + " failed due to : ", e);
    }
    return response;
  }
}
