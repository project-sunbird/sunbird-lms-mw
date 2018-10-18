package org.sunbird.user.dao.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.dao.UserExternalIdentityDao;

public class UserExternalIdentityDaoImpl implements UserExternalIdentityDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);

  public String getUserIdFromExtIdAndProvider(Request reqObj) {
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
  private Map<String, Object> getUserFromExternalId(String extId, String provider, String idType) {
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

  private String getEncryptedData(String value) {
    try {
      return encryptionService.encryptData(value);
    } catch (Exception e) {
      throw new ProjectCommonException(
          ResponseCode.userDataEncryptionError.getErrorCode(),
          ResponseCode.userDataEncryptionError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }
}
