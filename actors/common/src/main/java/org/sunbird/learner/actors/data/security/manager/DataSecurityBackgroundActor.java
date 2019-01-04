package org.sunbird.learner.actors.data.security.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;

/** Background sync of data between Cassandra and Elastic Search. */
@ActorConfig(
  tasks = {},
  asyncTasks = {"backgroundEncryption", "backgroundDecryption"}
)
public class DataSecurityBackgroundActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "backgroundEncryption":
        backgroundEncrypt(request);
        break;
      case "backgroundDecryption":
        backgroundDecrypt(request);
        break;
      default:
        onReceiveUnsupportedOperation("EsSyncBackgroundActor");
        break;
    }
  }

  private void backgroundEncrypt(Request request) {
    List<Map<String, Object>> userDetails = getUserDetails(request);
    commonEncryptionDecryption(userDetails, true);
  }

  private void backgroundDecrypt(Request request) {
    List<Map<String, Object>> userDetails = getUserDetails(request);
    commonEncryptionDecryption(userDetails, false);
  }

  private void commonEncryptionDecryption(
      List<Map<String, Object>> userDetails, boolean isEncrypted) {
    List<String> userIdsListToSync = new ArrayList<>();
    int i = 0;
    int maxPhoneNoSizeAllowed =
        Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_USER_MAX_PHONE_LENGTH).trim());
    for (Map<String, Object> userMap : userDetails) {
      if (isEncrypted) {
        if (ProjectUtil.isEmailvalid((String) userMap.get(JsonKey.EMAIL))
            || (StringUtils.isNotBlank((String) userMap.get(JsonKey.PHONE))
                && ((String) userMap.get(JsonKey.PHONE)).length() <= maxPhoneNoSizeAllowed)) {
          encryptUserDataAndUpdateDb(userMap);
          userIdsListToSync.add((String) userMap.get(JsonKey.ID));
          i++;
        }
      } else {
        if ((StringUtils.isNotBlank((String) userMap.get(JsonKey.EMAIL))
                && !(ProjectUtil.isEmailvalid((String) userMap.get(JsonKey.EMAIL)))
            || (StringUtils.isNotBlank((String) userMap.get(JsonKey.PHONE))
                && ((String) userMap.get(JsonKey.PHONE)).length() > maxPhoneNoSizeAllowed))) {
          decryptUserDataAndUpdateDb(userMap);
          userIdsListToSync.add((String) userMap.get(JsonKey.ID));
          i++;
        }
      }
    }
    ProjectLogger.log(
        "EsSyncBackgroundActor:commonEncryptionDecryption: total number of user details"
            + (isEncrypted ? " encrypted " : " decrypted ")
            + "is: "
            + i,
        LoggerEnum.INFO);
    if (CollectionUtils.isNotEmpty(userIdsListToSync)) {
      syncToES(userIdsListToSync);
    }
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getUserDetails(Request request) {
    List<String> userIds = (List<String>) request.getRequest().get(JsonKey.USER_IDs);
    Response response =
        cassandraOperation.getRecordsByIdsWithSpecifiedColumns(
            JsonKey.SUNBIRD, JsonKey.USER, null, userIds);
    List<Map<String, Object>> userList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userList)) {
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidUserId);
    }
    ;
    return userList;
  }

  private void decryptUserDataAndUpdateDb(Map<String, Object> userMap) {
    try {
      UserUtility.decryptUserData(userMap);
      cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userMap);
      getUserAddressDataDecryptAndUpdateDb((String) userMap.get(JsonKey.ID));
    } catch (Exception e) {
      ProjectLogger.log(
          "DataSecurityBackgroundActor:decryptUserDataAndUpdateDb: Exception Occurred while decrypting user data for userId "
              + ((String) userMap.get(JsonKey.ID)),
          e);
    }
  }

  @SuppressWarnings("unchecked")
  private void getUserAddressDataDecryptAndUpdateDb(String userId) {
    Response response =
        cassandraOperation.getRecordsByProperty(
            addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), JsonKey.USER_ID, userId);
    List<Map<String, Object>> addressList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    try {
      UserUtility.decryptUserAddressData(addressList);
      for (Map<String, Object> address : addressList) {
        cassandraOperation.updateRecord(
            addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
      }
    } catch (Exception e) {
      ProjectLogger.log(
          "DataSecurityBackgroundActor:getUserAddressDataDecryptAndUpdateDb: Exception Occurred while decrypting user address data for userId "
              + (userId),
          e);
    }
  }

  private void encryptUserDataAndUpdateDb(Map<String, Object> userMap) {
    try {
      UserUtility.encryptUserData(userMap);
      cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userMap);
      getUserAddressDataEncryptAndUpdateDb((String) userMap.get(JsonKey.ID));
    } catch (Exception e) {
      ProjectLogger.log("Exception Occurred while encrypting user data ", e);
    }
  }

  @SuppressWarnings("unchecked")
  private void getUserAddressDataEncryptAndUpdateDb(String userId) {
    Response response =
        cassandraOperation.getRecordsByProperty(
            addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), JsonKey.USER_ID, userId);
    List<Map<String, Object>> addressList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    try {
      UserUtility.encryptUserAddressData(addressList);
      for (Map<String, Object> address : addressList) {
        cassandraOperation.updateRecord(
            addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
      }
    } catch (Exception e) {
      ProjectLogger.log("Exception Occurred while encrypting user data ", e);
    }
  }

  private void syncToES(List<String> userIds) {

    Request backgroundSyncRequest = new Request();
    backgroundSyncRequest.setOperation(ActorOperations.BACKGROUND_SYNC.getValue());
    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    requestMap.put(JsonKey.OBJECT_IDS, userIds);
    backgroundSyncRequest.getRequest().put(JsonKey.DATA, requestMap);

    tellToAnother(backgroundSyncRequest);
  }
}
