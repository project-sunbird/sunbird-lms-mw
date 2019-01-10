package org.sunbird.user.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.user.service.UserEncryptionService;
import org.sunbird.user.service.impl.UserEncryptionServiceImpl;

/** Background encrytion and decryption of user sensitive data. */
@ActorConfig(
  tasks = {},
  asyncTasks = {"backgroundEncryption", "backgroundDecryption"}
)
public class BackgroundUserDataEncryptionActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private UserEncryptionService userEncryptionService = UserEncryptionServiceImpl.getInstance();
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
    encryptData(userDetails);
  }

  private void backgroundDecrypt(Request request) {
    List<Map<String, Object>> userDetails = getUserDetails(request);
    decryptData(userDetails);
  }

  private void encryptData(List<Map<String, Object>> userDetails) {
    List<String> userIdsListToSync = new ArrayList<>();
    for (Map<String, Object> userMap : userDetails) {
      List<String> fieldsToEncrypt = userEncryptionService.getDecryptedFields(userMap);
      if (CollectionUtils.isNotEmpty(fieldsToEncrypt)) {
        encryptUserDataAndUpdateDb(userMap, fieldsToEncrypt);
        userIdsListToSync.add((String) userMap.get(JsonKey.ID));
      } else {
        ProjectLogger.log(
            "EsSyncBackgroundActor:encryptData: Cannot encrypt data for userId "
                + (String) userMap.get(JsonKey.ID),
            LoggerEnum.INFO);
      }
    }
    ProjectLogger.log(
        "EsSyncBackgroundActor:encryptData: total number of user details encrypted is: "
            + userIdsListToSync.size(),
        LoggerEnum.INFO);
    if (CollectionUtils.isNotEmpty(userIdsListToSync)) {
      syncToES(userIdsListToSync);
    }
  }

  private void decryptData(List<Map<String, Object>> userDetails) {
    List<String> userIdsListToSync = new ArrayList<>();
    for (Map<String, Object> userMap : userDetails) {
      List<String> fieldsToDecrypt = userEncryptionService.getEncryptedFields(userMap);
      if (CollectionUtils.isNotEmpty(fieldsToDecrypt)) {
        decryptUserDataAndUpdateDb(userMap);
        userIdsListToSync.add((String) userMap.get(JsonKey.ID));
      } else {
        ProjectLogger.log(
            "EsSyncBackgroundActor:decryptData: Cannot decrypt data for userId "
                + (String) userMap.get(JsonKey.ID),
            LoggerEnum.INFO);
      }
    }
    ProjectLogger.log(
        "EsSyncBackgroundActor:decryptData: total number of user details decrypted is: "
            + userIdsListToSync.size(),
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
    return userList;
  }

  private void encryptUserDataAndUpdateDb(
      Map<String, Object> userMap, List<String> fieldsToEncrypt) {
    try {
      UserUtility.encryptUserSpecificField(userMap, fieldsToEncrypt);
      cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userMap);
      ProjectLogger.log(
          "DataSecurityBackgroundActor:encryptUserDataAndUpdateDb: Updating user data for userId "
              + ((String) userMap.get(JsonKey.ID))
              + " is completed",
          LoggerEnum.INFO);

      List<Map<String, Object>> addressList = getAddressList((String) userMap.get(JsonKey.ID));
      if (CollectionUtils.isNotEmpty(addressList)) {
        UserUtility.encryptUserAddressData(addressList);
        updateAddressList(addressList);
        ProjectLogger.log(
            "DataSecurityBackgroundActor:encryptUserDataAndUpdateDb: Updating user address data for userId "
                + ((String) userMap.get(JsonKey.ID))
                + " is completed",
            LoggerEnum.INFO);
      }
    } catch (Exception e) {
      ProjectLogger.log(
          "DataSecurityBackgroundActor:encryptUserDataAndUpdateDb: Exception Occurred while encrypting user data ",
          e);
    }
  }

  private void decryptUserDataAndUpdateDb(Map<String, Object> userMap) {
    UserUtility.decryptUserData(userMap);
    cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userMap);
    ProjectLogger.log(
        "DataSecurityBackgroundActor:decryptUserDataAndUpdateDb: Updating user data for userId "
            + ((String) userMap.get(JsonKey.ID))
            + " is completed",
        LoggerEnum.INFO);

    List<Map<String, Object>> addressList = getAddressList((String) userMap.get(JsonKey.ID));
    if (CollectionUtils.isNotEmpty(addressList)) {
      UserUtility.decryptUserAddressData(addressList);
      updateAddressList(addressList);
      ProjectLogger.log(
          "DataSecurityBackgroundActor:decryptUserDataAndUpdateDb: Updating user address data for userId "
              + ((String) userMap.get(JsonKey.ID))
              + " is completed",
          LoggerEnum.INFO);
    }
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getAddressList(String userId) {
    Response response =
        cassandraOperation.getRecordsByProperty(
            addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), JsonKey.USER_ID, userId);
    return (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
  }

  private void updateAddressList(List<Map<String, Object>> addressList) {
    for (Map<String, Object> address : addressList) {
      cassandraOperation.updateRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
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
