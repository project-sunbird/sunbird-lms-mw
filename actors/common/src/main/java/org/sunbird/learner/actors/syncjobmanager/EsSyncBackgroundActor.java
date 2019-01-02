package org.sunbird.learner.actors.syncjobmanager;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

/** Background sync of data between Cassandra and Elastic Search. */
@ActorConfig(
  tasks = {},
  asyncTasks = {"backgroundSync", "backgroundEncryption", "backgroundDecryption"}
)
public class EsSyncBackgroundActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "backgroundSync":
        sync(request);
        break;
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

  @SuppressWarnings("unchecked")
  private void sync(Request message) {
    ProjectLogger.log("EsSyncBackgroundActor: sync called", LoggerEnum.INFO);

    long startTime = System.currentTimeMillis();
    Map<String, Object> req = message.getRequest();
    Map<String, Object> responseMap = new HashMap<>();
    List<Map<String, Object>> reponseList = null;
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> dataMap = (Map<String, Object>) req.get(JsonKey.DATA);

    String objectType = (String) dataMap.get(JsonKey.OBJECT_TYPE);
    List<Object> objectIds = null;
    if (dataMap.containsKey(JsonKey.OBJECT_IDS) && null != dataMap.get(JsonKey.OBJECT_IDS)) {
      objectIds = (List<Object>) dataMap.get(JsonKey.OBJECT_IDS);
    }

    Util.DbInfo dbInfo = getDbInfoObj(objectType);
    if (null == dbInfo) {
      throw new ProjectCommonException(
          ResponseCode.invalidObjectType.getErrorCode(),
          ResponseCode.invalidObjectType.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    String requestLogMsg = "";

    if (null != objectIds && !objectIds.isEmpty()) {
      requestLogMsg =
          MessageFormat.format(
              "type = {0} and IDs = {1}", objectType, Arrays.toString(objectIds.toArray()));

      ProjectLogger.log(
          "EsSyncBackgroundActor:sync: Fetching data for " + requestLogMsg + " started",
          LoggerEnum.INFO);
      Response response =
          cassandraOperation.getRecordsByProperty(
              dbInfo.getKeySpace(), dbInfo.getTableName(), JsonKey.ID, objectIds);
      reponseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      ProjectLogger.log(
          "EsSyncBackgroundActor:sync: Fetching data for " + requestLogMsg + " completed",
          LoggerEnum.INFO);
    }
    if (null != reponseList && !reponseList.isEmpty()) {
      for (Map<String, Object> map : reponseList) {
        responseMap.put((String) map.get(JsonKey.ID), map);
      }
    } else {
      if (objectIds.size() > 0) {
        ProjectLogger.log(
            "EsSyncBackgroundActor:sync: Skip sync for "
                + requestLogMsg
                + " as all IDs are invalid",
            LoggerEnum.ERROR);
        return;
      }

      ProjectLogger.log(
          "EsSyncBackgroundActor:sync: Sync all data for type = "
              + objectType
              + " as no IDs provided",
          LoggerEnum.INFO);

      Response response =
          cassandraOperation.getAllRecords(dbInfo.getKeySpace(), dbInfo.getTableName());
      reponseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

      ProjectLogger.log(
          "EsSyncBackgroundActor:sync: Fetching all data for type = " + objectType + " completed",
          LoggerEnum.INFO);

      ProjectLogger.log(
          "EsSyncBackgroundActor:sync: Number of entries to sync for type = "
              + objectType
              + " is "
              + reponseList.size(),
          LoggerEnum.INFO);

      if (null != reponseList) {
        for (Map<String, Object> map : reponseList) {
          responseMap.put((String) map.get(JsonKey.ID), map);
        }
      }
    }

    Iterator<Entry<String, Object>> itr = responseMap.entrySet().iterator();
    while (itr.hasNext()) {
      if (objectType.equals(JsonKey.USER)) {
        Entry<String, Object> entry = itr.next();
        Map<String, Object> userMap = (Map<String, Object>) entry.getValue();
        Boolean isDeleted = false;
        if (null != userMap.get(JsonKey.IS_DELETED)) {
          isDeleted = (Boolean) userMap.get(JsonKey.IS_DELETED);
        }
        if (!isDeleted) {
          result.add(getUserDetails(entry));
        }
      } else if (objectType.equals(JsonKey.ORGANISATION)) {
        result.add(getOrgDetails(itr.next()));
      } else if (objectType.equals(JsonKey.BATCH) || objectType.equals(JsonKey.USER_COURSE)) {
        result.add((Map<String, Object>) (itr.next().getValue()));
      }
    }

    ElasticSearchUtil.bulkInsertData(
        ProjectUtil.EsIndex.sunbird.getIndexName(), getType(objectType), result);
    long stopTime = System.currentTimeMillis();
    long elapsedTime = stopTime - startTime;

    ProjectLogger.log(
        "EsSyncBackgroundActor:sync: Total time taken to sync for type = "
            + objectType
            + " is "
            + elapsedTime
            + " ms",
        LoggerEnum.INFO);
  }

  private String getType(String objectType) {
    String type = "";
    if (objectType.equals(JsonKey.USER)) {
      type = ProjectUtil.EsType.user.getTypeName();
    } else if (objectType.equals(JsonKey.ORGANISATION)) {
      type = ProjectUtil.EsType.organisation.getTypeName();
    } else if (objectType.equals(JsonKey.BATCH)) {
      type = ProjectUtil.EsType.course.getTypeName();
    } else if (objectType.equals(JsonKey.USER_COURSE)) {
      type = ProjectUtil.EsType.usercourses.getTypeName();
    }
    return type;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getOrgDetails(Entry<String, Object> entry) {
    ProjectLogger.log("EsSyncBackgroundActor: getOrgDetails called", LoggerEnum.INFO);
    Map<String, Object> orgMap = (Map<String, Object>) entry.getValue();
    orgMap.remove(JsonKey.ORG_TYPE);
    if (orgMap.containsKey(JsonKey.ADDRESS_ID)
        && !StringUtils.isBlank((String) orgMap.get(JsonKey.ADDRESS_ID))) {
      orgMap.put(
          JsonKey.ADDRESS,
          getDetailsById(
              Util.dbInfoMap.get(JsonKey.ADDRESS_DB), (String) orgMap.get(JsonKey.ADDRESS_ID)));
    }
    ProjectLogger.log("EsSyncBackgroundActor: getOrgDetails returned", LoggerEnum.INFO);
    return orgMap;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getUserDetails(Entry<String, Object> entry) {
    ProjectLogger.log("EsSyncBackgroundActor: getUserDetails called", LoggerEnum.INFO);

    String userId = entry.getKey();
    Map<String, Object> userMap = (Map<String, Object>) entry.getValue();
    Util.removeAttributes(userMap, Arrays.asList(JsonKey.PASSWORD, JsonKey.UPDATED_BY));
    if (StringUtils.isBlank((String) userMap.get(JsonKey.COUNTRY_CODE))) {
      userMap.put(
          JsonKey.COUNTRY_CODE,
          PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_DEFAULT_COUNTRY_CODE));
    }
    userMap.put(JsonKey.ADDRESS, Util.getAddressDetails(userId, null));
    userMap.put(JsonKey.EDUCATION, Util.getUserEducationDetails(userId));
    userMap.put(JsonKey.JOB_PROFILE, Util.getJobProfileDetails(userId));
    userMap.put(JsonKey.ORGANISATIONS, Util.getUserOrgDetails(userId));
    userMap.put(BadgingJsonKey.BADGE_ASSERTIONS, Util.getUserBadge(userId));

    // save masked email and phone number
    Util.addMaskEmailAndPhone(userMap);

    // add the skills column into ES
    Util.getUserSkills(userId);

    // compute profile completeness and error field.
    Util.checkProfileCompleteness(userMap);

    // handle user profile visibility
    Util.checkUserProfileVisibility(
        userMap, getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()));
    userMap = Util.getUserDetailsFromRegistry(userMap);

    ProjectLogger.log("EsSyncBackgroundActor: getUserDetails returned", LoggerEnum.INFO);
    return userMap;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getDetailsById(DbInfo dbInfo, String userId) {
    try {
      Response response =
          cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(), userId);
      return ((((List<Map<String, Object>>) response.get(JsonKey.RESPONSE)).isEmpty())
          ? new HashMap<>()
          : ((List<Map<String, Object>>) response.get(JsonKey.RESPONSE)).get(0));
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
    return null;
  }

  private DbInfo getDbInfoObj(String objectType) {
    if (objectType.equals(JsonKey.USER)) {
      return Util.dbInfoMap.get(JsonKey.USER_DB);
    } else if (objectType.equals(JsonKey.ORGANISATION)) {
      return Util.dbInfoMap.get(JsonKey.ORG_DB);
    } else if (objectType.equals(JsonKey.BATCH)) {
      return Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    } else if (objectType.equals(JsonKey.USER_COURSE)) {
      return Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    }

    return null;
  }

  private void backgroundEncrypt(Request request) {
    List<Map<String, Object>> userDetails = getUserDetails(request);
    int i = 0;
    for (Map<String, Object> userMap : userDetails) {
      if (ProjectUtil.isEmailvalid((String) userMap.get(JsonKey.EMAIL))
          || ((String) userMap.get(JsonKey.PHONE)).length() < 14) {
        encryptUserDataAndUpdateDb(userMap);
        i++;
      }
    }
    ProjectLogger.log(
        "EsSyncBackgroundActor:backgroundEncrypt: total number of user details encrypted is: " + i,
        LoggerEnum.INFO);
    syncToES(request);
  }

  private void backgroundDecrypt(Request request) {
    List<Map<String, Object>> userDetails = getUserDetails(request);
    int i = 0;
    if (userDetails != null) {
      for (Map<String, Object> userMap : userDetails) {
        if (!ProjectUtil.isEmailvalid((String) userMap.get(JsonKey.EMAIL))
            || !(((String) userMap.get(JsonKey.PHONE)).length() < 14)) {
          decryptUserDataAndUpdateDb(userMap);
          i++;
        }
      }
    }
    ProjectLogger.log(
        "EsSyncBackgroundActor:backgroundDecrypt: total number of user details encrypted is: " + i,
        LoggerEnum.INFO);
    syncToES(request);
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getUserDetails(Request request) {
    List<String> userIds = (List<String>) request.getRequest().get(JsonKey.USER_IDs);
    Response response =
        cassandraOperation.getRecordsByIdsWithSpecifiedColumns(
            JsonKey.SUNBIRD, JsonKey.USER, null, userIds);
    List<Map<String, Object>> userList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userList)) return null;
    return userList;
  }

  private void decryptUserDataAndUpdateDb(Map<String, Object> userMap) {
    try {
      UserUtility.decryptUserData(userMap);
      cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userMap);
      getUserAddressDataDecryptAndUpdateDb((String) userMap.get(JsonKey.ID));
    } catch (Exception e) {
      ProjectLogger.log(
          "Exception Occurred while decrypting user data for userId "
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
          "Exception Occurred while decrypting user address data for userId " + (userId), e);
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

  @SuppressWarnings("unchecked")
  private void syncToES(Request request) {

    Request backgroundSyncRequest = new Request();
    backgroundSyncRequest.setOperation(ActorOperations.BACKGROUND_SYNC.getValue());
    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    requestMap.put(JsonKey.OBJECT_IDS, (List<String>) request.get(JsonKey.USER_IDs));
    backgroundSyncRequest.getRequest().put(JsonKey.DATA, requestMap);

    try {
      tellToAnother(backgroundSyncRequest);
    } catch (Exception e) {
      ProjectLogger.log(
          "EsSyncActor:triggerBackgroundSync: Exception occurred with error message = "
              + e.getMessage(),
          e);
    }
  }
}
