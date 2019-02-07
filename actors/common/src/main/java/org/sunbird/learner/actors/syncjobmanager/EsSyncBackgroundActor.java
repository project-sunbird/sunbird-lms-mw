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
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

/** Background sync of data between Cassandra and Elastic Search. */
@ActorConfig(
  tasks = {},
  asyncTasks = {"backgroundSync"}
)
public class EsSyncBackgroundActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();

    if (ActorOperations.BACKGROUND_SYNC.getValue().equalsIgnoreCase(operation)) {
      sync(request);
    } else {
      onReceiveUnsupportedOperation("EsSyncBackgroundActor");
    }
  }

  private void sync(Request message) {
    ProjectLogger.log("EsSyncBackgroundActor: sync called", LoggerEnum.INFO);

    long startTime = System.currentTimeMillis();
    Map<String, Object> req = message.getRequest();
    Map<String, Object> responseMap = new HashMap<>();
    List<Map<String, Object>> reponseList = null;
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> dataMap = (Map<String, Object>) req.get(JsonKey.DATA);

    String objectType = (String) dataMap.get(JsonKey.OBJECT_TYPE);
    List<Object> objectIds = new ArrayList<>();
    if (null != dataMap.get(JsonKey.OBJECT_IDS)) {
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

    if (CollectionUtils.isNotEmpty(objectIds)) {
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
    userMap.put(JsonKey.BATCHES, Util.getUserCourseBatch(userId));
    userMap.put(
        JsonKey.ROOT_ORG_NAME,
        Util.getRootOrgIdOrNameFromChannel((String) userMap.get(JsonKey.CHANNEL), true));

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
}
