package org.sunbird.learner.actors.syncjobmanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

/**
 * This class is used to sync the ElasticSearch and DB.
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {"sync"},
  asyncTasks = {}
)
public class EsSyncActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    String requestedOperation = request.getOperation();
    if (requestedOperation.equalsIgnoreCase(ActorOperations.SYNC.getValue())) {
      // return SUCCESS to controller and run the sync process in background
      Response response = new Response();
      response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
      sender().tell(response, self());
      syncData(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private void syncData(Request message) {
    ProjectLogger.log("DB data sync operation to elastic search started ", LoggerEnum.INFO);
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
    if (null != objectIds && !objectIds.isEmpty()) {
      ProjectLogger.log(
          "fetching data for "
              + objectType
              + " for these ids "
              + Arrays.toString(objectIds.toArray())
              + " started",
          LoggerEnum.INFO);
      Response response =
          cassandraOperation.getRecordsByProperty(
              dbInfo.getKeySpace(), dbInfo.getTableName(), JsonKey.ID, objectIds);
      reponseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      ProjectLogger.log(
          "fetching data for "
              + objectType
              + " for these ids "
              + Arrays.toString(objectIds.toArray())
              + " done",
          LoggerEnum.INFO);
    }
    if (null != reponseList && !reponseList.isEmpty()) {
      for (Map<String, Object> map : reponseList) {
        responseMap.put((String) map.get(JsonKey.ID), map);
      }
    } else {
      ProjectLogger.log("fetching all data for " + objectType + " started", LoggerEnum.INFO);
      Response response =
          cassandraOperation.getAllRecords(dbInfo.getKeySpace(), dbInfo.getTableName());
      reponseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      ProjectLogger.log("fetching all data for " + objectType + " done", LoggerEnum.INFO);
      ProjectLogger.log(
          "total db data to sync for " + objectType + " to Elastic search " + reponseList.size(),
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
        "total time taken to sync db data for "
            + objectType
            + " to Elastic search "
            + elapsedTime
            + " ms.",
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
    ProjectLogger.log("fetching org data started", LoggerEnum.INFO);
    Map<String, Object> orgMap = (Map<String, Object>) entry.getValue();
    orgMap.remove(JsonKey.ORG_TYPE);
    if (orgMap.containsKey(JsonKey.ADDRESS_ID)
        && !StringUtils.isBlank((String) orgMap.get(JsonKey.ADDRESS_ID))) {
      orgMap.put(
          JsonKey.ADDRESS,
          getDetailsById(
              Util.dbInfoMap.get(JsonKey.ADDRESS_DB), (String) orgMap.get(JsonKey.ADDRESS_ID)));
    }
    ProjectLogger.log("fetching org data completed", LoggerEnum.INFO);
    return orgMap;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getUserDetails(Entry<String, Object> entry) {
    String userId = entry.getKey();
    ProjectLogger.log("fetching user data started", LoggerEnum.INFO);
    Map<String, Object> userMap = (Map<String, Object>) entry.getValue();
    Util.removeAttributes(userMap, Arrays.asList(JsonKey.PASSWORD, JsonKey.UPDATED_BY));
    if (StringUtils.isBlank((String) userMap.get(JsonKey.COUNTRY_CODE))) {
      userMap.put(
          JsonKey.COUNTRY_CODE,
          PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_DEFAULT_COUNTRY_CODE));
    }
    ProjectLogger.log("fetching user address data started", LoggerEnum.INFO);
    userMap.put(JsonKey.ADDRESS, Util.getAddressDetails(userId, null));
    ProjectLogger.log("fetching user education data started", LoggerEnum.INFO);
    userMap.put(JsonKey.EDUCATION, Util.getUserEducationDetails(userId));
    ProjectLogger.log("fetching user job profile data started", LoggerEnum.INFO);
    userMap.put(JsonKey.JOB_PROFILE, Util.getJobProfileDetails(userId));
    ProjectLogger.log("fetching user org data started", LoggerEnum.INFO);
    userMap.put(JsonKey.ORGANISATIONS, Util.getUserOrgDetails(userId));
    ProjectLogger.log("fetching user Badge data  started", LoggerEnum.INFO);
    userMap.put(BadgingJsonKey.BADGE_ASSERTIONS, Util.getUserBadge(userId));
    // save masked email and phone number
    Util.addMaskEmailAndPhone(userMap);
    // add the skills column into ES
    Util.getUserSkills(userId);
    // compute profile completeness and error field.
    Util.checkProfileCompleteness(userMap);

    String registryId = (String) userMap.get(JsonKey.REGISTRY_ID);
    if (StringUtils.isNotBlank(registryId)) {
      try {
        userMap = Util.getUserDetailsFromRegistry(userMap);
      } catch (Exception ex) {
        ProjectLogger.log(
            "EsSyncActor:getUserDetails : Failed to fetch registry details for registryId: "
                + registryId,
            LoggerEnum.INFO.name());
      }
    }
    ProjectLogger.log("fetching user data completed", LoggerEnum.INFO);
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
