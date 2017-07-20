/**
 *
 */
package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;

/**
 * This class will handle all the background job.
 * Example when ever course is published then this job will
 * collect course related data from EKStep and update with Sunbird.
 *
 * @author Manzarul
 * @author Amit Kumar
 */
public class BackgroundJobManager extends UntypedAbstractActor {

  private static Map<String, String> headerMap = new HashMap<String, String>();
  private static Util.DbInfo dbInfo = null;

  static {
    headerMap.put("content-type", "application/json");
    headerMap.put("accept", "application/json");
  }

  private CassandraOperation cassandraOperation = new CassandraOperationImpl();

  @Override
  public void onReceive(Object message) throws Throwable {

    if (message instanceof Response) {
      ProjectLogger.log("BackgroundJobManager  onReceive called");
      if (dbInfo == null) {
        dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_MANAGEMENT_DB);
      }

      Response actorMessage = (Response) message;
      String requestedOperation = (String) actorMessage.get(JsonKey.OPERATION);
      ProjectLogger.log("Operation name is ==" + requestedOperation);
      if (requestedOperation.equalsIgnoreCase(ActorOperations.PUBLISH_COURSE.getValue())) {
        manageBackgroundJob(((Response) actorMessage).getResult());

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_USER_COUNT.getValue())) {
        updateUserCount(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue())) {
        updateUserInfoToEs(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.UPDATE_ORG_INFO_ELASTIC.getValue())) {
        updateOrgInfoToEs(actorMessage);

      } else if (requestedOperation
          .equalsIgnoreCase(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue())) {
        insertOrgInfoToEs(actorMessage);

      } else {
        ProjectLogger.log("UNSUPPORTED OPERATION");
        ProjectCommonException exception = new ProjectCommonException(
            ResponseCode.invalidOperationName.getErrorCode(),
            ResponseCode.invalidOperationName.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
      }
    } else {
      ProjectLogger.log("UNSUPPORTED MESSAGE FOR BACKGROUND JOB MANAGER");
    }
  }

  @SuppressWarnings("unchecked")
  private void insertOrgInfoToEs(Response actorMessage) {
    ProjectLogger.log("Calling method to save inside Es==");
    Map<String, Object> orgMap = (Map<String, Object>) actorMessage.get(JsonKey.ORGANISATION);
    if(ProjectUtil.isNotNull(orgMap)) {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
      String id = (String)orgMap.get(JsonKey.ID);
      Response orgResponse = cassandraOperation.getRecordById(orgDbInfo.getKeySpace() , orgDbInfo.getTableName() , id);
      List<Map<String , Object>> orgList = (List<Map<String, Object>>) orgResponse.getResult().get(JsonKey.RESPONSE);
      Map<String , Object> esMap = new HashMap<String , Object>();
      if(!(orgList.isEmpty())) {
        esMap = orgList.get(0);
      }
        insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(),
            id, esMap);
    }
  }
 
  @SuppressWarnings("unchecked")
  private void updateOrgInfoToEs(Response actorMessage) {
    Map<String, Object> orgMap = (Map<String, Object>) actorMessage.get(JsonKey.ORGANISATION);
    updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.organisation.getTypeName(),
        (String) orgMap.get(JsonKey.ID), orgMap);
  }

  private boolean updateDataToElastic(String indexName, String typeName, String identifier,
      Map<String, Object> data) {
    boolean response = ElasticSearchUtil.updateData(indexName, typeName, identifier, data);
    if (response) {
      return true;
    }
    ProjectLogger.log("unbale to save the data inside ES with identifier " + identifier,
        LoggerEnum.INFO.name());
    return false;

  }

  private void updateUserInfoToEs(Response actorMessage) {
    String userId = (String) actorMessage.get(JsonKey.ID);
    getUserProfile(userId);
  }

  @SuppressWarnings("unchecked")
  private void getUserProfile(String userId) {
    ProjectLogger.log("get user profile method call started user Id : " + userId);
    Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
    Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
    Response response = null;
    List<Map<String, Object>> list = null;
    try {
      response = cassandraOperation
          .getRecordById(userDbInfo.getKeySpace(), userDbInfo.getTableName(), userId);
      list = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      ProjectLogger
          .log("collecting user data to save user id : " + userId, LoggerEnum.INFO.name());
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }

    if (!(list.isEmpty())) {
      Map<String, Object> map = list.get(0);
      Response addrResponse = null;
      list = null;
      try {
        ProjectLogger.log("collecting user address operation user Id : " + userId);
        addrResponse = cassandraOperation
            .getRecordsByProperty(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(),
                JsonKey.USER_ID, userId);
        list = (List<Map<String, Object>>) addrResponse.getResult().get(JsonKey.RESPONSE);
        ProjectLogger.log("collecting user address operation completed user Id : " + userId);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      } finally {
        if (null == list) {
          list = new ArrayList<Map<String, Object>>();
        }
      }
      map.put(JsonKey.ADDRESS, list);
      list = null;
      Response eduResponse = null;
      try {
        eduResponse = cassandraOperation
            .getRecordsByProperty(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(),
                JsonKey.USER_ID, userId);
        list = (List<Map<String, Object>>) eduResponse.getResult().get(JsonKey.RESPONSE);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      } finally {
        if (null == list) {
          list = new ArrayList<Map<String, Object>>();
        }
      }
      for (Map<String, Object> eduMap : list) {
        String addressId = (String) eduMap.get(JsonKey.ADDRESS_ID);
        if (!ProjectUtil.isStringNullOREmpty(addressId)) {

          Response addrResponseMap = null;
          List<Map<String, Object>> addrList = null;
          try {
            addrResponseMap = cassandraOperation
                .getRecordById(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addressId);
            addrList = (List<Map<String, Object>>) addrResponseMap.getResult()
                .get(JsonKey.RESPONSE);
          } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
          } finally {
            if (null == addrList) {
              addrList = new ArrayList<Map<String, Object>>();
            }
          }
          eduMap.put(JsonKey.ADDRESS, addrList.get(0));
        }
      }
      map.put(JsonKey.EDUCATION, list);

      Response jobProfileResponse = null;
      list = null;
      try {
        ProjectLogger.log("collecting user jobprofile user Id : " + userId);
        jobProfileResponse = cassandraOperation
            .getRecordsByProperty(jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(),
                JsonKey.USER_ID, userId);
        list = (List<Map<String, Object>>) jobProfileResponse.getResult().get(JsonKey.RESPONSE);
        ProjectLogger.log("collecting user jobprofile collection completed userId : " + userId);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      } finally {
        if (null == list) {
          list = new ArrayList<Map<String, Object>>();
        }
      }
      for (Map<String, Object> eduMap : list) {
        String addressId = (String) eduMap.get(JsonKey.ADDRESS_ID);
        if (!ProjectUtil.isStringNullOREmpty(addressId)) {
          Response addrResponseMap = null;
          List<Map<String, Object>> addrList = null;
          try {
            addrResponseMap = cassandraOperation
                .getRecordById(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addressId);
            addrList = (List<Map<String, Object>>) addrResponseMap.getResult()
                .get(JsonKey.RESPONSE);
          } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
          } finally {
            if (null == addrList) {
              addrList = new ArrayList<Map<String, Object>>();
            }
          }
          eduMap.put(JsonKey.ADDRESS, addrList.get(0));
        }
      }
      map.put(JsonKey.JOB_PROFILE, list);

      Util.removeAttributes(map, Arrays.asList(JsonKey.PASSWORD, JsonKey.UPDATED_BY, JsonKey.ID));
    } else {
      ProjectLogger
          .log("User data not found to save to ES user Id : " + userId, LoggerEnum.INFO.name());
    }
    if (((List<Map<String, String>>) response.getResult().get(JsonKey.RESPONSE)).size() > 0) {
      ProjectLogger.log("saving started user to es userId : " + userId, LoggerEnum.INFO.name());
      Map<String, Object> map = ((List<Map<String, Object>>) response.getResult()
          .get(JsonKey.RESPONSE)).get(0);
      insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.user.getTypeName(),
          userId, map);
      ProjectLogger.log("saving completed user to es userId : " + userId);
    } else {
      ProjectLogger.log("user data not found to save to ES userId : " + userId);
    }

  }

  /**
   * Method to update the user count .
   */
  @SuppressWarnings("unchecked")
  private void updateUserCount(Response actorMessage) {
    String courseId = (String) actorMessage.get(JsonKey.COURSE_ID);
    Map<String, Object> updateRequestMap = actorMessage.getResult();

    Response result = cassandraOperation
        .getPropertiesValueById(dbInfo.getKeySpace(), dbInfo.getTableName(), courseId,
            JsonKey.USER_COUNT);
    Map<String, Object> responseMap = null;
    if (null != (result.get(JsonKey.RESPONSE))
        && ((List<Map<String, Object>>) result.get(JsonKey.RESPONSE)).size() > 0) {
      responseMap = ((List<Map<String, Object>>) result.get(JsonKey.RESPONSE)).get(0);
    }
    int userCount = (int) (responseMap.get(JsonKey.USER_COUNT) != null ? responseMap
        .get(JsonKey.USER_COUNT) : 0);
    updateRequestMap.put(JsonKey.USER_COUNT, userCount + 1);
    updateRequestMap.put(JsonKey.ID, courseId);
    updateRequestMap.remove(JsonKey.OPERATION);
    updateRequestMap.remove(JsonKey.COURSE_ID);
    Response resposne = cassandraOperation
        .updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), updateRequestMap);
    if (resposne.get(JsonKey.RESPONSE).equals(JsonKey.SUCCESS)) {
      ProjectLogger.log("USER COUNT UPDATED SUCCESSFULLY IN COURSE MGMT TABLE");
    } else {
      ProjectLogger.log("USER COUNT NOT UPDATED SUCCESSFULLY IN COURSE MGMT TABLE");
    }

  }

  /**
   * @param data Map<String, Object>
   * @return boolean
   */
  @SuppressWarnings("unchecked")
  private boolean manageBackgroundJob(Map<String, Object> data) {
    if (data == null) {
      return false;
    }
    List<Map<String, Object>> list = (List<Map<String, Object>>) data.get(JsonKey.RESPONSE);
    Map<String, Object> content = list.get(0);
    String contentId = (String) content.get(JsonKey.CONTENT_ID);
    if (!ProjectUtil.isStringNullOREmpty(contentId)) {
      String contentData = getCourseData(contentId);
      if (!ProjectUtil.isStringNullOREmpty(contentData)) {
        Map<String, Object> map = getContentDetails(contentData);
        map.put(JsonKey.ID, (String) content.get(JsonKey.COURSE_ID));
        updateCourseManagement(map);
        List<String> createdForValue = null;
        Object obj = content.get(JsonKey.COURSE_CREATED_FOR);
        if (obj != null) {
          createdForValue = (List<String>) obj;
        }
        content.remove(JsonKey.COURSE_CREATED_FOR);
        content.put(JsonKey.APPLICABLE_FOR, createdForValue);
        Map<String, Object> finalResponseMap = (Map<String, Object>) map.get(JsonKey.RESULT);
        finalResponseMap.putAll(content);
        finalResponseMap.put(JsonKey.OBJECT_TYPE, ProjectUtil.EsType.course.getTypeName());
        insertDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName(),
            (String) map.get(JsonKey.ID), finalResponseMap);
      }
    }
    return true;
  }

  /**
   * Method to get the course data.
   *
   * @param contnetId String
   * @return String
   */
  private String getCourseData(String contnetId) {
    String responseData = null;
    try {
      responseData = HttpUtil.sendGetRequest(
          PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTNET_URL) + contnetId,
          headerMap);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return responseData;
  }

  /**
   * Method to get the content details of the given content id.
   *
   * @param content String
   * @return Map<String, Object>
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> getContentDetails(String content) {
    Map<String, Object> map = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
    try {
      JSONObject object = new JSONObject(content);
      JSONObject resultObj = object.getJSONObject(JsonKey.RESULT);
      HashMap<String, Map<String, Object>> result =
          mapper.readValue(resultObj.toString(), HashMap.class);
      Map<String, Object> contentMap = result.get(JsonKey.CONTENT);
      map.put(JsonKey.APPICON, contentMap.get(JsonKey.APPICON));
      try {
        map.put(JsonKey.TOC_URL, contentMap.get(JsonKey.TOC_URL));
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
      map.put(JsonKey.COUNT, contentMap.get(JsonKey.LEAF_NODE_COUNT));
      map.put(JsonKey.RESULT, contentMap);

    } catch (JSONException e) {
      ProjectLogger.log(e.getMessage(), e);
    } catch (JsonParseException e) {
      ProjectLogger.log(e.getMessage(), e);
    } catch (JsonMappingException e) {
      ProjectLogger.log(e.getMessage(), e);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return map;
  }

  /**
   * Method to update the course management data on basis of course id.
   *
   * @param data Map<String, Object>
   * @return boolean
   */
  private boolean updateCourseManagement(Map<String, Object> data) {
    Map<String, Object> updateRequestMap = new HashMap<>();
    updateRequestMap
        .put(JsonKey.NO_OF_LECTURES, data.get(JsonKey.COUNT) != null ? data.get(JsonKey.COUNT) : 0);
    updateRequestMap.put(JsonKey.COURSE_LOGO_URL,
        data.get(JsonKey.APPICON) != null ? data.get(JsonKey.APPICON) : "");
    updateRequestMap
        .put(JsonKey.TOC_URL, data.get(JsonKey.TOC_URL) != null ? data.get(JsonKey.TOC_URL) : "");
    updateRequestMap.put(JsonKey.ID, (String) data.get(JsonKey.ID));
    Response resposne = cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(),
        updateRequestMap);
    ProjectLogger.log(resposne.toString());
    if (!(resposne.get(JsonKey.RESPONSE) instanceof ProjectCommonException)) {
      return true;
    }
    return false;
  }

  /**
   * Method to cache the course data .
   *
   * @param index String
   * @param type String
   * @param identifier String
   * @param data Map<String,Object>
   * @return boolean
   */
  private boolean insertDataToElastic(String index, String type, String identifier,
      Map<String, Object> data) {
    ProjectLogger.log("making call to ES for type ,identifier ,data==" + type +" " + identifier + data);
    String response = ElasticSearchUtil.createData(index, type, identifier, data);
    ProjectLogger.log("Getting ES save response for type , identiofier==" +type+"  " + identifier + "  "+ response);
    if (!ProjectUtil.isStringNullOREmpty(response)) {
      ProjectLogger.log("User Data is saved successfully ES ." + type+ "  " + identifier);
      return true;
    }
    ProjectLogger.log("unbale to save the data inside ES with identifier " + identifier,
        LoggerEnum.INFO.name());
    return false;
  }
}
