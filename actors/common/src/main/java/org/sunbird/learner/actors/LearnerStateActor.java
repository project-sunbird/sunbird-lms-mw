package org.sunbird.learner.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

/**
 * This actor will handle leaner's state operation like get course , get content etc.
 *
 * @author Manzarul
 * @author Arvind
 */
@ActorConfig(
  tasks = {"getCourse", "getContent"},
  asyncTasks = {}
)
public class LearnerStateActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  /**
   * Receives the actor message and perform the operation like get course , get content etc.
   *
   * @param request Object
   */
  @Override
  public void onReceive(Request request) throws Exception {
    Response response = new Response();
    if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_COURSE.getValue())) {
      String userId = (String) request.getRequest().get(JsonKey.USER_ID);

      Map<String, Object> filter = new HashMap<>();
      filter.put(JsonKey.USER_ID, userId);

      SearchDTO searchDto = new SearchDTO();
      searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filter);

      Map<String, Object> result =
          ElasticSearchUtil.complexSearch(
              searchDto,
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.usercourses.getTypeName());

      response.put(JsonKey.RESPONSE, result.get(JsonKey.CONTENT));
      sender().tell(response, self());

    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_CONTENT.getValue())) {

      Response res = new Response();
      Map<String, Object> requestMap = request.getRequest();
      String userId = (String) request.getRequest().get(JsonKey.USER_ID);

      if (StringUtils.isBlank(userId)) {
        throw new ProjectCommonException(
            ResponseCode.mandatoryParamsMissing.getErrorCode(),
            ResponseCode.mandatoryParamsMissing.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode(),
            JsonKey.USER_ID);
      }

      res = getCourseContentState(userId, requestMap);
      removeUnwantedProperties(res);
      sender().tell(res, self());
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private Response getCourseContentState(String userId, Map<String, Object> requestMap) {

    Response response = new Response();
    List<Map<String, Object>> contentList = null;

    String batchId = (String) requestMap.get(JsonKey.BATCH_ID);
    List<String> courseIds = new ArrayList<>();
    List<String> contentIds = new ArrayList<>();

    if (null != requestMap.get(JsonKey.COURSE_IDS)) {
      courseIds = (List<String>) requestMap.get(JsonKey.COURSE_IDS);
      if (courseIds.size() > 1 && StringUtils.isNotBlank(batchId)) {
        ProjectLogger.log(
            "LearnerStateActor:getContentByBatch: multiple course ids not allowed for batch",
            LoggerEnum.ERROR.name());
        throw new ProjectCommonException(
            ResponseCode.multipleCoursesNotAllowedForBatch.getErrorCode(),
            ResponseCode.multipleCoursesNotAllowedForBatch.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }

    if (null != requestMap.get(JsonKey.CONTENT_IDS)) {
      contentIds = (List<String>) requestMap.get(JsonKey.CONTENT_IDS);
    }

    if (contentIds.size() > 0 && courseIds.size() == 1 && StringUtils.isNotBlank(batchId)) {
      List<Object> primaryKeyList = new ArrayList<>();
      String courseId = courseIds.get(0);
      for (String contentId : contentIds) {
        String key = generatePrimaryKeyForContent(userId, batchId, courseId, contentId);
        primaryKeyList.add(key);
      }
      contentList = getContentByPrimaryKeys(primaryKeyList);
    } else if (StringUtils.isNotBlank(batchId)) {
      contentList = getContentByBatch(userId, batchId);
      if (null != requestMap.get(JsonKey.CONTENT_IDS)) {
        contentList = filterForMatchingContentIds(contentList, requestMap);
      }
    } else if (CollectionUtils.isNotEmpty(courseIds)) {
      contentList = getContentByCourses(userId, requestMap);
      if (courseIds.size() == 1) {
        if (null != requestMap.get(JsonKey.CONTENT_IDS)) {
          contentList = filterForMatchingContentIds(contentList, requestMap);
        }
      }
    } else if (CollectionUtils.isNotEmpty(contentIds)) {
      contentList = getContentByContentIds(userId, requestMap);
    }
    response.getResult().put(JsonKey.RESPONSE, contentList);
    return response;
  }

  private List<Map<String, Object>> filterForMatchingContentIds(
      List<Map<String, Object>> contentList, Map<String, Object> requestMap) {

    List<String> contentIds =
        new ArrayList<String>((List<String>) requestMap.get(JsonKey.CONTENT_IDS));
    List<Map<String, Object>> matchedContentList = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(contentIds)) {
      for (Map<String, Object> map : contentList) {
        boolean flag = true;
        for (int i = 0; i < contentIds.size() && flag; i++) {
          String contentId = contentIds.get(i);
          if (contentId.equals((String) map.get(JsonKey.CONTENT_ID))) {
            matchedContentList.add(map);
            flag = false;
          }
        }
      }
    }
    return matchedContentList;
  }

  private List<Map<String, Object>> getContentByBatch(String userId, String batchId) {

    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);

    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
    Map<String, Object> queryMap = new HashMap<String, Object>();
    queryMap.put(JsonKey.USER_ID, userId);
    queryMap.put(JsonKey.BATCH_ID, batchId);

    Response response =
        cassandraOperation.getRecordsByProperties(
            dbInfo.getKeySpace(), dbInfo.getTableName(), queryMap);
    contentList.addAll((List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE));
    return contentList;
  }

  private List<Map<String, Object>> getContentByPrimaryKeys(List<Object> primaryKeyList) {

    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);

    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
    Response response =
        cassandraOperation.getRecordsByProperty(
            dbInfo.getKeySpace(), dbInfo.getTableName(), JsonKey.ID, primaryKeyList);
    contentList.addAll((List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE));
    return contentList;
  }

  /**
   * Method to get content on basis of course ids.
   *
   * @param userId String
   * @param request Map<String, Object>
   * @return Response
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getContentByCourses(
      String userId, Map<String, Object> request) {

    Response response = new Response();
    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
    List<String> courseIds = new ArrayList<String>((List<String>) request.get(JsonKey.COURSE_IDS));
    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();

    Map<String, Object> queryMap = new HashMap<String, Object>();
    queryMap.put(JsonKey.USER_ID, userId);

    for (String courseId : courseIds) {
      queryMap.put(JsonKey.COURSE_ID, courseId);
      response =
          cassandraOperation.getRecordsByProperties(
              dbInfo.getKeySpace(), dbInfo.getTableName(), queryMap);
      contentList.addAll((List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE));
    }
    return contentList;
  }

  /** Method to get content on basis of content ids. */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getContentByContentIds(
      String userId, Map<String, Object> request) {
    Response response = null;
    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
    List<String> contentIds =
        new ArrayList<String>((List<String>) request.get(JsonKey.CONTENT_IDS));

    LinkedHashMap<String, Object> queryMap = new LinkedHashMap<String, Object>();
    queryMap.put(JsonKey.USER_ID, userId);

    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();

    for (String contentId : contentIds) {
      queryMap.put(JsonKey.CONTENT_ID, contentId);
      response =
          cassandraOperation.getRecordsByProperties(
              dbInfo.getKeySpace(), dbInfo.getTableName(), queryMap);
      contentList.addAll((List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE));
    }
    return contentList;
  }

  @SuppressWarnings("unchecked")
  private void removeUnwantedProperties(Response response) {
    List<Map<String, Object>> list =
        (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    for (Map<String, Object> map : list) {
      map.remove(JsonKey.DATE_TIME);
      map.remove(JsonKey.USER_ID);
      map.remove(JsonKey.ADDED_BY);
      map.remove(JsonKey.LAST_UPDATED_TIME);
    }
  }

  private String generatePrimaryKeyForContent(
      String userId, String batchId, String courseId, String contentId) {
    String key =
        userId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + contentId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + batchId;
    return OneWayHashing.encryptVal(key);
  }
}
