package org.sunbird.learner.actors;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.ProgressStatus;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.TelemetryUtil;

/**
 * This actor will handle course batch related operations.
 *
 * @author Manzarul
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {
    "createBatch",
    "updateBatch",
    "addUserBatch",
    "removeUserFromBatch",
    "getBatch",
    "getCourseBatchDetail"
  },
  asyncTasks = {}
)
public class CourseBatchManagementActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo dbInfo = null;

  /**
   * Receives the actor message and perform the course enrollment operation .
   *
   * @param message Object is an instance of Request
   */
  @Override
  public void onReceive(Request request) throws Throwable {
    dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);

    Util.initializeContext(request, TelemetryEnvKey.BATCH);
    // set request id to thread local...
    ExecutionContext.setRequestId(request.getRequestId());

    String requestedOperation = request.getOperation();
    if (requestedOperation.equalsIgnoreCase(ActorOperations.CREATE_BATCH.getValue())) {
      createCourseBatch(request);
    }
    if (requestedOperation.equalsIgnoreCase(ActorOperations.UPDATE_BATCH.getValue())) {
      updateCourseBatch(request);
    }
    if (requestedOperation.equalsIgnoreCase(ActorOperations.GET_BATCH.getValue())) {
      getCourseBatch(request);
    } else if (requestedOperation.equalsIgnoreCase(ActorOperations.ADD_USER_TO_BATCH.getValue())) {
      addUserCourseBatch(request);
    } else if (requestedOperation.equalsIgnoreCase(
        ActorOperations.GET_COURSE_BATCH_DETAIL.getValue())) {
      getCourseBatchDetail(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private void getCourseBatch(Request actorMessage) {

    Map<String, Object> map = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    Map<String, Object> result =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName(),
            (String) map.get(JsonKey.BATCH_ID));
    Response response = new Response();
    if (null != result) {
      response.put(JsonKey.RESPONSE, result);
    } else {
      result = new HashMap<>();
      response.put(JsonKey.RESPONSE, result);
    }
    sender().tell(response, self());
  }

  private void getCourseBatchDetail(Request actorMessage) {

    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    String batchId = (String) req.get(JsonKey.BATCH_ID);
    Response result =
        cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(), batchId);
    List<Map<String, Object>> courseList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if ((courseList.isEmpty())) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    sender().tell(result, self());
  }

  private void addUserCourseBatch(Request actorMessage) {

    ProjectLogger.log("Add user to course batch - called");
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    Response response = new Response();

    // objects of telemetry event...
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    String batchId = (String) req.get(JsonKey.BATCH_ID);
    TelemetryUtil.generateCorrelatedObject(batchId, JsonKey.BATCH, null, correlatedObject);
    // check batch exist in db or not
    Response courseBatchResult =
        cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(), batchId);
    List<Map<String, Object>> courseList =
        (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
    if ((courseList.isEmpty())) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String, Object> courseBatchObject = courseList.get(0);
    // check whether coursebbatch type is invite only or not ...
    if (ProjectUtil.isNull(courseBatchObject.get(JsonKey.ENROLLMENT_TYPE))
        || !((String) courseBatchObject.get(JsonKey.ENROLLMENT_TYPE))
            .equalsIgnoreCase(JsonKey.INVITE_ONLY)) {
      throw new ProjectCommonException(
          ResponseCode.enrollmentTypeValidation.getErrorCode(),
          ResponseCode.enrollmentTypeValidation.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (ProjectUtil.isNull(courseBatchObject.get(JsonKey.COURSE_CREATED_FOR))
        || ((List) courseBatchObject.get(JsonKey.COURSE_CREATED_FOR)).isEmpty()) {
      // throw exception since batch does not belong to any createdfor in DB ...
      throw new ProjectCommonException(
          ResponseCode.courseCreatedForIsNull.getErrorCode(),
          ResponseCode.courseCreatedForIsNull.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }

    String batchCreator = (String) courseBatchObject.get(JsonKey.CREATED_BY);
    if (StringUtils.isBlank(batchCreator)) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseCreatorId.getErrorCode(),
          ResponseCode.invalidCourseCreatorId.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    String batchCreatorRootOrgId = getRootOrg(batchCreator);

    Map<String, Boolean> participants =
        (Map<String, Boolean>) courseBatchObject.get(JsonKey.PARTICIPANT);
    // check whether can update user or not
    List<String> userIds = (List<String>) req.get(JsonKey.USER_IDs);
    if (participants == null) {
      participants = new HashMap<>();
    }

    Map<String, String> participantWithRootOrgIds = getRootOrgForMultipleUsers(userIds);

    for (String userId : userIds) {
      if (!(participants.containsKey(userId))) {
        if (!participantWithRootOrgIds.containsKey(userId)
            || (!batchCreatorRootOrgId.equals(participantWithRootOrgIds.get(userId)))) {
          response.put(userId, ResponseCode.userNotAssociatedToRootOrg.getErrorMessage());
          continue;
        }

        participants.put(
            userId,
            addUserCourses(
                batchId,
                (String) courseBatchObject.get(JsonKey.COURSE_ID),
                userId,
                (Map<String, String>) (courseBatchObject.get(JsonKey.COURSE_ADDITIONAL_INFO))));

        response.getResult().put(userId, JsonKey.SUCCESS);
        // create audit log for user here that user associated to the batch here , here
        // user is the targer object ...
        targetObject =
            TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        correlatedObject = new ArrayList<>();
        TelemetryUtil.generateCorrelatedObject(batchId, JsonKey.BATCH, null, correlatedObject);
        TelemetryUtil.telemetryProcessingCall(req, targetObject, correlatedObject);

      } else {
        response.getResult().put(userId, JsonKey.SUCCESS);
      }
    }

    courseBatchObject.put(JsonKey.PARTICIPANT, participants);
    cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), courseBatchObject);
    sender().tell(response, self());

    ProjectLogger.log("method call going to satrt for ES--.....");
    Request request = new Request();
    request.setOperation(ActorOperations.UPDATE_COURSE_BATCH_ES.getValue());
    request.getRequest().put(JsonKey.BATCH, courseBatchObject);
    ProjectLogger.log("making a call to save Course Batch data to ES");
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log(
          "Exception Occurred during saving Course Batch to Es while updating Course Batch : ", ex);
    }
  }

  private Map<String, String> getRootOrgForMultipleUsers(List<String> userIds) {

    Map<String, String> userWithRootOrgs = new HashMap<>();
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.ID, userIds);

    List<String> fields = new ArrayList<>();
    fields.add(JsonKey.ROOT_ORG_ID);
    fields.add(JsonKey.ID);
    fields.add(JsonKey.REGISTERED_ORG);

    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    searchDTO.setFields(fields);

    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDTO, ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());

    List<Map<String, Object>> esContent = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);

    for (Map<String, Object> user : esContent) {
      String rootOrg = getRootOrgFromUserMap(user);
      userWithRootOrgs.put((String) user.get(JsonKey.ID), rootOrg);
    }
    return userWithRootOrgs;
  }

  private String getRootOrg(String batchCreator) {
    Map<String, Object> userInfo =
        ElasticSearchUtil.getDataByIdentifier(
            EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), batchCreator);
    return getRootOrgFromUserMap(userInfo);
  }

  private String getRootOrgFromUserMap(Map<String, Object> userInfo) {

    String rootOrg = (String) userInfo.get(JsonKey.ROOT_ORG_ID);
    Map<String, Object> registeredOrgInfo =
        (Map<String, Object>) userInfo.get(JsonKey.REGISTERED_ORG);
    if (registeredOrgInfo != null && !registeredOrgInfo.isEmpty()) {
      if (null != registeredOrgInfo.get(JsonKey.IS_ROOT_ORG)
          && (Boolean) registeredOrgInfo.get(JsonKey.IS_ROOT_ORG)) {
        rootOrg = (String) registeredOrgInfo.get(JsonKey.ID);
      }
    }
    return rootOrg;
  }

  private Boolean addUserCourses(
      String batchId, String courseId, String userId, Map<String, String> additionalCourseInfo) {

    Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    Util.DbInfo coursePublishdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
    Response response =
        cassandraOperation.getRecordById(
            coursePublishdbInfo.getKeySpace(), coursePublishdbInfo.getTableName(), courseId);
    List<Map<String, Object>> resultList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (!ProjectUtil.CourseMgmtStatus.LIVE
        .getValue()
        .equalsIgnoreCase(additionalCourseInfo.get(JsonKey.STATUS))) {
      if (resultList.isEmpty()) {
        return false;
      }
      Map<String, Object> publishStatus = resultList.get(0);

      if (Status.ACTIVE.getValue() != (Integer) publishStatus.get(JsonKey.STATUS)) {
        return false;
      }
    }
    Boolean flag = false;
    Map<String, Object> userCourses = new HashMap<>();
    userCourses.put(JsonKey.USER_ID, userId);
    userCourses.put(JsonKey.BATCH_ID, batchId);
    userCourses.put(JsonKey.COURSE_ID, courseId);
    userCourses.put(JsonKey.ID, generatePrimaryKey(userCourses));
    userCourses.put(JsonKey.CONTENT_ID, courseId);
    userCourses.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
    userCourses.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
    userCourses.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
    userCourses.put(JsonKey.COURSE_PROGRESS, 0);
    userCourses.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.COURSE_LOGO_URL));
    userCourses.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.COURSE_NAME));
    userCourses.put(JsonKey.DESCRIPTION, additionalCourseInfo.get(JsonKey.DESCRIPTION));
    if (!StringUtils.isBlank(additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT))) {
      userCourses.put(
          JsonKey.LEAF_NODE_COUNT,
          Integer.parseInt("" + additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT)));
    }
    userCourses.put(JsonKey.TOC_URL, additionalCourseInfo.get(JsonKey.TOC_URL));
    try {
      cassandraOperation.insertRecord(
          courseEnrollmentdbInfo.getKeySpace(), courseEnrollmentdbInfo.getTableName(), userCourses);
      insertUserCoursesToES(userCourses);
      flag = true;
    } catch (Exception ex) {
      ProjectLogger.log("INSERT RECORD TO USER COURSES EXCEPTION ", ex);
      flag = false;
    }
    return flag;
  }

  private String generatePrimaryKey(Map<String, Object> req) {
    String userId = (String) req.get(JsonKey.USER_ID);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    String batchId = (String) req.get(JsonKey.BATCH_ID);
    return OneWayHashing.encryptVal(
        userId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + batchId);
  }

  /**
   * This method will create course under cassandra db.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void createCourseBatch(Request actorMessage) {

    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    Map<String, String> headers =
        (Map<String, String>) actorMessage.getRequest().get(JsonKey.HEADER);

    // objects of telemetry event...
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    String courseId = (String) req.get(JsonKey.COURSE_ID);
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Map<String, Object> ekStepContent =
        CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, headers);
    if (null == ekStepContent || ekStepContent.size() == 0) {
      ProjectLogger.log("Course Id not found in EkStep");
      throw new ProjectCommonException(
          ResponseCode.invalidCourseId.getErrorCode(),
          ResponseCode.invalidCourseId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    String enrolmentType = (String) req.get(JsonKey.ENROLLMENT_TYPE);
    List<String> createdFor = new ArrayList<>();
    if (req.containsKey(JsonKey.COURSE_CREATED_FOR)
        && req.get(JsonKey.COURSE_CREATED_FOR) instanceof List) {
      createdFor = (List<String>) req.get(JsonKey.COURSE_CREATED_FOR);
    }
    if (ProjectUtil.EnrolmentType.inviteOnly.getVal().equalsIgnoreCase(enrolmentType)
        && createdFor != null) {
      for (String orgId : createdFor) {
        if (!isOrgValid(orgId)) {
          throw new ProjectCommonException(
              ResponseCode.invalidOrgId.getErrorCode(),
              ResponseCode.invalidOrgId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
    if (req.containsKey(JsonKey.MENTORS) && req.get(JsonKey.MENTORS) instanceof List) {
      List<String> mentors = (List<String>) req.get(JsonKey.MENTORS);
      for (String userId : mentors) {
        Map<String, Object> result =
            ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName(),
                userId);
        // check whether is_deletd true or false
        if ((ProjectUtil.isNull(result))
            || (ProjectUtil.isNotNull(result) && result.isEmpty())
            || (ProjectUtil.isNotNull(result)
                && result.containsKey(JsonKey.IS_DELETED)
                && ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))
                && (Boolean) result.get(JsonKey.IS_DELETED))) {
          throw new ProjectCommonException(
              ResponseCode.invalidUserId.getErrorCode(),
              ResponseCode.invalidUserId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
    req.remove(JsonKey.PARTICIPANT);
    String courseCreator = (String) ekStepContent.get(JsonKey.CREATED_BY);
    String createdBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    req.put(JsonKey.ID, uniqueId);
    req.put(JsonKey.COURSE_ID, courseId);
    req.put(JsonKey.COURSE_CREATOR, courseCreator);
    req.put(JsonKey.CREATED_BY, createdBy);
    req.put(JsonKey.COUNTER_DECREMENT_STATUS, false);
    req.put(JsonKey.COUNTER_INCREMENT_STATUS, false);
    try {
      Date todaydate = format.parse(format.format(new Date()));
      Date requestedStartDate = format.parse((String) req.get(JsonKey.START_DATE));
      if (todaydate.compareTo(requestedStartDate) == 0) {
        req.put(JsonKey.STATUS, ProgressStatus.STARTED.getValue());
      } else {
        req.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
      }
    } catch (ParseException e) {
      ProjectLogger.log("Exception occurred while parsing date in CourseBatchManagementActor ", e);
    }

    req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    req.put(JsonKey.COURSE_ADDITIONAL_INFO, getAdditionalCourseInfo(ekStepContent));
    if (!StringUtils.isBlank(((String) req.get(JsonKey.HASHTAGID)))) {
      req.put(
          JsonKey.HASHTAGID,
          validateHashTagId(((String) req.get(JsonKey.HASHTAGID)), JsonKey.CREATE, ""));
    } else {
      req.put(JsonKey.HASHTAGID, uniqueId);
    }
    req.put(JsonKey.COUNTER_INCREMENT_STATUS, false);
    req.put(JsonKey.COUNTER_DECREMENT_STATUS, false);

    Response result =
        cassandraOperation.insertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), req);

    ProjectLogger.log(
        "Course Batch data saving to ES started-- for Id  " + uniqueId, LoggerEnum.INFO.name());
    String esResponse =
        ElasticSearchUtil.createData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName(),
            uniqueId,
            req);
    ProjectLogger.log(
        "Course Batch data saving to ES Completed -- for Id "
            + uniqueId
            + " with response =="
            + esResponse,
        LoggerEnum.INFO.name());
    result.put(JsonKey.BATCH_ID, uniqueId);
    sender().tell(result, self());

    targetObject =
        TelemetryUtil.generateTargetObject(uniqueId, JsonKey.BATCH, JsonKey.CREATE, null);
    TelemetryUtil.generateCorrelatedObject(courseId, JsonKey.COURSE, null, correlatedObject);
    TelemetryUtil.telemetryProcessingCall(req, targetObject, correlatedObject);

    Map<String, String> rollUp = new HashMap<>();
    rollUp.put("l1", courseId);
    TelemetryUtil.addTargetObjectRollUp(rollUp, targetObject);

    if (((String) result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("method call going to satrt for ES--.....");
      Request request = new Request();
      request.setOperation(ActorOperations.INSERT_COURSE_BATCH_ES.getValue());
      request.getRequest().put(JsonKey.BATCH, req);
      ProjectLogger.log("making a call to save Course Batch data to ES");
      try {
        tellToAnother(request);
      } catch (Exception ex) {
        ProjectLogger.log(
            "Exception Occurred during saving Course Batch to Es while creating Course Batch : ",
            ex);
      }
    } else {
      ProjectLogger.log("no call for ES to save Course Batch");
    }
  }

  private String validateHashTagId(String hashTagId, String opType, String id) {
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.HASHTAGID, hashTagId);
    SearchDTO searchDto = new SearchDTO();
    searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDto,
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName());
    List<Map<String, Object>> dataMapList = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
    if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
      if (!dataMapList.isEmpty()) {
        throw new ProjectCommonException(
            ResponseCode.invalidHashTagId.getErrorCode(),
            ResponseCode.invalidHashTagId.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else if (opType.equalsIgnoreCase(JsonKey.UPDATE) && !dataMapList.isEmpty()) {
      Map<String, Object> batchMap = dataMapList.get(0);
      if (!(((String) batchMap.get(JsonKey.ID)).equalsIgnoreCase(id))) {
        throw new ProjectCommonException(
            ResponseCode.invalidHashTagId.getErrorCode(),
            ResponseCode.invalidHashTagId.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    return hashTagId;
  }

  private Map<String, String> getAdditionalCourseInfo(Map<String, Object> ekStepContent) {

    Map<String, String> courseMap = new HashMap<>();
    courseMap.put(
        JsonKey.COURSE_LOGO_URL,
        (String) ekStepContent.getOrDefault(JsonKey.APP_ICON, "") != null
            ? (String) ekStepContent.getOrDefault(JsonKey.APP_ICON, "")
            : "");
    courseMap.put(
        JsonKey.COURSE_NAME,
        (String) ekStepContent.getOrDefault(JsonKey.NAME, "") != null
            ? (String) ekStepContent.getOrDefault(JsonKey.NAME, "")
            : "");
    courseMap.put(
        JsonKey.DESCRIPTION,
        (String) ekStepContent.getOrDefault(JsonKey.DESCRIPTION, "") != null
            ? (String) ekStepContent.getOrDefault(JsonKey.DESCRIPTION, "")
            : "");
    courseMap.put(
        JsonKey.TOC_URL,
        (String) ekStepContent.getOrDefault("toc_url", "") != null
            ? (String) ekStepContent.getOrDefault("toc_url", "")
            : "");
    if (ProjectUtil.isNotNull(ekStepContent.get(JsonKey.LEAF_NODE_COUNT))) {
      courseMap.put(
          JsonKey.LEAF_NODE_COUNT,
          ((Integer) ekStepContent.get(JsonKey.LEAF_NODE_COUNT)).toString());
    }
    courseMap.put(JsonKey.STATUS, (String) ekStepContent.getOrDefault(JsonKey.STATUS, ""));
    return courseMap;
  }

  /**
   * This method will allow user to update the course batch details.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void updateCourseBatch(Request actorMessage) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    // objects of telemetry event...
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    Response response =
        cassandraOperation.getRecordById(
            dbInfo.getKeySpace(), dbInfo.getTableName(), (String) req.get(JsonKey.ID));
    req.remove(JsonKey.IDENTIFIER);
    req.remove(JsonKey.STATUS);
    req.remove(JsonKey.COUNTER_INCREMENT_STATUS);
    req.remove(JsonKey.COUNTER_DECREMENT_STATUS);
    req.remove(JsonKey.PARTICIPANT);
    if (!StringUtils.isBlank(((String) req.get(JsonKey.HASHTAGID)))) {
      req.put(
          JsonKey.HASHTAGID,
          validateHashTagId(
              ((String) req.get(JsonKey.HASHTAGID)), JsonKey.UPDATE, (String) req.get(JsonKey.ID)));
    }
    List<Map<String, Object>> resList =
        ((List<Map<String, Object>>) response.get(JsonKey.RESPONSE));
    if (null != resList && !resList.isEmpty()) {
      Map<String, Object> res = resList.get(0);
      // to check batch is closed or not , if closed then throw exception.
      Date todate = null;
      Date endDate = null;
      try {
        if (!StringUtils.isBlank((String) res.get(JsonKey.END_DATE))) {
          endDate = format.parse((String) res.get(JsonKey.END_DATE));
        }
        todate = format.parse(format.format(new Date()));
      } catch (ParseException e) {
        ProjectLogger.log(
            "Exception occurred while parsing date in CourseBatchManagementActor ", e);
      }
      if (null != endDate && endDate.before(todate)) {
        throw new ProjectCommonException(
            ResponseCode.BatchCloseError.getErrorCode(),
            ResponseCode.BatchCloseError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      // Batch validation , if start date and End date both are coming.
      if (null != req.get(JsonKey.START_DATE) && null != req.get(JsonKey.END_DATE)) {
        Date dbBatchStartDate = null;
        Date todaydate = null;
        Date dbBatchEndDate = null;
        Date requestedStartDate = null;
        try {
          requestedStartDate = format.parse((String) req.get(JsonKey.START_DATE));
          dbBatchStartDate = format.parse((String) res.get(JsonKey.START_DATE));
          todaydate = format.parse(format.format(new Date()));
          if (!StringUtils.isBlank((String) res.get(JsonKey.END_DATE))) {
            dbBatchEndDate = format.parse((String) res.get(JsonKey.END_DATE));
          }
        } catch (Exception e) {
          ProjectLogger.log(
              "Exception occurred while parsing date in CourseBatchManagementActor ", e);
        }
        if (null != dbBatchEndDate && dbBatchEndDate.before(todaydate)) {
          throw new ProjectCommonException(
              ResponseCode.courseBatchEndDateError.getErrorCode(),
              ResponseCode.courseBatchEndDateError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (dbBatchStartDate.before(todaydate)) {
          if (!(requestedStartDate.equals(dbBatchStartDate))) {
            throw new ProjectCommonException(
                ResponseCode.invalidBatchStartDateError.getErrorCode(),
                ResponseCode.invalidBatchStartDateError.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          }
        } else {
          if (requestedStartDate.before(todaydate)) {
            throw new ProjectCommonException(
                ResponseCode.invalidBatchStartDateError.getErrorCode(),
                ResponseCode.invalidBatchStartDateError.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          }
        }

      } else if (null != req.get(JsonKey.START_DATE) && null == req.get(JsonKey.END_DATE)) {
        // Batch validation , if only start date is coming.
        Date dbBatchStartDate = null;
        Date todaydate = null;
        Date requestedStartDate = null;
        Date dbBatchEndDate = null;
        try {
          dbBatchStartDate = format.parse((String) res.get(JsonKey.START_DATE));
          requestedStartDate = format.parse((String) req.get(JsonKey.START_DATE));
          todaydate = format.parse(format.format(new Date()));
          if (!StringUtils.isBlank((String) res.get(JsonKey.END_DATE))) {
            dbBatchEndDate = format.parse((String) res.get(JsonKey.END_DATE));
          }
        } catch (Exception e) {
          ProjectLogger.log(
              "Exception occurred while parsing date in CourseBatchManagementActor ", e);
        }
        if (dbBatchStartDate.before(todaydate)) {
          if (!(requestedStartDate.equals(dbBatchStartDate))) {
            throw new ProjectCommonException(
                ResponseCode.invalidBatchStartDateError.getErrorCode(),
                ResponseCode.invalidBatchStartDateError.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          }
        } else {
          if (requestedStartDate.before(todaydate)) {
            throw new ProjectCommonException(
                ResponseCode.invalidBatchStartDateError.getErrorCode(),
                ResponseCode.invalidBatchStartDateError.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          }
        }
        if (todaydate.compareTo(requestedStartDate) == 0) {
          req.put(JsonKey.STATUS, ProgressStatus.STARTED.getValue());
        }
        if (dbBatchEndDate != null && requestedStartDate.after(dbBatchEndDate)) {
          throw new ProjectCommonException(
              ResponseCode.invalidBatchStartDateError.getErrorCode(),
              ResponseCode.invalidBatchStartDateError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }

      } else if (null == req.get(JsonKey.START_DATE) && null != req.get(JsonKey.END_DATE)) {
        // Batch validation , if only End date is coming.
        Date dbBatchStartDate = null;
        Date todaydate = null;
        Date requestedEndDate = null;
        Date dbBatchEndDate = null;
        try {
          dbBatchStartDate = format.parse((String) res.get(JsonKey.START_DATE));
          requestedEndDate = format.parse((String) req.get(JsonKey.END_DATE));
          todaydate = format.parse(format.format(new Date()));
          if (!StringUtils.isBlank((String) res.get(JsonKey.END_DATE))) {
            dbBatchEndDate = format.parse((String) res.get(JsonKey.END_DATE));
          }
        } catch (ParseException e) {
          ProjectLogger.log(
              "Exception occurred while parsing date in CourseBatchManagementActor ", e);
        }
        if (dbBatchEndDate != null && dbBatchEndDate.before(todaydate)) {
          throw new ProjectCommonException(
              ResponseCode.courseBatchEndDateError.getErrorCode(),
              ResponseCode.courseBatchEndDateError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (dbBatchStartDate.after(requestedEndDate)) {
          throw new ProjectCommonException(
              ResponseCode.invalidBatchStartDateError.getErrorCode(),
              ResponseCode.invalidBatchStartDateError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }

      String enrolmentType = "";
      if (req.containsKey(JsonKey.ENROLMENTTYPE)) {
        enrolmentType = (String) req.get(JsonKey.ENROLMENTTYPE);
      } else {
        enrolmentType = (String) res.get(JsonKey.ENROLMENTTYPE);
      }
      List<String> createdFor = new ArrayList<>();
      if (req.containsKey(JsonKey.COURSE_CREATED_FOR)
          && req.get(JsonKey.COURSE_CREATED_FOR) instanceof List) {
        createdFor = (List<String>) req.get(JsonKey.COURSE_CREATED_FOR);
      }

      if (null != createdFor) {
        List<String> dbValueCreatedFor = (List<String>) res.get(JsonKey.COURSE_CREATED_FOR);
        if (ProjectUtil.EnrolmentType.inviteOnly.getVal().equalsIgnoreCase(enrolmentType)) {
          if (createdFor != null) {
            for (String orgId : createdFor) {
              if (!dbValueCreatedFor.contains(orgId)) {
                if (!isOrgValid(orgId)) {
                  throw new ProjectCommonException(
                      ResponseCode.invalidOrgId.getErrorCode(),
                      ResponseCode.invalidOrgId.getErrorMessage(),
                      ResponseCode.CLIENT_ERROR.getResponseCode());
                } else {
                  dbValueCreatedFor.add(orgId);
                }
              }
            }
          }
        } else {
          // enrollmentType is open
          if (null != createdFor)
            for (String orgId : createdFor) {
              if (!dbValueCreatedFor.contains(orgId)) {
                dbValueCreatedFor.add(orgId);
              }
            }
        }
        req.put(JsonKey.COURSE_CREATED_FOR, dbValueCreatedFor);
      }

      if (req.containsKey(JsonKey.MENTORS) && req.get(JsonKey.MENTORS) instanceof List) {
        List<String> mentors = (List<String>) req.get(JsonKey.MENTORS);
        List<String> dnMentorsValue = (List<String>) res.get(JsonKey.MENTORS);
        if (null != mentors) {
          for (String userId : mentors) {
            if (!dnMentorsValue.contains(userId)) {
              Map<String, Object> result =
                  ElasticSearchUtil.getDataByIdentifier(
                      ProjectUtil.EsIndex.sunbird.getIndexName(),
                      ProjectUtil.EsType.user.getTypeName(),
                      userId);
              // check whether is_deletd true or false
              if ((ProjectUtil.isNull(result))
                  || (ProjectUtil.isNotNull(result) && result.isEmpty())
                  || (ProjectUtil.isNotNull(result)
                      && result.containsKey(JsonKey.IS_DELETED)
                      && ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))
                      && (Boolean) result.get(JsonKey.IS_DELETED))) {
                throw new ProjectCommonException(
                    ResponseCode.invalidUserId.getErrorCode(),
                    ResponseCode.invalidUserId.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
              }
              dnMentorsValue.add(userId);
            }
          }
        }
        req.put(JsonKey.MENTORS, dnMentorsValue);
      }

      Response result =
          cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), req);
      sender().tell(result, self());

      targetObject =
          TelemetryUtil.generateTargetObject(
              (String) req.get(JsonKey.ID), JsonKey.BATCH, JsonKey.UPDATE, null);
      TelemetryUtil.telemetryProcessingCall(req, targetObject, correlatedObject);

      Map<String, String> rollUp = new HashMap<>();
      rollUp.put("l1", (String) res.get(JsonKey.COURSE_ID));
      TelemetryUtil.addTargetObjectRollUp(rollUp, targetObject);

      if (((String) result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
        ProjectLogger.log("method call going to satrt for ES--.....");
        Request request = new Request();
        request.setOperation(ActorOperations.UPDATE_COURSE_BATCH_ES.getValue());
        request.getRequest().put(JsonKey.BATCH, req);
        ProjectLogger.log("making a call to save Course Batch data to ES");
        try {
          tellToAnother(request);
        } catch (Exception ex) {
          ProjectLogger.log(
              "Exception Occurred during saving Course Batch to Es while updating Course Batch : ",
              ex);
        }
      } else {
        ProjectLogger.log("no call for ES to save Course Batch");
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  /**
   * THis method will do the organization validation.
   *
   * @param orgId String
   * @return boolean
   */
  private boolean isOrgValid(String orgId) {
    Map<String, Object> resp =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(),
            orgId);
    if (resp != null && resp.size() > 0) {
      ProjectLogger.log("organisation found in ES with id ==" + orgId);
      return true;
    }
    ProjectLogger.log("organisation not found in ES with id ==" + orgId);
    return false;
  }

  private void insertUserCoursesToES(Map<String, Object> courseMap) {
    Request request = new Request();
    request.setOperation(ActorOperations.INSERT_USR_COURSES_INFO_ELASTIC.getValue());
    request.getRequest().put(JsonKey.USER_COURSES, courseMap);
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occurred during saving user count to Es : ", ex);
    }
  }
}
