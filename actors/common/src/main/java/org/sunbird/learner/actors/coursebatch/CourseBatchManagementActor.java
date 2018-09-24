package org.sunbird.learner.actors.coursebatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.impl.InterServiceCommunicationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.ProgressStatus;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.actors.CourseEnrollmentActor;
import org.sunbird.learner.actors.coursebatch.dao.CourseBatchDao;
import org.sunbird.learner.actors.coursebatch.dao.impl.CourseBatchDaoImpl;
import org.sunbird.learner.util.CourseBatchSchedulerUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.course.batch.CourseBatch;
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

  private CourseBatchDao courseBatchDao = CourseBatchDaoImpl.getInstance();
  private Util.DbInfo dbInfo = null;
  /**
   * Receives the actor message and perform the course enrollment operation .
   *
   * @param request Object is an instance of Request
   */
  @Override
  public void onReceive(Request request) throws Throwable {
    dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);

    Util.initializeContext(request, TelemetryEnvKey.BATCH);
    // set request id to thread local...
    ExecutionContext.setRequestId(request.getRequestId());

    String requestedOperation = request.getOperation();
    switch (requestedOperation) {
      case "createBatch":
        createCourseBatch(request);
        break;
      case "updateBatch":
        updateCourseBatch(request);
        break;
      case "getBatch":
        getCourseBatch(request);
        break;
      case "addUserBatch":
        addUserCourseBatch(request);
        break;
      case "getCourseBatchDetail":
        getCourseBatchDetail(request);
        break;
      default:
        onReceiveUnsupportedOperation(request.getOperation());
        break;
    }
  }
  /**
   * This method will create course under cassandra db.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void createCourseBatch(Request actorMessage) {
    Map<String, Object> req = actorMessage.getRequest();
    Map<String, Object> targetObject;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    req = setDataToReq(req, actorMessage);
    List<String> participants = (List<String>) req.get(JsonKey.PARTICIPANTS);
    req.remove(JsonKey.PARTICIPANTS);
    Response result = courseBatchDao.create(req);
    createDataToEs((String) req.get(JsonKey.ID), req);
    result.put(JsonKey.BATCH_ID, req.get(JsonKey.ID));
    if (participants != null) {
      addParticipants(participants, req);
    }
    sender().tell(result, self());

    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) req.get(JsonKey.ID), JsonKey.BATCH, JsonKey.CREATE, null);
    TelemetryUtil.generateCorrelatedObject(
        (String) req.get(JsonKey.COURSE_ID), JsonKey.COURSE, null, correlatedObject);
    TelemetryUtil.telemetryProcessingCall(req, targetObject, correlatedObject);

    Map<String, String> rollUp = new HashMap<>();
    rollUp.put("l1", (String) req.get(JsonKey.COURSE_ID));
    TelemetryUtil.addTargetObjectRollUp(rollUp, targetObject);

    if (((String) result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      insertCourseBatchToEs(req);
    } else {
      ProjectLogger.log("no call for ES to save Course Batch");
    }
  }

  private void createDataToEs(String uniqueId, Map<String, Object> req) {
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
  }

  public Map<String, Object> setDataToReq(Map<String, Object> req, Request actorMessage) {
    Map<String, String> headers =
        (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    Map<String, Object> ekStepContent = getEkStepContent(courseId, headers);
    String enrolmentType = (String) req.get(JsonKey.ENROLLMENT_TYPE);
    validateContentOrg(req);
    if (req.containsKey(JsonKey.MENTORS)) validateMentors((List<String>) req.get(JsonKey.MENTORS));
    req.remove(JsonKey.PARTICIPANT);
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    String courseCreator = (String) ekStepContent.get(JsonKey.CREATED_BY);
    String createdBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
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
    return req;
  }

  private void insertCourseBatchToEs(Map<String, Object> req) {
    Request request = new Request();
    request.setOperation(ActorOperations.INSERT_COURSE_BATCH_ES.getValue());
    request.getRequest().put(JsonKey.BATCH, req);
    ProjectLogger.log(
        "CourseBatchManagementActor:createCourseBatch making a call to save Course Batch data to ES.",
        LoggerEnum.INFO.name());
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log(
          "Exception Occurred during saving Course Batch to Es while creating Course Batch : ", ex);
    }
  }

  private void validateMentors(List<String> mentors) {
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

  private void addParticipants(List<String> participants, Map<String, Object> req) {
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    String batchId = (String) req.get(JsonKey.ID);

    CourseBatch courseBatchObject = courseBatchDao.readById(batchId);
    String batchCreator = courseBatchObject.getCreatedBy();
    if (StringUtils.isBlank(batchCreator)) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseCreatorId.getErrorCode(),
          ResponseCode.invalidCourseCreatorId.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    validateCourseBatchData(courseBatchObject);
    String batchCreatorRootOrgId = getRootOrg(batchCreator);
    Map<String, Boolean> dbParticipants = courseBatchObject.getParticipant();
    if (dbParticipants == null) {
      dbParticipants = new HashMap();
    }
    Map<String, String> participantWithRootOrgIds = getRootOrgForMultipleUsers(participants);
    Map<String, Boolean> finalParticipants = new HashMap<>();
    for (String userId : participants) {
      if (!(dbParticipants.containsKey(userId))) {
        if (!participantWithRootOrgIds.containsKey(userId)
            || (!batchCreatorRootOrgId.equals(participantWithRootOrgIds.get(userId)))) {
          continue;
        }
        finalParticipants.put(
            userId,
            addUserCourses(
                batchId,
                courseBatchObject.getCourseId(),
                userId,
                (courseBatchObject.getCourseAdditionalInfo())));
        targetObject =
            TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        correlatedObject = new ArrayList<>();
        TelemetryUtil.generateCorrelatedObject(batchId, JsonKey.BATCH, null, correlatedObject);
        TelemetryUtil.telemetryProcessingCall(req, targetObject, correlatedObject);
      } else {
        finalParticipants.put(userId, dbParticipants.get(userId));
        dbParticipants.remove(userId);
      }
    }
    if (!dbParticipants.isEmpty()) {
      // Add code to remove user from course;
      InterServiceCommunicationImpl interServiceCommunication = new InterServiceCommunicationImpl();
      dbParticipants.forEach(
          (s, aBoolean) -> {
            Request request = new Request();
            request.setOperation(ActorOperations.UNENROLL_COURSE.getValue());
            Map<String, Object> map = new HashMap<>();
            map.put(JsonKey.USER_ID, s);
            map.put(JsonKey.BATCH_ID, batchId);
            map.put(JsonKey.COURSE_ID, req.get(JsonKey.COURSE_ID));
            request.setRequest(map);
            interServiceCommunication.getResponse(
                getActorRef(ActorOperations.UNENROLL_COURSE.getValue()), request);
          });
    }
    courseBatchObject.setParticipant(finalParticipants);
    courseBatchDao.update(new ObjectMapper().convertValue(courseBatchObject, Map.class));
    updateEs(courseBatchObject);
  }

  private void updateEs(CourseBatch courseBatch) {
    ProjectLogger.log("method call going to satrt for ES--.....");
    Request request = new Request();
    request.setOperation(ActorOperations.UPDATE_COURSE_BATCH_ES.getValue());
    request
        .getRequest()
        .put(JsonKey.BATCH, new ObjectMapper().convertValue(courseBatch, Map.class));
    ProjectLogger.log("making a call to save Course Batch data to ES");
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log(
          "Exception Occurred during saving Course Batch to Es while updating Course Batch : ", ex);
    }
  }

  private void validateCourseBatchData(CourseBatch courseBatchObject) {
    if (ProjectUtil.isNull(courseBatchObject.getCreatedFor())
        || courseBatchObject.getCreatedFor().isEmpty()) {
      // throw exception since batch does not belong to any createdfor in DB ...
      throw new ProjectCommonException(
          ResponseCode.courseCreatedForIsNull.getErrorCode(),
          ResponseCode.courseCreatedForIsNull.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    if (ProjectUtil.isNull(courseBatchObject.getEnrollmentType())
        || !(courseBatchObject.getEnrollmentType()).equalsIgnoreCase(JsonKey.INVITE_ONLY)) {
      throw new ProjectCommonException(
          ResponseCode.enrollmentTypeValidation.getErrorCode(),
          ResponseCode.enrollmentTypeValidation.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  /**
   * This method will allow user to update the course batch details.
   *
   * @param actorMessage Request
   */
  @SuppressWarnings("unchecked")
  private void updateCourseBatch(Request actorMessage) {
    Map<String, Object> targetObject = null;

    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    Map<String, Object> req = setUpdateReq(actorMessage);
    CourseBatch courseBatch = courseBatchDao.readById((String) req.get(JsonKey.ID));
    req = updateReq(req, (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY), courseBatch);
    List<String> participants = (List<String>) req.get(JsonKey.PARTICIPANTS);
    req.remove(JsonKey.PARTICIPANTS);
    Response result = courseBatchDao.update(req);

    if (participants != null) {
      addParticipants(participants, req);
    }
    sender().tell(result, self());
    req.put(JsonKey.ENROLLMENT_TYPE, courseBatch.getEnrollmentType());
    req.put(JsonKey.COURSE_ID, courseBatch.getCourseId());
    req.put(JsonKey.HASHTAGID, courseBatch.getHashtagId());
    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) req.get(JsonKey.ID), JsonKey.BATCH, JsonKey.UPDATE, null);
    TelemetryUtil.telemetryProcessingCall(req, targetObject, correlatedObject);

    Map<String, String> rollUp = new HashMap<>();
    rollUp.put("l1", courseBatch.getCourseId());
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
  }

  private Map<String, Object> updateReq(
      Map<String, Object> req, String requestedBy, CourseBatch courseBatch) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Map<String, Object> courseBatchMap = new ObjectMapper().convertValue(courseBatch, Map.class);
    Date todayDate = getDate(null, format, null);
    Date dbBatchStartDate = getDate(JsonKey.START_DATE, format, courseBatchMap);
    Date dbBatchEndDate = getDate(JsonKey.END_DATE, format, courseBatchMap);
    Date requestedStartDate = getDate(JsonKey.START_DATE, format, req);
    Date requestedEndDate = getDate(JsonKey.END_DATE, format, req);

    validateUpdateBatchStartDate(requestedStartDate);
    validateBatchStartAndEndDate(
        dbBatchStartDate, dbBatchEndDate, requestedStartDate, requestedEndDate, todayDate);
    validateUserPermission(courseBatchMap, requestedBy);
    String enrolmentType = "";
    if (req.containsKey(JsonKey.ENROLMENTTYPE)) {
      enrolmentType = (String) req.get(JsonKey.ENROLMENTTYPE);
    } else {
      enrolmentType = courseBatch.getEnrollmentType();
    }

    List<String> createdFor = new ArrayList<>();
    if (req.containsKey(JsonKey.COURSE_CREATED_FOR)) {
      createdFor = (List<String>) req.get(JsonKey.COURSE_CREATED_FOR);
      validateContentOrg(req);
    }
    List<String> dbValueCreatedFor = courseBatch.getCreatedFor();
    if (ProjectUtil.EnrolmentType.inviteOnly.getVal().equalsIgnoreCase(enrolmentType)) {
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
    } else {
      for (String orgId : createdFor) {
        if (!dbValueCreatedFor.contains(orgId)) {
          dbValueCreatedFor.add(orgId);
        }
      }
    }
    req.put(JsonKey.COURSE_CREATED_FOR, dbValueCreatedFor);
    if (req.containsKey(JsonKey.MENTORS)) {
      validateMentors((List<String>) req.get(JsonKey.MENTORS));
    }

    if (null != requestedStartDate && todayDate.equals(requestedStartDate)) {
      req.put(JsonKey.STATUS, ProgressStatus.STARTED.getValue());
      CourseBatchSchedulerUtil.updateCourseBatchDbStatus(req, true);
    }
    return req;
  }

  private Map<String, Object> setUpdateReq(Request actorMessage) {

    Map<String, Object> req = actorMessage.getRequest();
    // objects of telemetry event...

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
    return req;
  }

  private void validateUserPermission(Map<String, Object> res, String requestedBy) {
    if (!(requestedBy.equalsIgnoreCase((String) res.get(JsonKey.CREATED_BY))
        || ((Map<String, Boolean>) res.get(JsonKey.PARTICIPANT)).containsKey(requestedBy))) {
      throw new ProjectCommonException(
          ResponseCode.unAuthorized.getErrorCode(),
          ResponseCode.unAuthorized.getErrorMessage(),
          ResponseCode.unAuthorized.getResponseCode());
    }
  }

  private void addUserCourseBatch(Request actorMessage) {

    ProjectLogger.log("Add user to course batch - called");
    Map<String, Object> req = actorMessage.getRequest();
    Response response = new Response();

    // objects of telemetry event...
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    String batchId = (String) req.get(JsonKey.BATCH_ID);
    TelemetryUtil.generateCorrelatedObject(batchId, JsonKey.BATCH, null, correlatedObject);

    CourseBatch courseBatch = courseBatchDao.readById(batchId);
    Map<String, Object> courseBatchObject = new ObjectMapper().convertValue(courseBatch, Map.class);
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
    courseBatchDao.update(courseBatchObject);
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

  private void getCourseBatch(Request actorMessage) {
    Map<String, Object> result =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.course.getTypeName(),
            (String) actorMessage.getContext().get(JsonKey.BATCH_ID));
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

    //    Map<String, Object> req = (Map<String, Object>)
    // actorMessage.getRequest().get(JsonKey.BATCH);
    String batchId = (String) actorMessage.getContext().get(JsonKey.BATCH_ID);
    CourseBatch courseBatch = courseBatchDao.readById(batchId);
    List<Map<String, Object>> courseBatchList = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    courseBatchList.add(mapper.convertValue(courseBatch, Map.class));
    Response result = new Response();
    result.getResult().put(JsonKey.RESPONSE, courseBatchList);

    sender().tell(result, self());
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
      courseBatchDao.createCourseEnrolment(userCourses);
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

  private void validateUpdateBatchStartDate(Date startDate) {
    if (startDate != null) {
      try {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.format(startDate);
      } catch (Exception e) {
        throw new ProjectCommonException(
            ResponseCode.dateFormatError.getErrorCode(),
            ResponseCode.dateFormatError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      throw new ProjectCommonException(
          ResponseCode.courseBatchStartDateRequired.getErrorCode(),
          ResponseCode.courseBatchStartDateRequired.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private void validateBatchStartAndEndDate(
      Date existingStartDate,
      Date existingEndDate,
      Date requestedStartDate,
      Date requestedEndDate,
      Date todayDate) {

    Date startDate = requestedStartDate != null ? requestedStartDate : existingStartDate;
    Date endDate = requestedEndDate != null ? requestedEndDate : existingEndDate;

    ProjectLogger.log(
        "existingStartDate, existingEndDate, requestedStartDate, requestedEndDate, todaydate"
            + existingStartDate
            + ","
            + existingEndDate
            + ","
            + requestedStartDate
            + ","
            + requestedEndDate
            + ","
            + todayDate,
        LoggerEnum.INFO.name());

    if ((existingStartDate.before(todayDate) || existingStartDate.equals(todayDate))
        && !(existingStartDate.equals(requestedStartDate))) {

      ProjectLogger.log("validateBatchStartAndEndDate: senario1: ", LoggerEnum.INFO.name());
      throw new ProjectCommonException(
          ResponseCode.invalidBatchStartDateError.getErrorCode(),
          ResponseCode.invalidBatchStartDateError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    if ((requestedStartDate.before(todayDate)) && !requestedStartDate.equals(existingStartDate)) {

      ProjectLogger.log("validateBatchStartAndEndDate: senario2: ", LoggerEnum.INFO.name());
      throw new ProjectCommonException(
          ResponseCode.invalidBatchStartDateError.getErrorCode(),
          ResponseCode.invalidBatchStartDateError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    if (startDate.after(endDate)) {

      ProjectLogger.log("validateBatchStartAndEndDate: senario3: ", LoggerEnum.INFO.name());
      throw new ProjectCommonException(
          ResponseCode.invalidBatchEndDateError.getErrorCode(),
          ResponseCode.invalidBatchEndDateError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    if ((endDate != null && !endDate.after(todayDate))
        || (existingEndDate != null && !existingEndDate.after(todayDate))) {

      ProjectLogger.log("validateBatchStartAndEndDate: senario4: ", LoggerEnum.INFO.name());
      throw new ProjectCommonException(
          ResponseCode.courseBatchEndDateError.getErrorCode(),
          ResponseCode.courseBatchEndDateError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private Date getDate(String key, SimpleDateFormat format, Map<String, Object> map) {
    try {
      if (MapUtils.isEmpty(map)) {
        return format.parse(format.format(new Date()));
      } else {
        if (StringUtils.isNotBlank((String) map.get(key))) {
          return format.parse((String) map.get(key));
        } else {
          return null;
        }
      }
    } catch (ParseException e) {

      ProjectLogger.log(
          "CourseBatchManagementActor:getDate: Exception occurred with message = " + e.getMessage(),
          e);
    }
    return null;
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

  private Map<String, Object> getEkStepContent(String courseId, Map<String, String> headers) {
    Map<String, Object> ekStepContent =
        CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, headers);
    if (null == ekStepContent || ekStepContent.size() == 0) {
      ProjectLogger.log("Course Id not found in EkStep===" + courseId, LoggerEnum.INFO.name());
      throw new ProjectCommonException(
          ResponseCode.invalidCourseId.getErrorCode(),
          ResponseCode.invalidCourseId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return ekStepContent;
  }

  private void validateContentOrg(Map<String, Object> req) {
    List<String> createdFor = new ArrayList<>();
    if (req.containsKey(JsonKey.COURSE_CREATED_FOR)) {
      createdFor = (List<String>) req.get(JsonKey.COURSE_CREATED_FOR);
    }
    for (String orgId : createdFor) {
      if (!isOrgValid(orgId)) {
        throw new ProjectCommonException(
            ResponseCode.invalidOrgId.getErrorCode(),
            ResponseCode.invalidOrgId.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
  }
}
