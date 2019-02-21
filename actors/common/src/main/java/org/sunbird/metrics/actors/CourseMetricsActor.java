package org.sunbird.metrics.actors;

import static org.sunbird.common.models.util.ProjectUtil.isNotNull;
import static org.sunbird.common.models.util.ProjectUtil.isNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.ReportTrackingStatus;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;

@ActorConfig(
  tasks = {
    "courseProgressMetrics",
    "courseConsumptionMetrics",
    "courseProgressMetricsV2",
    "courseProgressMetricsReport",
    "courseConsumptionMetricsReport"
  },
  asyncTasks = {}
)
public class CourseMetricsActor extends BaseMetricsActor {

  private static final String COURSE_PROGRESS_REPORT = "Course Progress Report";
  protected static final String CONTENT_ID = "content_id";
  private static ObjectMapper mapper = new ObjectMapper();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private DecryptionService decryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(
          null);

  @Override
  public void onReceive(Request request) throws Throwable {
    String requestedOperation = request.getOperation();
    switch (requestedOperation) {
      case "courseProgressMetrics":
        courseProgressMetrics(request);
        break;
      case "courseConsumptionMetrics":
        courseConsumptionMetrics(request);
        break;
      case "courseProgressMetricsV2":
        courseProgressMetricsV2(request);
        break;
      case "courseProgressMetricsReport":
        courseProgressMetricsReport(request);
        break;
      default:
        onReceiveUnsupportedOperation(request.getOperation());
        break;
    }
  }

  private void courseProgressMetricsV2(Request actorMessage) {
    ProjectLogger.log("CourseMetricsActor: courseProgressMetrics called.", LoggerEnum.INFO.name());
    Integer limit = (Integer) actorMessage.getContext().get(JsonKey.LIMIT);
    String sortBy = (String) actorMessage.getContext().get(JsonKey.SORTBY);
    String batchId = (String) actorMessage.getContext().get(JsonKey.BATCH_ID);
    Integer offset = (Integer) actorMessage.getContext().get(JsonKey.OFFSET);
    String userName = (String) actorMessage.getContext().get(JsonKey.USERNAME);
    String sortOrder = (String) actorMessage.getContext().get(JsonKey.SORT_ORDER);

    String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
    validateUserId(requestedBy);
    Map<String, Object> courseBatchResult = validateAndGetCourseBatch(batchId);
    int leafNodeCount = 0;
    Map<String, Object> tempMap =
        (Map<String, Object>) courseBatchResult.get(JsonKey.COURSE_ADDITIONAL_INFO);
    if (!MapUtils.isEmpty(tempMap)) {
      String leafCount = (String) tempMap.get(JsonKey.LEAF_NODE_COUNT);
      if (!StringUtils.isEmpty(leafCount) && StringUtils.isNumeric(leafCount)) {
        leafNodeCount = Integer.parseInt(leafCount);
      }
    }
    Map<String, Object> filter = new HashMap<>();
    filter.put(JsonKey.BATCHES + "." + JsonKey.BATCH_ID, batchId);

    SearchDTO searchDTO = new SearchDTO();
    if (!StringUtils.isEmpty(userName)) {
      Map<String, Object> internalMap = new HashMap<>();
      internalMap.put(JsonKey.FIRST_NAME, userName);
      filter.put("startsWith", internalMap);
    }
    searchDTO.setLimit(limit);
    searchDTO.setOffset(offset);
    if (!StringUtils.isEmpty(sortBy)) {
      Map<String, String> sortMap = new HashMap<>();
      if (JsonKey.USERNAME.equalsIgnoreCase(sortBy)) {
        sortBy = JsonKey.FIRST_NAME;
      }
      if (JsonKey.PROGRESS.equalsIgnoreCase(sortBy)) {
        sortBy = JsonKey.BATCHES + "." + JsonKey.PROGRESS;
      }
      if (JsonKey.ENROLLED_ON.equalsIgnoreCase(sortBy)) {
        sortBy = JsonKey.BATCHES + "." + JsonKey.ENROLLED_ON;
      }
      if (StringUtils.isEmpty(sortOrder)) {
        sortMap.put(sortBy, JsonKey.ASC);
      } else {
        sortMap.put(sortBy, sortOrder);
      }
      searchDTO.setSortBy(sortMap);
    }
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filter);

    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDTO, ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());
    if (isNull(result) || result.size() == 0) {
      ProjectLogger.log(
          "CourseMetricsActor:courseProgressMetricsV2: No search results found.",
          LoggerEnum.INFO.name());
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidCourseBatchId);
    }

    List<Map<String, Object>> esContents = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
    Map<String, Object> courseProgressResult = new HashMap<>();
    List<Map<String, Object>> userData = new ArrayList<>();
    for (Map<String, Object> esContent : esContents) {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.USER_NAME, (String) esContent.get(JsonKey.FIRST_NAME));
      String phone = null;
      if (esContent.containsKey(JsonKey.ENC_PHONE)) {
        phone = decryptAndMaskPhone((String) esContent.get(JsonKey.ENC_PHONE));
      }
      map.put(JsonKey.PHONE, phone);
      map.put(JsonKey.ORG_NAME, esContent.get(JsonKey.ROOT_ORG_NAME));
      for (Map<String, Object> batchMap :
          (List<Map<String, Object>>) esContent.get(JsonKey.BATCHES)) {
        if (batchId.equalsIgnoreCase((String) batchMap.get(JsonKey.BATCH_ID))) {
          calculateCourseProgressPercentage(batchMap, leafNodeCount);
          formatEnrolledOn(batchMap);
          map.putAll(batchMap);
        }
      }
      userData.add(map);
    }

    courseProgressResult.put(JsonKey.COUNT, result.get(JsonKey.COUNT));
    courseProgressResult.put(JsonKey.DATA, userData);
    courseProgressResult.put(JsonKey.START_DATE, courseBatchResult.get(JsonKey.START_DATE));
    courseProgressResult.put(JsonKey.END_DATE, courseBatchResult.get(JsonKey.END_DATE));
    courseProgressResult.put(JsonKey.COMPLETED_COUNT, getCompletedCount(batchId, leafNodeCount));
    Response response = new Response();
    response.put("response", "SUCCESS");
    response.getResult().putAll(courseProgressResult);
    sender().tell(response, self());
  }

  private String decryptAndMaskPhone(String phone) {
    return UserUtility.maskEmailOrPhone(phone, JsonKey.PHONE);
  }

  private void formatEnrolledOn(Map<String, Object> batchMap) {
    String timeStamp = (String) batchMap.get(JsonKey.LAST_ACCESSED_ON);
    try {
      SimpleDateFormat sdf = new SimpleDateFormat(ProjectUtil.ELASTIC_DATE_FORMAT);
      Date parsedDate = sdf.parse(timeStamp);
      batchMap.put(JsonKey.LAST_ACCESSED_ON, ProjectUtil.formatDate(parsedDate));
    } catch (Exception e) {
      ProjectLogger.log("formatEnrolledOn : " + e.getMessage(), LoggerEnum.INFO);
    }
  }

  private int getCompletedCount(String batchId, int leafNodeCount) {
    SearchDTO searchDTO = new SearchDTO();
    Map<String, Object> filter = new HashMap<>();
    filter.put(JsonKey.BATCHES + "." + JsonKey.BATCH_ID, batchId);
    filter.put(JsonKey.BATCHES + "." + JsonKey.PROGRESS, leafNodeCount);
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filter);

    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDTO, ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());

    if (isNull(result) || result.size() == 0) {
      ProjectLogger.log(
          "CourseMetricsActor:getCompletedCount: No search results found.", LoggerEnum.INFO.name());
      return 0;
    } else {
      List<Map<String, Object>> esBatchResult =
          (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
      int count = 0;
      if (!CollectionUtils.isEmpty(esBatchResult)) {
        for (Map<String, Object> esContent : esBatchResult) {
          List<Map<String, Object>> batches =
              (List<Map<String, Object>>) esContent.get(JsonKey.BATCHES);
          if (!CollectionUtils.isEmpty(batches)) {
            for (Map<String, Object> batchMap : batches) {
              if (batchId.equalsIgnoreCase((String) batchMap.get(JsonKey.BATCH_ID))
                  && leafNodeCount == (Integer) batchMap.get(JsonKey.PROGRESS)) {
                count++;
              }
            }
          }
        }
      }
      ProjectLogger.log(
          "CourseMetricsActor:getCompletedCount: search results found."
              + result.get(JsonKey.COUNT)
              + " LeafNodeCount = "
              + leafNodeCount,
          LoggerEnum.INFO.name());
      return count;
    }
  }

  private Map<String, Object> validateAndGetCourseBatch(String batchId) {
    if (StringUtils.isBlank(batchId)) {
      ProjectLogger.log(
          "CourseMetricsActor:validateAndGetCourseBatch: batchId is invalid (blank).",
          LoggerEnum.INFO.name());
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidCourseBatchId);
    }
    // check batch exist in ES or not
    Map<String, Object> courseBatchResult =
        ElasticSearchUtil.getDataByIdentifier(
            EsIndex.sunbird.getIndexName(), EsType.course.getTypeName(), batchId);
    if (isNull(courseBatchResult) || courseBatchResult.size() == 0) {
      ProjectLogger.log(
          "CourseMetricsActor:validateAndGetCourseBatch: batchId not found.",
          LoggerEnum.INFO.name());
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidCourseBatchId);
    }
    return courseBatchResult;
  }

  private void validateUserId(String requestedBy) {

    Map<String, Object> requestedByInfo =
        ElasticSearchUtil.getDataByIdentifier(
            EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), requestedBy);
    if (isNull(requestedByInfo)
        || StringUtils.isBlank((String) requestedByInfo.get(JsonKey.FIRST_NAME))) {
      throw new ProjectCommonException(
          ResponseCode.invalidUserId.getErrorCode(),
          ResponseCode.invalidUserId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private void courseProgressMetricsReport(Request actorMessage) {

    ProjectLogger.log(
        "CourseMetricsActor: courseProgressMetricsReport called.", LoggerEnum.INFO.name());
    SimpleDateFormat simpleDateFormat = ProjectUtil.getDateFormatter();
    simpleDateFormat.setLenient(false);

    String requestedBy = (String) actorMessage.get(JsonKey.REQUESTED_BY);

    Map<String, Object> requestedByInfo =
        ElasticSearchUtil.getDataByIdentifier(
            EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), requestedBy);
    if (isNull(requestedByInfo)
        || StringUtils.isBlank((String) requestedByInfo.get(JsonKey.FIRST_NAME))) {
      throw new ProjectCommonException(
          ResponseCode.invalidUserId.getErrorCode(),
          ResponseCode.invalidUserId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    if (StringUtils.isBlank((String) requestedByInfo.get(JsonKey.ENC_EMAIL))) {
      ProjectCommonException.throwClientErrorException(ResponseCode.emailRequired);
    }

    String batchId = (String) actorMessage.getRequest().get(JsonKey.BATCH_ID);
    if (StringUtils.isBlank(batchId)) {
      ProjectLogger.log(
          "CourseMetricsActor:courseProgressMetricsReport: batchId is invalid (blank).",
          LoggerEnum.INFO.name());
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidCourseBatchId.getErrorCode(),
              ResponseCode.invalidCourseBatchId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check batch exist in ES or not
    Map<String, Object> courseBatchResult =
        ElasticSearchUtil.getDataByIdentifier(
            EsIndex.sunbird.getIndexName(), EsType.course.getTypeName(), batchId);
    if (isNull(courseBatchResult) || courseBatchResult.size() == 0) {
      ProjectLogger.log(
          "CourseMetricsActor:courseProgressMetricsReport: batchId not found.",
          LoggerEnum.INFO.name());
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidCourseBatchId.getErrorCode(),
              ResponseCode.invalidCourseBatchId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Util.DbInfo reportTrackingdbInfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);

    String requestId = ProjectUtil.getUniqueIdFromTimestamp(1);

    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    String fileFormat = (String) actorMessage.getRequest().get(JsonKey.FORMAT);

    Map<String, Object> requestDbInfo = new HashMap<>();
    requestDbInfo.put(JsonKey.ID, requestId);
    requestDbInfo.put(JsonKey.USER_ID, requestedBy);
    requestDbInfo.put(JsonKey.FIRST_NAME, requestedByInfo.get(JsonKey.FIRST_NAME));
    requestDbInfo.put(JsonKey.STATUS, ReportTrackingStatus.NEW.getValue());
    requestDbInfo.put(JsonKey.RESOURCE_ID, batchId);
    requestDbInfo.put(JsonKey.PERIOD, periodStr);
    requestDbInfo.put(JsonKey.CREATED_DATE, simpleDateFormat.format(new Date()));
    requestDbInfo.put(JsonKey.UPDATED_DATE, simpleDateFormat.format(new Date()));
    String decryptedEmail =
        decryptionService.decryptData((String) requestedByInfo.get(JsonKey.ENC_EMAIL));
    requestDbInfo.put(JsonKey.EMAIL, decryptedEmail);
    requestDbInfo.put(JsonKey.TYPE, COURSE_PROGRESS_REPORT);
    requestDbInfo.put(JsonKey.FORMAT, fileFormat);
    requestDbInfo.put(JsonKey.RESOURCE_NAME, getCourseNameFromBatch(courseBatchResult));

    cassandraOperation.insertRecord(
        reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(), requestDbInfo);

    Response response = new Response();
    response.put(JsonKey.REQUEST_ID, requestId);
    sender().tell(response, self());

    // assign the back ground task to background job actor ...
    Request backGroundRequest = new Request();
    backGroundRequest.setOperation(ActorOperations.PROCESS_DATA.getValue());
    backGroundRequest.getRequest().put(JsonKey.REQUEST, JsonKey.CourseProgress);
    backGroundRequest.getRequest().put(JsonKey.REQUEST_ID, requestId);
    tellToAnother(backGroundRequest);
  }

  private String getCourseNameFromBatch(Map<String, Object> courseBatchResult) {

    String courseName = null;
    if (courseBatchResult.get(JsonKey.COURSE_ADDITIONAL_INFO) != null
        && courseBatchResult.get(JsonKey.COURSE_ADDITIONAL_INFO) instanceof Map) {
      Map<String, String> map =
          (Map<String, String>) courseBatchResult.get(JsonKey.COURSE_ADDITIONAL_INFO);
      courseName = map.get(JsonKey.COURSE_NAME);
    }
    return courseName;
  }

  @SuppressWarnings("unchecked")
  private void courseProgressMetrics(Request actorMessage) {
    ProjectLogger.log("CourseMetricsActor: courseProgressMetrics called.", LoggerEnum.INFO.name());
    Request request = new Request();
    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    String batchId = (String) actorMessage.getRequest().get(JsonKey.BATCH_ID);

    String requestedBy = (String) actorMessage.get(JsonKey.REQUESTED_BY);

    Map<String, Object> requestedByInfo =
        ElasticSearchUtil.getDataByIdentifier(
            EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), requestedBy);
    if (isNull(requestedByInfo)
        || StringUtils.isBlank((String) requestedByInfo.get(JsonKey.FIRST_NAME))) {
      throw new ProjectCommonException(
          ResponseCode.invalidUserId.getErrorCode(),
          ResponseCode.invalidUserId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    if (StringUtils.isBlank(batchId)) {
      ProjectLogger.log(
          "CourseMetricsActor:courseProgressMetrics: batchId is invalid (blank).",
          LoggerEnum.INFO.name());
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidCourseBatchId.getErrorCode(),
              ResponseCode.invalidCourseBatchId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check batch exist in ES or not
    Map<String, Object> courseBatchResult =
        ElasticSearchUtil.getDataByIdentifier(
            EsIndex.sunbird.getIndexName(), EsType.course.getTypeName(), batchId);
    if (isNull(courseBatchResult) || courseBatchResult.size() == 0) {
      ProjectLogger.log(
          "CourseMetricsActor:courseProgressMetrics: batchId not found.", LoggerEnum.INFO.name());
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidCourseBatchId.getErrorCode(),
              ResponseCode.invalidCourseBatchId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    // get start and end time ---
    Map<String, String> dateRangeFilter = new HashMap<>();

    request.setId(actorMessage.getId());
    request.setContext(actorMessage.getContext());
    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.PERIOD, periodStr);
    Map<String, Object> filter = new HashMap<>();
    filter.put(JsonKey.BATCH_ID, batchId);
    filter.put(JsonKey.ACTIVE, true);
    if (!("fromBegining".equalsIgnoreCase(periodStr))) {
      Map<String, String> dateRange = getDateRange(periodStr);
      dateRangeFilter.put(GTE, (String) dateRange.get(STARTDATE));
      dateRangeFilter.put(
          LTE, ((String) dateRange.get(ENDDATE)) + JsonKey.END_TIME_IN_HOUR_MINUTE_SECOND);
      ProjectLogger.log(
          "CourseMetricsActor:courseProgressMetrics Date range is : " + dateRangeFilter,
          LoggerEnum.INFO.name());
      filter.put(JsonKey.DATE_TIME, dateRangeFilter);
    }

    List<String> coursefields = new ArrayList<>();
    coursefields.add(JsonKey.USER_ID);
    coursefields.add(JsonKey.PROGRESS);
    coursefields.add(JsonKey.COURSE_ENROLL_DATE);
    coursefields.add(JsonKey.BATCH_ID);
    coursefields.add(JsonKey.DATE_TIME);
    coursefields.add(JsonKey.LEAF_NODE_COUNT);
    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            createESRequest(filter, null, coursefields),
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            EsType.usercourses.getTypeName());
    List<Map<String, Object>> esContent = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);

    if (CollectionUtils.isNotEmpty(esContent)) {
      List<String> userIds = new ArrayList<>();

      calculateCourseProgressPercentage(esContent);
      for (Map<String, Object> entry : esContent) {
        String userId = (String) entry.get(JsonKey.USER_ID);
        userIds.add(userId);
      }

      Set<String> uniqueUserIds = new HashSet<>(userIds);
      Map<String, Object> userfilter = new HashMap<>();
      userfilter.put(JsonKey.USER_ID, uniqueUserIds.stream().collect(Collectors.toList()));
      List<String> userfields = new ArrayList<>();
      userfields.add(JsonKey.USER_ID);
      userfields.add(JsonKey.USERNAME);
      userfields.add(JsonKey.ROOT_ORG_ID);
      userfields.add(JsonKey.FIRST_NAME);
      userfields.add(JsonKey.LAST_NAME);
      Map<String, Object> userresult =
          ElasticSearchUtil.complexSearch(
              createESRequest(userfilter, null, userfields),
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              EsType.user.getTypeName());
      List<Map<String, Object>> useresContent =
          (List<Map<String, Object>>) userresult.get(JsonKey.CONTENT);

      Map<String, Map<String, Object>> userInfoCache = new HashMap<>();
      Set<String> orgSet = new HashSet<>();
      for (Map<String, Object> map : useresContent) {
        String userId = (String) map.get(JsonKey.USER_ID);
        map.put("user", userId);
        map.put(
            JsonKey.USERNAME, decryptionService.decryptData((String) map.get(JsonKey.USERNAME)));
        String registerdOrgId = (String) map.get(JsonKey.ROOT_ORG_ID);
        if (isNotNull(registerdOrgId)) {
          orgSet.add(registerdOrgId);
        }
        userInfoCache.put(userId, new HashMap<String, Object>(map));
        // remove the org info from user content bcoz it is not desired in the user info
        // result
        map.remove(JsonKey.ROOT_ORG_ID);
        map.remove(JsonKey.USER_ID);
      }

      Map<String, Object> orgfilter = new HashMap<>();
      orgfilter.put(JsonKey.ID, orgSet.stream().collect(Collectors.toList()));
      List<String> orgfields = new ArrayList<>();
      orgfields.add(JsonKey.ID);
      orgfields.add(JsonKey.ORGANISATION_NAME);
      Map<String, Object> orgresult =
          ElasticSearchUtil.complexSearch(
              createESRequest(orgfilter, null, orgfields),
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              EsType.organisation.getTypeName());
      List<Map<String, Object>> orgContent =
          (List<Map<String, Object>>) orgresult.get(JsonKey.CONTENT);

      Map<String, String> orgInfoCache = new HashMap<>();
      for (Map<String, Object> map : orgContent) {

        String regOrgId = (String) map.get(JsonKey.ID);
        String regOrgName = (String) map.get(JsonKey.ORGANISATION_NAME);
        orgInfoCache.put(regOrgId, regOrgName);
      }

      Map<String, Object> batchFilter = new HashMap<>();
      batchFilter.put(JsonKey.ID, batchId);
      Map<String, Object> batchresult =
          ElasticSearchUtil.complexSearch(
              createESRequest(batchFilter, null, null),
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              EsType.course.getTypeName());
      List<Map<String, Object>> batchContent =
          (List<Map<String, Object>>) batchresult.get(JsonKey.CONTENT);

      Map<String, Map<String, Object>> batchInfoCache = new HashMap<>();
      for (Map<String, Object> map : batchContent) {
        String id = (String) map.get(JsonKey.ID);
        batchInfoCache.put(id, map);
      }
      for (Map<String, Object> map : esContent) {
        String userId = (String) map.get(JsonKey.USER_ID);
        map.put("user", userId);
        map.put("enrolledOn", map.get(JsonKey.COURSE_ENROLL_DATE));
        map.put("lastAccessTime", map.get(JsonKey.DATE_TIME));
        if (isNotNull(userInfoCache.get(userId))) {
          map.put(JsonKey.USERNAME, userInfoCache.get(userId).get(JsonKey.USERNAME));
          map.put(JsonKey.FIRST_NAME, userInfoCache.get(userId).get(JsonKey.FIRST_NAME));
          map.put(JsonKey.LAST_NAME, userInfoCache.get(userId).get(JsonKey.LAST_NAME));
          map.put("org", orgInfoCache.get(userInfoCache.get(userId).get(JsonKey.ROOT_ORG_ID)));
          if (isNotNull(batchInfoCache.get(map.get(JsonKey.BATCH_ID)))) {
            map.put(
                "batchEndsOn", batchInfoCache.get(map.get(JsonKey.BATCH_ID)).get(JsonKey.END_DATE));
          }
        } else {
          map.put(JsonKey.USERNAME, null);
          map.put("org", null);
          map.put("batchEndsOn", null);
        }
        map.remove(JsonKey.DATE_TIME);
        map.remove(JsonKey.COURSE_ENROLL_DATE);
        map.remove(JsonKey.USER_ID);
        map.remove(JsonKey.BATCH_ID);
      }

      Map<String, Object> responseMap = new LinkedHashMap<>();
      Map<String, Object> userdataMap = new LinkedHashMap<>();
      Map<String, Object> courseprogressdataMap = new LinkedHashMap<>();
      Map<String, Object> valueMap = new LinkedHashMap<>();

      userdataMap.put(JsonKey.NAME, "List of users enrolled for the course");
      userdataMap.put("split", "content.sum(time_spent)");
      userdataMap.put("buckets", useresContent);

      courseprogressdataMap.put(JsonKey.NAME, "List of users enrolled for the course");
      courseprogressdataMap.put("split", "content.sum(time_spent)");
      courseprogressdataMap.put("buckets", esContent);

      valueMap.put("course.progress.users_enrolled.count", userdataMap);
      valueMap.put("course.progress.course_progress_per_user.count", courseprogressdataMap);

      responseMap.put("period", periodStr);
      responseMap.put("series", valueMap);

      Response response = new Response();
      response.putAll(responseMap);
      sender().tell(response, self());
      return;
    } else {

      ProjectLogger.log(
          "CourseMetricsActor:courseProgressMetrics: Course not found for given batchId.",
          LoggerEnum.INFO.name());

      Map<String, Object> responseMap = new LinkedHashMap<>();
      Map<String, Object> userdataMap = new LinkedHashMap<>();
      Map<String, Object> courseprogressdataMap = new LinkedHashMap<>();
      Map<String, Object> valueMap = new LinkedHashMap<>();

      userdataMap.put(JsonKey.NAME, "List of users enrolled for the course");
      userdataMap.put("split", "content.sum(time_spent)");
      userdataMap.put("buckets", new ArrayList<>());

      courseprogressdataMap.put(JsonKey.NAME, "List of users enrolled for the course");
      courseprogressdataMap.put("split", "content.sum(time_spent)");
      courseprogressdataMap.put("buckets", esContent);

      valueMap.put("course.progress.users_enrolled.count", userdataMap);
      valueMap.put("course.progress.course_progress_per_user.count", courseprogressdataMap);

      responseMap.put("period", periodStr);
      responseMap.put("series", valueMap);

      Response response = new Response();
      response.putAll(responseMap);
      sender().tell(response, self());

      return;
    }
  }

  private void courseConsumptionMetrics(Request actorMessage) {
    ProjectLogger.log(
        "CourseMetricsActor: courseConsumptionMetrics called.", LoggerEnum.INFO.name());
    try {
      String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
      String courseId = (String) actorMessage.getRequest().get(JsonKey.COURSE_ID);
      String requestedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

      Map<String, Object> requestObject = new HashMap<>();
      requestObject.put(JsonKey.PERIOD, getEkstepPeriod(periodStr));
      Map<String, Object> filterMap = new HashMap<>();
      filterMap.put(CONTENT_ID, courseId);
      requestObject.put(JsonKey.FILTER, filterMap);

      Map<String, Object> result =
          ElasticSearchUtil.getDataByIdentifier(
              EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), requestedBy);
      if (null == result || result.isEmpty()) {
        ProjectCommonException exception =
            new ProjectCommonException(
                ResponseCode.unAuthorized.getErrorCode(),
                ResponseCode.unAuthorized.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      String rootOrgId = (String) result.get(JsonKey.ROOT_ORG_ID);
      if (StringUtils.isBlank(rootOrgId)) {
        ProjectCommonException exception =
            new ProjectCommonException(
                ResponseCode.noDataForConsumption.getErrorCode(),
                ResponseCode.noDataForConsumption.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
      }
      Map<String, Object> rootOrgData =
          ElasticSearchUtil.getDataByIdentifier(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName(),
              rootOrgId);
      if (null == rootOrgData || rootOrgData.isEmpty()) {
        ProjectCommonException exception =
            new ProjectCommonException(
                ResponseCode.invalidData.getErrorCode(),
                ResponseCode.invalidData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
      }

      String channel = (String) rootOrgData.get(JsonKey.HASHTAGID);
      ProjectLogger.log(
          "CourseMetricsActor:courseConsumptionMetrics: Root organisation hashtag id = " + channel,
          LoggerEnum.INFO.name());
      String responseFormat = getCourseConsumptionData(periodStr, courseId, requestObject, channel);
      Response response =
          metricsResponseGenerator(responseFormat, periodStr, getViewData(courseId));
      sender().tell(response, self());
    } catch (ProjectCommonException e) {
      ProjectLogger.log(
          "CourseMetricsActor:courseConsumptionMetrics: Exception in getting course consumption data: "
              + e.getMessage(),
          e);
      sender().tell(e, self());
      return;
    } catch (Exception e) {
      ProjectLogger.log(
          "CourseMetricsActor:courseConsumptionMetrics: Generic exception in getting course consumption data: "
              + e.getMessage(),
          e);
      throw new ProjectCommonException(
          ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  private String getCourseConsumptionData(
      String periodStr, String courseId, Map<String, Object> requestObject, String channel) {
    Request request = new Request();
    requestObject.put(JsonKey.CHANNEL, channel);
    request.setRequest(requestObject);
    String responseFormat = "";
    try {
      String requestStr = mapper.writeValueAsString(request);
      String analyticsBaseUrl = ProjectUtil.getConfigValue(JsonKey.ANALYTICS_API_BASE_URL);
      String ekStepResponse =
          makePostRequest(analyticsBaseUrl, JsonKey.EKSTEP_METRICS_API_URL, requestStr);
      responseFormat =
          courseConsumptionResponseGenerator(periodStr, ekStepResponse, courseId, channel);
    } catch (Exception e) {
      ProjectLogger.log("Error occurred", e);
      throw new ProjectCommonException(
          ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    return responseFormat;
  }

  private Map<String, Object> getViewData(String courseId) {
    Map<String, Object> courseData = new HashMap<>();
    Map<String, Object> viewData = new HashMap<>();
    courseData.put(JsonKey.COURSE_ID, courseId);
    viewData.put(JsonKey.COURSE, courseData);
    return viewData;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getCourseCompletedData(
      String periodStr, String courseId, String channel) {
    Map<String, Object> dateRange = getStartAndEndDate(periodStr);
    Map<String, Object> filter = new HashMap<>();
    Map<String, Object> resultMap = new HashMap<>();
    filter.put(JsonKey.COURSE_ID, courseId);
    Map<String, String> dateRangeFilter = new HashMap<>();
    dateRangeFilter.put(GTE, (String) dateRange.get(STARTDATE));
    dateRangeFilter.put(LTE, (String) dateRange.get(ENDDATE));
    filter.put(JsonKey.DATE_TIME, dateRangeFilter);
    filter.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.COMPLETED.getValue());

    List<String> coursefields = new ArrayList<>();
    coursefields.add(JsonKey.USER_ID);
    coursefields.add(JsonKey.PROGRESS);
    coursefields.add(JsonKey.STATUS);

    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            createESRequest(filter, null, coursefields),
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            EsType.usercourses.getTypeName());
    if (null == result || result.isEmpty()) {
      throw new ProjectCommonException(
          ResponseCode.noDataForConsumption.getErrorCode(),
          ResponseCode.noDataForConsumption.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    List<Map<String, Object>> esContent = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);

    List<String> userIds = new ArrayList<>();
    Double timeConsumed = 0D;
    for (Map<String, Object> entry : esContent) {
      String userId = (String) entry.get(JsonKey.USER_ID);
      timeConsumed = timeConsumed + getMetricsForUser(courseId, userId, periodStr, channel);
      userIds.add(userId);
    }
    Integer users_count = userIds.size();
    resultMap.put("user_count", users_count);
    if (0 == users_count) {
      resultMap.put("avg_time_course_completed", 0);
    } else {
      resultMap.put("avg_time_course_completed", timeConsumed / users_count);
    }
    return resultMap;
  }

  @SuppressWarnings("unchecked")
  private Double getMetricsForUser(
      String courseId, String userId, String periodStr, String channel) {
    Double userTimeConsumed = 0D;
    Map<String, Object> requestObject = new HashMap<>();
    Request request = new Request();
    requestObject.put(JsonKey.PERIOD, getEkstepPeriod(periodStr));
    Map<String, Object> filterMap = new HashMap<>();
    filterMap.put(CONTENT_ID, courseId);
    filterMap.put(USER_ID, userId);
    requestObject.put(JsonKey.FILTER, filterMap);
    ProjectLogger.log("Channel for Course" + channel);
    if (null == channel || channel.isEmpty()) {
      throw new ProjectCommonException(
          ResponseCode.noDataForConsumption.getErrorCode(),
          ResponseCode.noDataForConsumption.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    requestObject.put(JsonKey.CHANNEL, channel);
    request.setRequest(requestObject);
    try {
      String requestStr = mapper.writeValueAsString(request);
      String analyticsBaseUrl = ProjectUtil.getConfigValue(JsonKey.ANALYTICS_API_BASE_URL);
      String ekStepResponse =
          makePostRequest(analyticsBaseUrl, JsonKey.EKSTEP_METRICS_API_URL, requestStr);
      Map<String, Object> resultData = mapper.readValue(ekStepResponse, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      userTimeConsumed = (Double) resultData.get("m_total_ts");
    } catch (Exception e) {
      ProjectLogger.log("Error occurred", e);
    }
    return userTimeConsumed;
  }

  @SuppressWarnings("unchecked")
  private String courseConsumptionResponseGenerator(
      String period, String ekstepResponse, String courseId, String channel) {
    String result = "";
    try {
      Map<String, Object> resultData = mapper.readValue(ekstepResponse, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      List<Map<String, Object>> resultList =
          (List<Map<String, Object>>) resultData.get(JsonKey.METRICS);
      List<Map<String, Object>> userBucket = createBucketStructure(period);
      List<Map<String, Object>> consumptionBucket = createBucketStructure(period);
      Map<String, Object> userData = null;
      int index = 0;
      Collections.reverse(resultList);
      Map<String, Object> resData = null;
      for (Map<String, Object> res : resultList) {
        resData = consumptionBucket.get(index);
        userData = userBucket.get(index);
        String bucketDate = "";
        String metricsDate = "";
        if ("5w".equalsIgnoreCase(period)) {
          bucketDate = (String) resData.get("key");
          bucketDate = bucketDate.substring(bucketDate.length() - 2, bucketDate.length());
          metricsDate = String.valueOf(res.get("d_period"));
          metricsDate = metricsDate.substring(metricsDate.length() - 2, metricsDate.length());
        } else {
          bucketDate = (String) resData.get("key_name");
          metricsDate = String.valueOf(res.get("d_period"));
          Date date = new SimpleDateFormat("yyyyMMdd").parse(metricsDate);
          metricsDate = new SimpleDateFormat("yyyy-MM-dd").format(date);
        }
        if (metricsDate.equalsIgnoreCase(bucketDate)) {
          Double totalTimeSpent = (Double) res.get("m_total_ts");
          Integer totalUsers = (Integer) res.get("m_total_users_count");
          resData.put(VALUE, totalTimeSpent);
          userData.put(VALUE, totalUsers);
        }
        if (index < consumptionBucket.size() && index < userBucket.size()) {
          index++;
        }
      }

      Map<String, Object> series = new HashMap<>();

      Map<String, Object> seriesData = new LinkedHashMap<>();
      seriesData.put(JsonKey.NAME, "Timespent for content consumption");
      seriesData.put(JsonKey.SPLIT, "content.sum(time_spent)");
      seriesData.put(JsonKey.TIME_UNIT, "seconds");
      seriesData.put(GROUP_ID, "course.timespent.sum");
      seriesData.put("buckets", consumptionBucket);
      series.put("course.consumption.time_spent", seriesData);
      seriesData = new LinkedHashMap<>();
      if ("5w".equalsIgnoreCase(period)) {
        seriesData.put(JsonKey.NAME, "Number of users by week");
      } else {
        seriesData.put(JsonKey.NAME, "Number of users by day");
      }
      seriesData.put(JsonKey.SPLIT, "content.users.count");
      seriesData.put(GROUP_ID, "course.users.count");
      seriesData.put("buckets", userBucket);
      series.put("course.consumption.content.users.count", seriesData);
      Map<String, Object> courseCompletedData = new HashMap<>();
      try {
        courseCompletedData = getCourseCompletedData(period, courseId, channel);
      } catch (Exception e) {
        ProjectLogger.log("Error occurred", e);
      }
      ProjectLogger.log("Course completed Data" + courseCompletedData);
      resultData = (Map<String, Object>) resultData.get(JsonKey.SUMMARY);
      Map<String, Object> snapshot = new LinkedHashMap<>();
      Map<String, Object> dataMap = new HashMap<>();
      dataMap.put(JsonKey.NAME, "Total time of Content consumption");
      dataMap.put(VALUE, resultData.get("m_total_ts"));
      dataMap.put(JsonKey.TIME_UNIT, "seconds");
      snapshot.put("course.consumption.time_spent.count", dataMap);
      dataMap = new LinkedHashMap<>();
      dataMap.put(JsonKey.NAME, "User access course over time");
      dataMap.put(VALUE, resultData.get("m_total_users_count"));
      snapshot.put("course.consumption.time_per_user", dataMap);
      dataMap = new LinkedHashMap<>();
      dataMap.put(JsonKey.NAME, "Total users completed the course");
      int userCount =
          courseCompletedData.get("user_count") == null
              ? 0
              : (Integer) courseCompletedData.get("user_count");
      dataMap.put(VALUE, userCount);
      snapshot.put("course.consumption.users_completed", dataMap);
      dataMap = new LinkedHashMap<>();
      dataMap.put(JsonKey.NAME, "Average time per user for course completion");
      int avgTime =
          courseCompletedData.get("avg_time_course_completed") == null
              ? 0
              : (courseCompletedData.get("avg_time_course_completed") instanceof Double)
                  ? ((Double) courseCompletedData.get("avg_time_course_completed")).intValue()
                  : (Integer) courseCompletedData.get("avg_time_course_completed");
      dataMap.put(VALUE, avgTime);
      dataMap.put(JsonKey.TIME_UNIT, "seconds");
      snapshot.put("course.consumption.time_spent_completion_count", dataMap);

      Map<String, Object> responseMap = new HashMap<>();
      responseMap.put(JsonKey.SNAPSHOT, snapshot);
      responseMap.put(JsonKey.SERIES, series);
      result = mapper.writeValueAsString(responseMap);
    } catch (Exception e) {
      ProjectLogger.log("Error occurred", e);
    }
    return result;
  }
}
