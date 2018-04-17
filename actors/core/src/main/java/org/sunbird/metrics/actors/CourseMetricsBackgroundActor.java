package org.sunbird.metrics.actors;

import static org.sunbird.common.models.util.ProjectUtil.isNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.ReportTrackingStatus;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

@ActorConfig(
  tasks = {},
  asyncTasks = {"courseProgressMetricsData"}
)
public class CourseMetricsBackgroundActor extends BaseMetricsActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo reportTrackingdbInfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);
  private DecryptionService decryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(
          null);

  @Override
  public void onReceive(Request request) throws Throwable {
    if (request
        .getOperation()
        .equalsIgnoreCase(ActorOperations.COURSE_PROGRESS_METRICS_DATA.getValue())) {
      courseProgressMetricsData(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  @SuppressWarnings("unchecked")
  public void courseProgressMetricsData(Request actorMessage) {

    ProjectLogger.log("CourseMetricsActor-courseProgressMetrics called");
    SimpleDateFormat format = ProjectUtil.getDateFormatter();
    format.setLenient(false);

    Map<String, Object> req = actorMessage.getRequest();
    String requestId = (String) req.get(JsonKey.REQUEST_ID);

    // fetch the DB details from database on basis of requestId ....
    Response response =
        cassandraOperation.getRecordById(
            reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(), requestId);
    List<Map<String, Object>> responseList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (responseList.isEmpty()) {
      ProjectLogger.log("Invalid data");
      throw new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    Map<String, Object> reportDbInfo = responseList.get(0);

    List<List<Object>> finalList = null;
    String periodStr = (String) reportDbInfo.get(JsonKey.PERIOD);
    String batchId = (String) reportDbInfo.get(JsonKey.RESOURCE_ID);
    // get start and end time ---
    Map<String, String> dateRangeFilter = new HashMap<>();

    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.PERIOD, periodStr);
    Map<String, Object> filter = new HashMap<>();
    filter.put(JsonKey.BATCH_ID, batchId);

    if (!("fromBegining".equalsIgnoreCase(periodStr))) {
      Map<String, Object> dateRange = getStartAndEndDate(periodStr);
      dateRangeFilter.put(GTE, (String) dateRange.get(STARTDATE));
      dateRangeFilter.put(LTE, (String) dateRange.get(ENDDATE));
      filter.put(JsonKey.DATE_TIME, dateRangeFilter);
    }

    List<String> coursefields = new ArrayList<>();
    coursefields.add(JsonKey.USER_ID);
    coursefields.add(JsonKey.PROGRESS);
    coursefields.add(JsonKey.BATCH_ID);
    coursefields.add(JsonKey.LEAF_NODE_COUNT);

    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            createESRequest(filter, null, coursefields),
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            EsType.usercourses.getTypeName());
    List<Map<String, Object>> userCoursesContent =
        (List<Map<String, Object>>) result.get(JsonKey.CONTENT);

    if (!(userCoursesContent.isEmpty())) {
      List<String> userIds = new ArrayList<>();

      calculateCourseProgressPercentage(userCoursesContent);

      for (Map<String, Object> entry : userCoursesContent) {
        String userId = (String) entry.get(JsonKey.USER_ID);
        userIds.add(userId);
      }

      Set<String> uniqueUserIds = new HashSet<>(userIds);
      Map<String, Object> userfilter = new HashMap<>();
      userfilter.put(JsonKey.USER_ID, uniqueUserIds.stream().collect(Collectors.toList()));
      List<String> userfields = new ArrayList<>();
      userfields.add(JsonKey.USER_ID);
      userfields.add(JsonKey.USERNAME);
      userfields.add(JsonKey.FIRST_NAME);
      userfields.add(JsonKey.LOGIN_ID);
      userfields.add(JsonKey.CREATED_DATE);
      userfields.add(JsonKey.LANGUAGE);
      userfields.add(JsonKey.SUBJECT);
      userfields.add(JsonKey.GRADE);
      userfields.add(JsonKey.GENDER);

      Map<String, Object> userresult =
          ElasticSearchUtil.complexSearch(
              createESRequest(userfilter, null, userfields),
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              EsType.user.getTypeName());
      List<Map<String, Object>> useresContent =
          (List<Map<String, Object>>) userresult.get(JsonKey.CONTENT);

      Map<String, Map<String, Object>> userInfoCache = new HashMap<>();

      // decrypt the user info get from the elastic search
      useresContent = decryptionService.decryptData(useresContent);
      for (Map<String, Object> map : useresContent) {
        String userId = (String) map.get(JsonKey.USER_ID);
        map.put("user", userId);
        userInfoCache.put(userId, new HashMap<String, Object>(map));
        // remove the org info from user content bcoz it is not desired in the user info
        // result
        map.remove(JsonKey.REGISTERED_ORG_ID);
        map.remove(JsonKey.USER_ID);
      }

      List<Object> columnNames =
          Arrays.asList(
              JsonKey.LOGIN_ID,
              JsonKey.NAME,
              JsonKey.CREATED_DATE,
              JsonKey.LANGUAGE,
              JsonKey.SUBJECT,
              JsonKey.GRADE,
              JsonKey.PROGRESS);
      finalList = new ArrayList<>();
      finalList.add(columnNames);

      for (Map<String, Object> map : userCoursesContent) {
        List<Object> list = new ArrayList<>();
        Map<String, Object> userMap = userInfoCache.get(map.get(JsonKey.USER_ID));
        if (isNotNull(userMap)) {
          list.add(userMap.get(JsonKey.LOGIN_ID));
          list.add(userMap.get(JsonKey.FIRST_NAME));
          list.add(userMap.get(JsonKey.CREATED_DATE));
          list.add(userMap.get(JsonKey.LANGUAGE));
          list.add(userMap.get(JsonKey.SUBJECT));
          list.add(userMap.get(JsonKey.GRADE));
          list.add(map.get(JsonKey.PROGRESS));
        } else {
          list.add(null);
          list.add(null);
          list.add(null);
          list.add(null);
          list.add(null);
          list.add(null);
          list.add(map.get(JsonKey.PROGRESS));
        }
        finalList.add(list);
      }
    }

    Map<String, Object> requestDbInfo = new HashMap<>();
    requestDbInfo.put(JsonKey.ID, requestId);
    requestDbInfo.put(JsonKey.STATUS, ReportTrackingStatus.GENERATING_DATA.getValue());
    requestDbInfo.put(JsonKey.UPDATED_DATE, format.format(new Date()));

    ObjectMapper mapper = new ObjectMapper();
    try {
      String data = mapper.writeValueAsString(finalList);
      requestDbInfo.put(JsonKey.DATA, data);
    } catch (JsonProcessingException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    cassandraOperation.updateRecord(
        reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(), requestDbInfo);

    Request backGroundRequest = new Request();
    backGroundRequest.setOperation(ActorOperations.FILE_GENERATION_AND_UPLOAD.getValue());
    backGroundRequest.getRequest().put(JsonKey.DATA, finalList);
    backGroundRequest.getRequest().put(JsonKey.REQUEST_ID, requestId);
    tellToAnother(backGroundRequest);
  }
}
