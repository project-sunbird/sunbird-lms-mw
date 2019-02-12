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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
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

  private static final String DOT = ".";
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

    ProjectLogger.log(
        "CourseMetricsBackgroundActor: courseProgressMetricsData called.", LoggerEnum.INFO.name());
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
      ProjectLogger.log(
          "CourseMetricsBackgroundActor:courseProgressMetricsData: requestId not found.",
          LoggerEnum.INFO.name());
      throw new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    Map<String, Object> reportDbInfo = responseList.get(0);

    List<List<Object>> finalList = new ArrayList<>();
    List<Object> columnNames =
        Arrays.asList(
            JsonKey.USER_NAME_HEADER,
            JsonKey.ORG_NAME_HEADER,
            JsonKey.SCHOOL_NAME_HEADER,
            JsonKey.COURSE_ENROLL_DATE_HEADER,
            JsonKey.PROGRESS_HEADER);
    finalList.add(columnNames);
    String periodStr = (String) reportDbInfo.get(JsonKey.PERIOD);
    String batchId = (String) reportDbInfo.get(JsonKey.RESOURCE_ID);
    // get start and end time ---
    Map<String, String> dateRangeFilter = new HashMap<>();

    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.PERIOD, periodStr);
    Map<String, Object> filter = new HashMap<>();
    filter.put(JsonKey.BATCH_ID, batchId);
    filter.put(JsonKey.ACTIVE, true);
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
    coursefields.add(JsonKey.COURSE_ENROLL_DATE);
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
      userfields.add(JsonKey.FIRST_NAME);
      userfields.add(JsonKey.LAST_NAME);
      userfields.add(JsonKey.ORGANISATIONS + DOT + JsonKey.ORGANISATION_ID);
      userfields.add(JsonKey.ROOT_ORG_ID);
      userfields.add(JsonKey.GENDER);

      Map<String, Object> userresult =
          ElasticSearchUtil.complexSearch(
              createESRequest(userfilter, null, userfields),
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              EsType.user.getTypeName());
      List<Map<String, Object>> useresContent =
          (List<Map<String, Object>>) userresult.get(JsonKey.CONTENT);

      Map<String, Map<String, Object>> userInfoCache = new HashMap<>();
      Set<String> uniqueOrgIds = new HashSet<>();
      // decrypt the user info get from the elastic search
      useresContent = decryptionService.decryptData(useresContent);
      for (Map<String, Object> map : useresContent) {
        String userId = (String) map.get(JsonKey.USER_ID);
        map.put("user", userId);
        userInfoCache.put(userId, new HashMap<String, Object>(map));
        map.remove(JsonKey.USER_ID);
        uniqueOrgIds.add((String) map.get(JsonKey.ROOT_ORG_ID));
        List<Map<String, Object>> userOrgs =
            (List<Map<String, Object>>) map.get(JsonKey.ORGANISATIONS);
        if (!CollectionUtils.isEmpty(userOrgs)) {
          uniqueOrgIds.addAll(
              userOrgs
                  .stream()
                  .map(userOrg -> (String) userOrg.get(JsonKey.ORGANISATION_ID))
                  .collect(Collectors.toSet()));
        }
      }
      Map<String, Map<String, Object>> orgDetails = fetchOrgDetailsById(uniqueOrgIds);

      for (Map<String, Object> map : userCoursesContent) {
        List<Object> list = new ArrayList<>();
        Map<String, Object> userMap = userInfoCache.get(map.get(JsonKey.USER_ID));
        if (isNotNull(userMap)) {
          list.add(getFullName(userMap));
          list.add(getRootOrgName(userMap, orgDetails));
          list.add(getCommaSepSubOrgName(userMap, orgDetails));
          list.add(map.get(JsonKey.COURSE_ENROLL_DATE));
          list.add(map.get(JsonKey.PROGRESS));
        } else {
          list.add(null);
          list.add(null);
          list.add(null);
          list.add(null);
          list.add(map.get(JsonKey.PROGRESS));
        }
        finalList.add(list);
      }
    } else {
      ProjectLogger.log(
          "CourseMetricsBackgroundActor:courseProgressMetricsData user course content is empty",
          LoggerEnum.ERROR);
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

  private Map<String, Map<String, Object>> fetchOrgDetailsById(Set<String> uniqueOrgIds) {
    List<String> orgFields = new ArrayList<>();
    orgFields.add(JsonKey.ID);
    orgFields.add(JsonKey.ORG_NAME);

    Map<String, Object> orgFilter = new HashMap<>();
    orgFilter.put(JsonKey.ID, uniqueOrgIds.stream().collect(Collectors.toList()));
    Map<String, Map<String, Object>> orgDetails = new HashMap<>();
    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            createESRequest(orgFilter, null, orgFields),
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            EsType.organisation.getTypeName());
    List<Map<String, Object>> orgContent = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);

    if (!(orgContent.isEmpty())) {
      orgContent.forEach(org -> orgDetails.put((String) org.get(JsonKey.ID), org));
    }
    return orgDetails;
  }

  private String getFullName(Map<String, Object> userMap) {
    String fullName =
        userMap.containsKey(JsonKey.FIRST_NAME)
                && StringUtils.isNotEmpty((String) userMap.get(JsonKey.FIRST_NAME))
            ? (String) userMap.get(JsonKey.FIRST_NAME)
            : StringUtils.EMPTY;
    fullName +=
        userMap.containsKey(JsonKey.LAST_NAME)
                && StringUtils.isNotEmpty((String) userMap.get(JsonKey.LAST_NAME))
            ? (" " + (String) userMap.get(JsonKey.LAST_NAME))
            : StringUtils.EMPTY;
    return fullName;
  }

  private String getRootOrgName(
      Map<String, Object> userMap, Map<String, Map<String, Object>> orgDetails) {
    String rootOrgId = (String) userMap.get(JsonKey.ROOT_ORG_ID);
    if (orgDetails.containsKey(rootOrgId) && !MapUtils.isEmpty(orgDetails.get(rootOrgId))) {
      Map<String, Object> orgDetail = orgDetails.get(rootOrgId);
      return (String) orgDetail.get(JsonKey.ORG_NAME);
    }
    return StringUtils.EMPTY;
  }

  private String getCommaSepSubOrgName(
      Map<String, Object> userMap, Map<String, Map<String, Object>> orgDetails) {
    String rootOrgId = (String) userMap.get(JsonKey.ROOT_ORG_ID);
    List<Map<String, Object>> userOrgs =
        (List<Map<String, Object>>) userMap.get(JsonKey.ORGANISATIONS);
    if (!CollectionUtils.isEmpty(userOrgs)) {
      List<String> orgNames =
          userOrgs
              .stream()
              .filter(userOrg -> !rootOrgId.equals((String) userOrg.get(JsonKey.ORGANISATION_ID)))
              .map(
                  userOrg -> {
                    Map<String, Object> orgDetail =
                        orgDetails.get((String) userOrg.get(JsonKey.ORGANISATION_ID));
                    if (!MapUtils.isEmpty(orgDetail)) {
                      return (String) orgDetail.get(JsonKey.ORG_NAME);
                    }
                    return null;
                  })
              .filter(orgName -> orgName != null)
              .collect(Collectors.toList());
      if (!CollectionUtils.isEmpty(orgNames)) {
        return String.join(",", orgNames);
      }
    }
    return StringUtils.EMPTY;
  }
}
