package org.sunbird.metrics.actors;

import static org.sunbird.common.models.util.ProjectUtil.isNotNull;

import akka.actor.ActorRef;
import akka.actor.Props;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.stream.Collectors;
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
import org.sunbird.common.models.util.ProjectUtil.ReportTrackingStatus;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.BackgroundJobManager;
import org.sunbird.learner.util.Util;

public class CourseMetricsActor extends BaseMetricsActor {

  private static final Object COURSE_PROGRESS_REPORT = " Course Prgoress Report" ;
  ElasticSearchUtil elasticSearchUtil = new ElasticSearchUtil();

  protected static final String CONTENT_ID = "content_id";
  private static ObjectMapper mapper = new ObjectMapper();
  private ActorRef backGroundActorRef;
  private static final String DEFAULT_BATCH_ID ="1";
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  public CourseMetricsActor() {
    backGroundActorRef = getContext().actorOf(Props.create(MetricsBackGroundJobActor.class), "metricsBackGroundJobActor");
  }

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("CourseMetricsActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.COURSE_PROGRESS_METRICS.getValue())) {
          courseProgressMetrics(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.COURSE_CREATION_METRICS.getValue())) {
          courseConsumptionMetrics(actorMessage);
        }else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.COURSE_PROGRESS_METRICS_REPORT.getValue())) {
          courseProgressMetricsReport(actorMessage);
        }else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.COURSE_CREATION_METRICS_REPORT.getValue())) {
          courseConsumptionMetricsReport(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION", LoggerEnum.INFO.name());
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                  ResponseCode.invalidOperationName.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    } else {
      // Throw exception as message body
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void courseConsumptionMetricsReport(Request actorMessage) {

    ProjectLogger.log("CourseMetricsActor-courseProgressMetrics called");
    Request request = new Request();
    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    String batchId = (String) actorMessage.getRequest().get(JsonKey.COURSE_ID);

    Response response = new Response();
    response.getResult().put(JsonKey.PROCESS_ID , 121);
    sender().tell(response, self());
  }

  private void courseProgressMetricsReport(Request actorMessage) {

    ProjectLogger.log("CourseMetricsActor-courseProgressMetrics called");

    String requestedBy = (String) actorMessage.get(JsonKey.REQUESTED_BY);

    Map<String , Object> requestedByInfo = ElasticSearchUtil.getDataByIdentifier(EsIndex.sunbird.getIndexName() , EsType.user.getTypeName() ,requestedBy);
    if(ProjectUtil.isNull(requestedByInfo) || ProjectUtil.isStringNullOREmpty((String)requestedByInfo.get(JsonKey.FIRST_NAME))){
      throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    Util.DbInfo reportTrackingdbInfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);

    Request request = new Request();
    List<List<Object>> finalList = null;
    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    String fileFormat = (String) actorMessage.getRequest().get(JsonKey.FORMAT);
    String batchId = (String) actorMessage.getRequest().get(JsonKey.BATCH_ID);
    //get start and end time ---
    Map<String , String> dateRangeFilter = new HashMap<>();

    request.setId(actorMessage.getId());
    request.setContext(actorMessage.getContext());
    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.PERIOD, periodStr);
    Map<String, Object> filter = new HashMap<>();
    Map<String , String> aggs = new HashMap<>();
    filter.put(JsonKey.BATCH_ID, batchId);

    if(!("fromBegining".equalsIgnoreCase(periodStr))) {
      Map<String, Object> dateRange = getStartAndEndDate(periodStr);
      dateRangeFilter.put(GTE , (String)dateRange.get(startDate));
      dateRangeFilter.put(LTE , (String)dateRange.get(endDate));
      filter.put(JsonKey.DATE_TIME , dateRangeFilter);
    }

    List<String> coursefields = new ArrayList<>();
    coursefields.add(JsonKey.USER_ID);
    coursefields.add(JsonKey.PROGRESS);
    //coursefields.add(JsonKey.COURSE_ENROLL_DATE);
    coursefields.add(JsonKey.BATCH_ID);
    //coursefields.add(JsonKey.DATE_TIME);

    Map<String, Object> result = ElasticSearchUtil.complexSearch(createESRequest(filter , null,
        coursefields), ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.usercourses.getTypeName());
    List<Map<String , Object>> userCoursesContent = (List<Map<String , Object>>)result.get(JsonKey.CONTENT);

    if(!(userCoursesContent.isEmpty())) {
      List<String> userIds = new ArrayList<String>();

      for (Map<String, Object> entry : userCoursesContent) {
        String userId = (String) entry.get(JsonKey.USER_ID);
        userIds.add(userId);
      }

      Set<String> uniqueUserIds = new HashSet<String>(userIds);
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

      //userfields.add(JsonKey.REGISTERED_ORG_ID);
      Map<String, Object> userresult = ElasticSearchUtil
          .complexSearch(createESRequest(userfilter, null, userfields),
              ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());
      List<Map<String, Object>> useresContent = (List<Map<String, Object>>) userresult
          .get(JsonKey.CONTENT);

      Map<String, Map<String, Object>> userInfoCache = new HashMap<>();
      Set<String> orgSet = new HashSet<>();

      for (Map<String, Object> map : useresContent) {
        String userId = (String) map.get(JsonKey.USER_ID);
        map.put("user", userId);
        userInfoCache.put(userId, new HashMap<String, Object>(map));
        // remove the org info from user content bcoz it is not desired in the user info result
        map.remove(JsonKey.REGISTERED_ORG_ID);
        map.remove(JsonKey.USER_ID);
      }


     List<Object> columnNames = Arrays.asList(JsonKey.LOGIN_ID , JsonKey.NAME , JsonKey.CREATED_DATE , JsonKey.LANGUAGE
     ,JsonKey.SUBJECT , JsonKey.GRADE , JsonKey.PROGRESS);

      finalList = new ArrayList<>();

      finalList.add(columnNames);

      for(Map<String , Object> map : userCoursesContent){
        List<Object> list = new ArrayList<>();
        Map<String , Object> userMap = userInfoCache.get(map.get(JsonKey.USER_ID));
        if(isNotNull(userMap)) {
          list.add(userMap.get(JsonKey.LOGIN_ID));
          list.add(userMap.get(JsonKey.FIRST_NAME));
          list.add(userMap.get(JsonKey.CREATED_DATE));
          list.add(userMap.get(JsonKey.LANGUAGE));
          list.add(userMap.get(JsonKey.SUBJECT));
          list.add(userMap.get(JsonKey.GRADE));
          list.add(map.get(JsonKey.PROGRESS));
        }else{
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

    String requestId = ProjectUtil.getUniqueIdFromTimestamp(1);

    Map<String , Object> requestDbInfo = new HashMap<>();
    requestDbInfo.put(JsonKey.ID , requestId);
    requestDbInfo.put(JsonKey.USER_ID, requestedBy);
    requestDbInfo.put(JsonKey.FIRST_NAME, requestedByInfo.get(JsonKey.FIRST_NAME));
    requestDbInfo.put(JsonKey.STATUS, ReportTrackingStatus.NEW.getValue());
    requestDbInfo.put(JsonKey.RESOURCE_ID , batchId);
    requestDbInfo.put(JsonKey.PERIOD , periodStr);
    requestDbInfo.put(JsonKey.CREATED_DATE , format.format(new Date()));
    requestDbInfo.put(JsonKey.UPDATED_DATE , format.format(new Date()));
    requestDbInfo.put(JsonKey.EMAIL, requestedByInfo.get(JsonKey.EMAIL));
    requestDbInfo.put(JsonKey.TYPE , COURSE_PROGRESS_REPORT);
    requestDbInfo.put(JsonKey.FORMAT , fileFormat);

    ObjectMapper mapper= new ObjectMapper();
    try {
      String data = mapper.writeValueAsString(finalList);
      requestDbInfo.put(JsonKey.DATA , data);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    cassandraOperation.insertRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        requestDbInfo);

    Response response = new Response();
    response.put(JsonKey.REQUEST_ID , requestId);
    sender().tell(response, self());

    // assign the back ground task to background job actor ...
    Request backGroundRequest = new Request();
    backGroundRequest.setOperation(ActorOperations.FILE_UPLOAD_AND_SEND_MAIL.getValue());

    Map<String , Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUEST_ID , requestId);
    innerMap.put(JsonKey.DATA , finalList);

    backGroundRequest.setRequest(innerMap);
    backGroundActorRef.tell(backGroundRequest , self());
  }

  private void assignTaskToBackGround(List<List<Object>> finalList) {
  }

  @SuppressWarnings("unchecked")
  private void courseProgressMetrics(Request actorMessage) {
    ProjectLogger.log("CourseMetricsActor-courseProgressMetrics called");
    Request request = new Request();
    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    String batchId = (String) actorMessage.getRequest().get(JsonKey.COURSE_ID);
    //get start and end time ---
    Map<String , String> dateRangeFilter = new HashMap<>();

    request.setId(actorMessage.getId());
    request.setContext(actorMessage.getContext());
    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.PERIOD, periodStr);
    Map<String, Object> filter = new HashMap<>();
    Map<String , String> aggs = new HashMap<>();
    filter.put(JsonKey.BATCH_ID, batchId);

    if(!("fromBegining".equalsIgnoreCase(periodStr))) {
      Map<String, Object> dateRange = getStartAndEndDate(periodStr);
      dateRangeFilter.put(GTE , (String)dateRange.get(startDate));
      dateRangeFilter.put(LTE , (String)dateRange.get(endDate));
      filter.put(JsonKey.DATE_TIME , dateRangeFilter);
    }

    List<String> coursefields = new ArrayList<>();
    coursefields.add(JsonKey.USER_ID);
    coursefields.add(JsonKey.PROGRESS);
    coursefields.add(JsonKey.COURSE_ENROLL_DATE);
    coursefields.add(JsonKey.BATCH_ID);
    coursefields.add(JsonKey.DATE_TIME);

    Map<String, Object> result = ElasticSearchUtil.complexSearch(createESRequest(filter , null,
        coursefields), ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.usercourses.getTypeName());
    List<Map<String , Object>> esContent = (List<Map<String , Object>>)result.get(JsonKey.CONTENT);

    if(!(esContent.isEmpty())) {
      List<String> userIds = new ArrayList<String>();

      for (Map<String, Object> entry : esContent) {
        String userId = (String) entry.get(JsonKey.USER_ID);
        userIds.add(userId);
      }

      Set<String> uniqueUserIds = new HashSet<String>(userIds);
      Map<String, Object> userfilter = new HashMap<>();
      userfilter.put(JsonKey.USER_ID, uniqueUserIds.stream().collect(Collectors.toList()));
      List<String> userfields = new ArrayList<>();
      userfields.add(JsonKey.USER_ID);
      userfields.add(JsonKey.USERNAME);
      userfields.add(JsonKey.REGISTERED_ORG_ID);
      Map<String, Object> userresult = ElasticSearchUtil
          .complexSearch(createESRequest(userfilter, null, userfields),
              ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName());
      List<Map<String, Object>> useresContent = (List<Map<String, Object>>) userresult
          .get(JsonKey.CONTENT);

      Map<String, Map<String, Object>> userInfoCache = new HashMap<>();
      Set<String> orgSet = new HashSet<>();

      for (Map<String, Object> map : useresContent) {
        String userId = (String) map.get(JsonKey.USER_ID);
        map.put("user", userId);
        String registerdOrgId = (String) map.get(JsonKey.REGISTERED_ORG_ID);
        if (isNotNull(registerdOrgId)) {
          orgSet.add(registerdOrgId);
        }
        userInfoCache.put(userId, new HashMap<String, Object>(map));
        // remove the org info from user content bcoz it is not desired in the user info result
        map.remove(JsonKey.REGISTERED_ORG_ID);
        map.remove(JsonKey.USER_ID);
      }

      Map<String, Object> orgfilter = new HashMap<>();
      orgfilter.put(JsonKey.ID, orgSet.stream().collect(Collectors.toList()));
      List<String> orgfields = new ArrayList<>();
      orgfields.add(JsonKey.ID);
      orgfields.add(JsonKey.ORGANISATION_NAME);
      Map<String, Object> orgresult = ElasticSearchUtil
          .complexSearch(createESRequest(orgfilter, null, orgfields),
              ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
      List<Map<String, Object>> orgContent = (List<Map<String, Object>>) orgresult
          .get(JsonKey.CONTENT);

      Map<String, String> orgInfoCache = new HashMap<>();
      for (Map<String, Object> map : orgContent) {

        String regOrgId = (String) map.get(JsonKey.ID);
        String regOrgName = (String) map.get(JsonKey.ORGANISATION_NAME);
        orgInfoCache.put(regOrgId, regOrgName);

      }

      Map<String, Object> batchFilter = new HashMap<>();
      batchFilter.put(JsonKey.ID, batchId);
      Map<String, Object> batchresult = ElasticSearchUtil
          .complexSearch(createESRequest(batchFilter, null, null),
              ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.course.getTypeName());
      List<Map<String, Object>> batchContent = (List<Map<String, Object>>) batchresult
          .get(JsonKey.CONTENT);

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
          map.put("org",
              orgInfoCache.get((String) userInfoCache.get(userId).get(JsonKey.REGISTERED_ORG_ID)));
          if (isNotNull(batchInfoCache.get((String) map.get(JsonKey.BATCH_ID)))) {
            map.put("batchEndsOn",
                batchInfoCache.get((String) map.get(JsonKey.BATCH_ID)).get(JsonKey.END_DATE));
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
    }else{

      ProjectLogger.log("CourseMetricsActor-courseProgressMetrics-- batch is not valid .");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidCourseBatchId.getErrorCode(),
              ResponseCode.invalidCourseBatchId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());

    }
  }

  @SuppressWarnings("unchecked")
  private void courseConsumptionMetricsMock(Request actorMessage) {
    Request request = new Request();
    String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
    String courseId = (String) actorMessage.getRequest().get(JsonKey.COURSE_ID);
    request.setId(actorMessage.getId());
    request.setContext(actorMessage.getContext());
    Map<String, Object> requestMap = new HashMap<>();
    Map<String, Object> filter = new HashMap<>();
    requestMap.put(JsonKey.PERIOD, periodStr);
    filter.put(JsonKey.TAG, courseId);
    //requestMap.put(JsonKey.CHANNEL, getChannel());
    request.setRequest(requestMap);
    Map<String,Object> resultData = new HashMap<>();
   /* try {
      //requestStr = mapper.writeValueAsString(request);
      //String resp = getDataFromEkstep(requestStr);
      //Map<String,Object> ekstepResponse = mapper.readValue(resp, Map.class);
      //resultData = (Map<String,Object>) ekstepResponse.get(JsonKey.RESULT);
    } catch (JsonProcessingException e) {
     ProjectLogger.log(e.getMessage(),e);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);
    }*/
    Map<String, Object> responseMap = new LinkedHashMap<>();
    Map<String,Object> snapshot = new LinkedHashMap<>();
    Map<String,Object> dataMap = new HashMap<>();
    dataMap.put(JsonKey.NAME, "Total time of Content consumption" );
    dataMap.put(VALUE,"345");
    snapshot.put("course.consumption.time_spent.count", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "User access course over time" );
    dataMap.put(VALUE,"213");
    snapshot.put("course.consumption.time_per_user", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "Total users completed the course" );
    dataMap.put(VALUE,"512");
    snapshot.put("course.consumption.users_completed", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "Average time per user for course completion" );
    dataMap.put(VALUE,"512");
    snapshot.put("course.consumption.time_spent_completion_count", dataMap);
    List<Map<String,Object>> valueMap = new ArrayList<>();
    List<Map<String, Object>> bucket = new ArrayList<>();
    valueMap = (List<Map<String,Object>>) resultData.get("metrics");
    try {
      for(int count=0;count<7;count++){
        Map<String, Object> parentCountObject = new LinkedHashMap<String, Object>();
        String value = "2017-07-2"+(count);
        parentCountObject.put("key", new SimpleDateFormat("yyyy-MM-dd").parse(value).getTime());
        parentCountObject.put("key_name", value);
        parentCountObject.put("value", count*8);
        bucket.add(parentCountObject);
      }
    }catch(Exception e){
      ProjectLogger.log(e.getMessage(), e);
    }
    Map<String, Object> series = new HashMap<>();
    
    Map<String,Object> seriesData = new LinkedHashMap<>();
    seriesData.put(JsonKey.NAME, "Timespent for content consumption");
    seriesData.put(JsonKey.SPLIT, "content.sum(time_spent)");
    seriesData.put("buckets", bucket);
    series.put("course.consumption.time_spent", seriesData);
    responseMap.putAll(getViewData(courseId));
    responseMap.put(JsonKey.PERIOD, periodStr);
    responseMap.put(JsonKey.SNAPSHOT, snapshot);
    responseMap.put(JsonKey.SERIES, series);
    Response response = new Response();
    response.putAll(responseMap);
    sender().tell(response, self()); 
  }

  private void courseConsumptionMetrics(Request actorMessage) {
    ProjectLogger.log("In courseConsumptionMetrics api");
    try {
      String periodStr = (String) actorMessage.getRequest().get(JsonKey.PERIOD);
      String courseId = (String) actorMessage.getRequest().get(JsonKey.COURSE_ID);
      Request request = new Request();
      request.setId(actorMessage.getId());
      Map<String, Object> requestObject = new HashMap<>();
      requestObject.put(JsonKey.PERIOD, getEkstepPeriod(periodStr));
      Map<String, Object> filterMap = new HashMap<>();
      filterMap.put(CONTENT_ID, courseId);
      requestObject.put(JsonKey.FILTER, filterMap);
      // TODO: get channel from
      String channel = "";
      requestObject.put(JsonKey.CHANNEL, channel);
      request.setRequest(requestObject);
      String requestStr = mapper.writeValueAsString(request);
      String ekStepResponse = makePostRequest(JsonKey.EKSTEP_METRICS_API_URL, requestStr);
      String responseFormat =
          courseConsumptionResponseGenerator(periodStr, ekStepResponse, courseId);
      Response response =
          metricsResponseGenerator(responseFormat, periodStr, getViewData(courseId));
      sender().tell(response, self());
    } catch (ProjectCommonException e) {
      ProjectLogger.log("Some error occurs", e);
      sender().tell(e, self());
      return;
    } catch (Exception e) {
      ProjectLogger.log("Some error occurs", e);
      throw new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  private Map<String, Object> getViewData(String courseId) {
    Map<String, Object> courseData = new HashMap<>();
    Map<String, Object> viewData = new HashMap<>();
    courseData.put(JsonKey.COURSE_ID, courseId);
    viewData.put(JsonKey.COURSE, courseData);
    return viewData;
  }

  

  @SuppressWarnings("unchecked")
  private Map<String, Object> getCourseCompletedData(String periodStr, String courseId) {
    Map<String, Object> dateRange = getStartAndEndDate(periodStr);
    Map<String, Object> filter = new HashMap<>();
    Map<String, Object> resultMap = new HashMap<>();
    filter.put(JsonKey.COURSE_ID, courseId);
    Map<String, String> dateRangeFilter = new HashMap<>();
    dateRangeFilter.put(GTE, (String) dateRange.get(startDate));
    dateRangeFilter.put(LTE, (String) dateRange.get(endDate));
    filter.put(JsonKey.DATE_TIME, dateRangeFilter);
    filter.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.COMPLETED.getValue());

    List<String> coursefields = new ArrayList<>();
    coursefields.add(JsonKey.USER_ID);
    coursefields.add(JsonKey.PROGRESS);
    coursefields.add(JsonKey.STATUS);

    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(createESRequest(filter, null, coursefields),
            ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.usercourses.getTypeName());
    List<Map<String, Object>> esContent = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);

    List<String> userIds = new ArrayList<String>();
    Double timeConsumed = 0D;
    for (Map<String, Object> entry : esContent) {
      String userId = (String) entry.get(JsonKey.USER_ID);
      timeConsumed = timeConsumed + getMetricsForUser(courseId, userId, periodStr);
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
  private Double getMetricsForUser(String courseId, String userId, String periodStr) {
    Double userTimeConsumed = 0D;
    Map<String, Object> requestObject = new HashMap<>();
    Request request = new Request();
    requestObject.put(JsonKey.PERIOD, getEkstepPeriod(periodStr));
    Map<String, Object> filterMap = new HashMap<>();
    filterMap.put(CONTENT_ID, courseId);
    filterMap.put(USER_ID, userId);
    requestObject.put(JsonKey.FILTER, filterMap);
    // TODO: get channel from
    String channel = "";
    requestObject.put(JsonKey.CHANNEL, channel);
    request.setRequest(requestObject);
    try {
      String requestStr = mapper.writeValueAsString(request);
      String ekStepResponse = makePostRequest(JsonKey.EKSTEP_METRICS_API_URL, requestStr);
      Map<String, Object> resultData = mapper.readValue(ekStepResponse, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      userTimeConsumed = (Double) resultData.get("m_total_ts");
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return userTimeConsumed;
  }

  @Override
  protected Map<String, Object> getViewData(String id, Object data) {
    return null;
  }

  @SuppressWarnings("unchecked")
  private String courseConsumptionResponseGenerator(String period, String ekstepResponse,
      String courseId) {
    String result = "";
    try {
      Map<String, Object> resultData = mapper.readValue(ekstepResponse, Map.class);
      resultData = (Map<String, Object>) resultData.get(JsonKey.RESULT);
      List<Map<String, Object>> resultList =
          (List<Map<String, Object>>) resultData.get(JsonKey.METRICS);
      List<Map<String, Object>> buckets = createBucketStructure(period);
      List<Map<String, Object>> userBucket = new ArrayList<>();
      List<Map<String, Object>> consumptionBucket = new ArrayList<>();
      Map<String, Object> userData = new HashMap<>();
      int index = 0;
      Collections.reverse(resultList);
      Map<String, Object> resData = new HashMap<>();
      for (Map<String, Object> res : resultList) {
        resData = buckets.get(index);
        userData = resData;
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
        consumptionBucket.add(resData);
        userBucket.add(userData);
        if (index < buckets.size()) {
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

      Map<String, Object> courseCompletedData = getCourseCompletedData(period, courseId);
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
      dataMap.put(JsonKey.TIME_UNIT, "seconds");
      snapshot.put("course.consumption.time_per_user", dataMap);
      dataMap = new LinkedHashMap<>();
      dataMap.put(JsonKey.NAME, "Total users completed the course");
      dataMap.put(VALUE, courseCompletedData.get("user_count"));
      snapshot.put("course.consumption.users_completed", dataMap);
      dataMap = new LinkedHashMap<>();
      dataMap.put(JsonKey.NAME, "Average time per user for course completion");
      dataMap.put(VALUE, courseCompletedData.get("avg_time_course_completed"));
      dataMap.put(JsonKey.TIME_UNIT, "seconds");
      snapshot.put("course.consumption.time_spent_completion_count", dataMap);

      Map<String, Object> responseMap = new HashMap<>();
      responseMap.put(JsonKey.SNAPSHOT, snapshot);
      responseMap.put(JsonKey.SERIES, series);
      result = mapper.writeValueAsString(responseMap);
    } catch (JsonProcessingException e) {
      ProjectLogger.log(e.getMessage());
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return result;
  }
}
