package org.sunbird.metrics.actors;

import static org.sunbird.common.models.util.ProjectUtil.isNotNull;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.stream.Collectors;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import org.sunbird.dto.SearchDTO;

public class CourseMetricsActor extends BaseMetricsActor {

  ElasticSearchUtil elasticSearchUtil = new ElasticSearchUtil();

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("OrganisationManagementActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.COURSE_PROGRESS_METRICS.getValue())) {
          courseProgressMetrics(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.COURSE_CREATION_METRICS.getValue())) {
          courseConsumptionMetrics(actorMessage);
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

  private void courseProgressMetrics(Request actorMessage) {
    ProjectLogger.log("OrganisationManagementActor-courseProgressMetrics called");
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

      responseMap.put("period", "7d");
      responseMap.put("series", valueMap);

      Response response = new Response();
      response.putAll(responseMap);
      sender().tell(response, self());
    }else{

      ProjectLogger.log("OrganisationManagementActor-courseProgressMetrics-- Courses for bbatch is empty .");

      Map<String, Object> responseMap = new LinkedHashMap<>();
      Map<String, Object> userdataMap = new LinkedHashMap<>();
      Map<String, Object> courseprogressdataMap = new LinkedHashMap<>();
      Map<String, Object> valueMap = new LinkedHashMap<>();

      userdataMap.put(JsonKey.NAME, "List of users enrolled for the course");
      userdataMap.put("split", "content.sum(time_spent)");
      userdataMap.put("buckets", new ArrayList());

      courseprogressdataMap.put(JsonKey.NAME, "List of users enrolled for the course");
      courseprogressdataMap.put("split", "content.sum(time_spent)");
      courseprogressdataMap.put("buckets", esContent);

      valueMap.put("course.progress.users_enrolled.count", userdataMap);
      valueMap.put("course.progress.course_progress_per_user.count", courseprogressdataMap);

      responseMap.put("period", "7d");
      responseMap.put("series", valueMap);

      Response response = new Response();
      response.putAll(responseMap);
      sender().tell(response, self());

    }
  }

  @SuppressWarnings("unchecked")
  private void courseConsumptionMetrics(Request actorMessage) {
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
    dataMap.put(JsonKey.VALUE,"345");
    snapshot.put("course.consumption.time_spent.count", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "User access course over time" );
    dataMap.put(JsonKey.VALUE,"213");
    snapshot.put("course.consumption.time_per_user", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "Total users completed the course" );
    dataMap.put(JsonKey.VALUE,"512");
    snapshot.put("course.consumption.users_completed", dataMap);
    dataMap = new LinkedHashMap<>();
    dataMap.put(JsonKey.NAME, "Average time per user for course completion" );
    dataMap.put(JsonKey.VALUE,"512");
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

  private Map<String, Object> getViewData(String courseId){
    Map<String,Object> courseData = new HashMap<>();
    Map<String,Object> viewData = new HashMap<>();
    courseData.put(JsonKey.COURSE_ID,courseId);
    viewData.put(JsonKey.COURSE, courseData);
    return viewData;
  }

  private SearchDTO createESRequest(Map<String, Object> filters, Map<String, String> aggs,
      List<String> fields){
    SearchDTO searchDTO = new SearchDTO();

    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS , filters);
    if(isNotNull(aggs)) {
      searchDTO.getFacets().add(aggs);
    }
    if(isNotNull(fields)){
      searchDTO.setFields(fields);
    }
    return searchDTO;
  }

  @Override
  protected Map<String, Object> getViewData(String id, Object data) {
    return null;
  }
}
