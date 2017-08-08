package org.sunbird.metrics.actors;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import akka.actor.UntypedAbstractActor;

public class CourseMetricsActor extends UntypedAbstractActor {

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
    /*try {
      requestStr = mapper.writeValueAsString(request);
      //String resp = getDataFromEkstep(requestStr);
      //Map<String,Object> ekstepResponse = mapper.readValue(resp, Map.class);
      //resultData = (Map<String,Object>) ekstepResponse.get(JsonKey.RESULT);
    } catch (JsonProcessingException e) {
     ProjectLogger.log(e.getMessage(),e);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);
    }*/
    Map<String, Object> responseMap = new LinkedHashMap<>();
    Map<String,Object> dataMap = new HashMap<>();
    List<Map<String,Object>> valueMap = new ArrayList<>();
    List<Map<String, Object>> bucket = new ArrayList<>();
    try {
      for(int count=0;count<7;count++){
        Map<String, Object> parentCountObject = new LinkedHashMap<String, Object>();
        parentCountObject.put("user", "123456"+(count));
        parentCountObject.put("userName", Character.toString ((char) (count+65)));
        bucket.add(parentCountObject);
      }
    }catch(Exception e){
      ProjectLogger.log(e.getMessage(), e);
    }
    Map<String, Object> series = new LinkedHashMap<>();
    
    Map<String,Object> seriesData = new LinkedHashMap<>();
    seriesData.put(JsonKey.NAME, "List of users enrolled for the course");
    seriesData.put(JsonKey.SPLIT, "content.sum(time_spent)");
    seriesData.put("buckets", bucket);
    series.put("course.progress.users_enrolled.count", seriesData);
    seriesData = new LinkedHashMap<>();
    bucket = new ArrayList<>();
    try {
      for(int count=0;count<7;count++){
        Map<String, Object> parentCountObject = new LinkedHashMap<String, Object>();
        parentCountObject.put("user", "123456"+(count));
        parentCountObject.put("progress", count*8);
        parentCountObject.put("lastAccessTime", "2017-07-2"+count+" "+(count*3)+":"+(count*2)+":"+(count*4));
        parentCountObject.put("userName", Character.toString ((char) (count+65)));
        parentCountObject.put("batchEndsOn", "2017-07-"+(count+20));
        parentCountObject.put("org", "sunbird");
        parentCountObject.put("enrolledOn", "2017-07-"+(count+10));
        bucket.add(parentCountObject);
      }
    }catch(Exception e){
      ProjectLogger.log(e.getMessage(), e);
    }
    seriesData.put(JsonKey.NAME, "List of users enrolled for the course");
    seriesData.put(JsonKey.SPLIT, "content.sum(time_spent)");
    seriesData.put("buckets", bucket);
    series.put("course.progress.course_progress_per_user.count", seriesData);
    responseMap.putAll(getViewData(courseId));
    responseMap.put(JsonKey.PERIOD, periodStr);
    responseMap.put(JsonKey.SERIES, series);
    Response response = new Response();
    response.putAll(responseMap);
    sender().tell(response, self()); 
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

}
