/**
 * 
 */
package org.sunbird.learner.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.actors.CourseEnrollmentActor;

/**
 * This class will update course batch count to EKStep.
 * First it will get batch details from ES , then  collect old open/private 
 * batch count value form EKStep then update cassandra db and EKStep course 
 * instance count under EKStep.
 * @author Manzarul
 *
 */
public class CourseBatchSchedulerUtil {
  public static Map<String,String> headerMap = new HashMap<>();
  static {
    String header = System.getenv(JsonKey.EKSTEP_AUTHORIZATION);
    if (ProjectUtil.isStringNullOREmpty(header)) {
      header = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION);
    } else {
      header = JsonKey.BEARER+header;
    }
     headerMap.put(JsonKey.AUTHORIZATION, header);
     headerMap.put("Content-Type", "application/json");
  }
  /**
   * 
   * @param startDate
   * @param endDate
   * @return
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> getBatchDetailsFromES(String startDate,
      String endDate) {
    ProjectLogger.log("method call start to collect get course batch data -"
        + startDate + " " + endDate);
    Map<String, Object> response = new HashMap<>();
    SearchDTO dto = new SearchDTO();
    Map<String, Object> map = new HashMap<>();
    Map<String , String> dateRangeFilter = new HashMap<>();
    dateRangeFilter.put("<=" , startDate);
    map.put(JsonKey.START_DATE , dateRangeFilter);
    map.put(JsonKey.COUNTER_INCREMENT_STATUS, false);
    dto.addAdditionalProperty(JsonKey.FILTERS, map);
    List<Map<String, Object>> listOfMap = new ArrayList<>();
    List<Map<String, Object>> endBatchMap = new ArrayList<>();
    Map<String, Object> responseMap = ElasticSearchUtil.complexSearch(dto,
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.course.getTypeName());
    if (responseMap != null && responseMap.size() > 0) {
      Object val = responseMap.get(JsonKey.CONTENT);
      if (val != null) {
        listOfMap = (List<Map<String, Object>>) val;
      } else {
        ProjectLogger
            .log("No data found for start date course batch===" + startDate);
      }
    } else {
      ProjectLogger
          .log("No data found for start date course batch===" + startDate);
    }
    response.put(JsonKey.START_DATE, listOfMap);
    map.clear();
    dateRangeFilter.clear();
    dateRangeFilter.put("<=" , (String)endDate);
    map.put(JsonKey.END_DATE , dateRangeFilter);
    map.put(JsonKey.COUNTER_DECREMENT_STATUS, false);
    dto.addAdditionalProperty(JsonKey.FILTERS, map);
    responseMap = ElasticSearchUtil.complexSearch(dto,
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.course.getTypeName());
    if (responseMap != null && responseMap.size() > 0) {
      Object val = responseMap.get(JsonKey.CONTENT);
      if (val != null) {
        endBatchMap = (List<Map<String, Object>>) val;
      } else {
        ProjectLogger
            .log("No data found for end date course batch===" + endDate);
      }
    } else {
      ProjectLogger.log("No data found for end date course batch===" + endDate);
    }
    response.put(JsonKey.END_DATE, endBatchMap);
    ProjectLogger.log("method call end to collect get course batch data -"
        + startDate + " " + endDate);
    return response;
  }
  
 /**
  *  
  * @param value
  */
  public static void updateCourseBatchDbStatus(Map<String,Object> map,Boolean increment) {
    ProjectLogger.log("updating course batch details start");
    CassandraOperation cassandraOperation = new CassandraOperationImpl();
    Util.DbInfo courseBatchDBInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    try{
      String response = doOperationInEkStepCourse((String)map.get(JsonKey.COURSE_ID),increment,(String)map.get(JsonKey.ENROLLMENT_TYPE));
      if(response.equals(JsonKey.SUCCESS)){
        boolean flag = updateDataIntoES(map);
        if(flag){
          cassandraOperation.updateRecord(courseBatchDBInfo.getKeySpace(), courseBatchDBInfo.getTableName(), map);
        }
      }else{
        ProjectLogger.log("Ekstep content updatation failed.");
      }
    }catch(Exception e){
      ProjectLogger.log("Exception occured while savin data to course batch db ", e);
    }
  }
  
  /**
   * 
   * @param val
   */
  public static boolean updateDataIntoES (Map<String,Object> map) {
    Boolean flag = false;
    try{
      flag =  ElasticSearchUtil.updateData(ProjectUtil.EsIndex.sunbird.getIndexName(), 
          ProjectUtil.EsType.course.getTypeName(), (String)map.get(JsonKey.ID), map) ;
    }catch(Exception e){
      ProjectLogger.log("Exception occured while saving course batch data to ES",e);
    }
    return flag;
  }
  
  public static String doOperationInEkStepCourse (String courseId, boolean increment,String enrollmentType ) {
    String name = System.getenv(JsonKey.SUNBIRD_INSTALLATION) == null ? PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_INSTALLATION) : System.getenv("sunbird.installation");
    String contentName = "";
    String response = "";
    if(enrollmentType.equals(ProjectUtil.EnrolmentType.open.getVal())){
      contentName = "c_"+name+"_open_batch_count";
    }else{
      contentName = "c_"+name+"_private_batch_count";
    }
    //collect data from EKStep.
    Map<String, Object> ekStepContent =
        CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, headerMap);
    if(ekStepContent != null && ekStepContent.size()>0) {
       int val = (int) ekStepContent.getOrDefault(contentName, 0);
       if(increment){
         val = val +1;
       }else{
         if(val != 0)
           val = val -1;
       }
      try {
        ProjectLogger.log("updating content details to Ekstep start");
        String contentUpdateUrl = System.getenv(JsonKey.EKSTEP_CONTENT_UPDATE_URL);
        if(ProjectUtil.isStringNullOREmpty(contentUpdateUrl)){
          contentUpdateUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_UPDATE_URL);
        }
          response = HttpUtil.sendPatchRequest(contentUpdateUrl+courseId, 
              "{\"request\": {\"content\": {\""+contentName+"\": "+val+"}}}", headerMap);
         ProjectLogger.log("batch count update response=="+response + " " + courseId);
      } catch (IOException e) {
        ProjectLogger.log("Error while updating content value "+e.getMessage() ,e);
      }
    } else {
      ProjectLogger.log("EKstep content not found for course id==" + courseId);
    }
    return response;
  }
  
}
