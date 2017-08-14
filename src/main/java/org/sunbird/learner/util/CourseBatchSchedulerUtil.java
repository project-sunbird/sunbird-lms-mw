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
  private static  Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
  private static Map<String,String> headerMap = new HashMap<>();
  static {
    String header = System.getenv(JsonKey.EKSTEP_AUTHORIZATION);
    if (ProjectUtil.isStringNullOREmpty(header)) {
      header = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION);
    } else {
      header = JsonKey.BEARER+header;
    }
     headerMap.put(JsonKey.AUTHORIZATION, header);
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
    map.put(JsonKey.START_DATE, startDate);
    map.put(JsonKey.COUNTER_INCREMENT_STATUS, false);
    dto.addAdditionalProperty(JsonKey.FILTERS, map);
    List<Map<String, Object>> listOfMap = new ArrayList<>();
    List<Map<String, Object>> endBatchMap = new ArrayList<>();
    Map<String, Object> responseMap = ElasticSearchUtil.complexSearch(dto,
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.course.getTypeName());
    if (responseMap != null && responseMap.size() > 0) {
      Object val = responseMap.get(JsonKey.RESULT);
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
    map.put(JsonKey.END_DATE, endDate);
    map.put(JsonKey.COUNTER_DECREMENT_STATUS, false);
    dto.addAdditionalProperty(JsonKey.FILTERS, map);
    responseMap = ElasticSearchUtil.complexSearch(dto,
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.course.getTypeName());
    if (responseMap != null && responseMap.size() > 0) {
      Object val = responseMap.get(JsonKey.RESULT);
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
  public static void updateCourseBatchDbStatus(Map<String,Object> value) {
    CassandraOperation cassandraOperation = new CassandraOperationImpl();
    cassandraOperation.updateRecord(userdbInfo.getKeySpace(), userdbInfo.getTableName(), value);
  }
  
  /**
   * 
   * @param val
   */
  public static void updateDateIntoES (Map<String,Object> val) {
    ElasticSearchUtil.updateData(ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.course.getTypeName(), (String)val.get(JsonKey.ID), val) ;
  }
  
  public static void doOperationInEkStepCourse (String contentname, String courseId, boolean increment) {
    
    //collect data from EKStep.
    Map<String, Object> ekStepContent =
        CourseEnrollmentActor.getCourseObjectFromEkStep(courseId, headerMap);
    if(ekStepContent != null && ekStepContent.size()>0) {
       int val = (int) ekStepContent.getOrDefault(contentname, 0);
      try {
           HttpUtil.sendPostRequest("URL", "", headerMap) ;
      } catch (IOException e) {
        ProjectLogger.log("Error while updating content value "+e.getMessage() ,e);
      }
    } else {
      ProjectLogger.log("EKstep content not found for course id==" + courseId);
    }
    
    
  }
  
}
