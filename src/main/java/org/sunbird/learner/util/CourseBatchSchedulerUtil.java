/**
 * 
 */
package org.sunbird.learner.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.dto.SearchDTO;

/**
 * @author Manzarul
 *
 */
public class CourseBatchSchedulerUtil {
  private static  Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
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
    dto.addAdditionalProperty(JsonKey.FILTER, map);
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
    dto.addAdditionalProperty(JsonKey.FILTER, map);
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
    
  }
  
}
