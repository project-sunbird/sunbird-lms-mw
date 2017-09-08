package org.sunbird.metrics.actors;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.responsecode.ResponseCode;

public class MetricsCache {

  private Map<String, Map<String, Object>> orgCreationCache = new ConcurrentHashMap<>();
  private Map<String, Map<String, Object>> orgConsumptionCache = new ConcurrentHashMap<>();
  private Map<String, Map<String, Object>> courseProgressCache = new ConcurrentHashMap<>();
  private Map<String, Map<String, Object>> courseConsumptionCache = new ConcurrentHashMap<>();


  public Object getData(String operation, String id, String period){
    try {
    switch(operation){
      case JsonKey.OrgCreation :{
        return orgCreationCache.get(id).get(period);
      }
      case JsonKey.OrgConsumption :{
        return orgConsumptionCache.get(id).get(period);
      }
      case JsonKey.CourseProgress :{
        return courseProgressCache.get(id).get(period);
      }
      case JsonKey.CourseConsumption :{
        return courseConsumptionCache.get(id).get(period);
      }
      default : {
        throw new ProjectCommonException(ResponseCode.invalidData.getErrorCode(), ResponseCode.invalidData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    }catch (NullPointerException e) {
      ProjectLogger.log("Error occured", e);
      return null;
    }
  }
  
  public void clearCache(){
    orgCreationCache = new ConcurrentHashMap<>();
    orgConsumptionCache = new ConcurrentHashMap<>();
    courseProgressCache = new ConcurrentHashMap<>();
    courseConsumptionCache = new ConcurrentHashMap<>();
  }
  
  public void setData(String operation, String id, String period, Object data){
    Map<String,Object> periodMap = new HashMap<>();
    periodMap.put(period, data);
    switch (operation){
      case JsonKey.OrgCreation :{
         orgCreationCache.put(id, periodMap);
         break;
      }
      case JsonKey.OrgConsumption :{
        orgConsumptionCache.put(id, periodMap);
        break;
      }
      case JsonKey.CourseProgress :{
        courseProgressCache.put(id, periodMap);
        break;
      }
      case JsonKey.CourseConsumption :{
        courseConsumptionCache.put(id, periodMap);
        break;
      }
      default : {
        throw new ProjectCommonException(ResponseCode.invalidData.getErrorCode(), ResponseCode.invalidData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      } 
    }
  }
}
