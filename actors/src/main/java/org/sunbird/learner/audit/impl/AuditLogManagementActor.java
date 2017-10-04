package org.sunbird.learner.audit.impl;

import akka.actor.UntypedAbstractActor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.Months;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;

public class AuditLogManagementActor extends UntypedAbstractActor {
  private PropertiesCache cache = PropertiesCache.getInstance();
  
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("AuditLogManagementActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.SEARCH_AUDIT_LOG.getValue())) {
          searchAuditHistory(actorMessage);
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
  
  private Map<String,Object> searchAuditHistory(Request actorMessage){
    Map<String,Object> filters = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.FILTERS);
    SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd");
    dateFormat.setLenient(false);
    String fromDate = (String) filters.get("fromDate");
    String toDate = (String) filters.get("toDate");
    Map<String,Object> map = null;
    if(ProjectUtil.isStringNullOREmpty(fromDate) && ProjectUtil.isStringNullOREmpty(toDate)){
      toDate = dateFormat.format(new Date());
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DATE, -(Integer.parseInt(cache.getProperty("default_date_range"))));
      Date toDate1 = cal.getTime();    
      fromDate = dateFormat.format(toDate1);
      map = new HashMap<>();
      map.put(">=", fromDate);
      map.put("<=", toDate);
      filters.put(JsonKey.DATE,map);
      filters.remove(fromDate);
      filters.remove(toDate);
    }else if(!ProjectUtil.isStringNullOREmpty(fromDate) && !ProjectUtil.isStringNullOREmpty(toDate)){
      Date date1 = null;
      Date date2 = null;
      try {
        date1 = dateFormat.parse(fromDate);
        date2 = dateFormat.parse(toDate);
      } catch (ParseException e) {
        ProjectLogger.log("Exception occurred while parsing date ", e);
      }
      DateTime d1 = new DateTime(date1);
      DateTime d2 = new DateTime(date2);
      Months d = Months.monthsBetween(d1,d2);
      int monthsDiff = d.getMonths();
      if(monthsDiff > 3){
        throw new ProjectCommonException(ResponseCode.invalidDateRange.getErrorCode(),
            ResponseCode.invalidDateRange.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }else {
        map = new HashMap<>();
        map.put(">=", fromDate);
        map.put("<=", toDate);
        filters.put(JsonKey.DATE,map);
        filters.remove(fromDate);
        filters.remove(toDate);
      }
    }else if(!ProjectUtil.isStringNullOREmpty(fromDate) && ProjectUtil.isStringNullOREmpty(toDate)){
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DATE, Integer.parseInt(cache.getProperty("default_date_range")));
      Date todate = cal.getTime();    
      map = new HashMap<>();
      map.put(">=", fromDate);
      map.put("<=", todate);
      filters.put(JsonKey.DATE,map);
      filters.remove(fromDate);
      filters.remove(toDate);
    }else if(ProjectUtil.isStringNullOREmpty(fromDate) && !ProjectUtil.isStringNullOREmpty(toDate)){
      filters.put(JsonKey.DATE,toDate);
      filters.remove(fromDate);
      filters.remove(toDate);
    }
    
    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS , filters);
    return ElasticSearchUtil.complexSearch(searchDTO , "","");
  }

}
