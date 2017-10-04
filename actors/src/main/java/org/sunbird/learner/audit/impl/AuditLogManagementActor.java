package org.sunbird.learner.audit.impl;

import akka.actor.UntypedAbstractActor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.Months;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
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
  
  private void searchAuditHistory(Request actorMessage){
    Map<String,Object> filters = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.FILTERS);
    SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");
    SimpleDateFormat dateFormat2= new SimpleDateFormat("yyyy-MM-dd");
    dateFormat.setLenient(false);
    String fromDate = (String) filters.get("fromDate");
    String toDate = (String) filters.get("toDate");
    Map<String,Object> map = null;
    Calendar cal = Calendar.getInstance();
    if(ProjectUtil.isStringNullOREmpty(fromDate) && ProjectUtil.isStringNullOREmpty(toDate)){
      toDate = dateFormat.format(new Date());
      cal.add(Calendar.DATE, -(Integer.parseInt(cache.getProperty("default_date_range"))));
      Date toDate1 = cal.getTime();    
      fromDate = dateFormat.format(toDate1);
      map = new HashMap<>();
      map.put(">=", fromDate);
      map.put("<=", toDate);
      filters.put(JsonKey.DATE,map);
    }else if(!ProjectUtil.isStringNullOREmpty(fromDate) && !ProjectUtil.isStringNullOREmpty(toDate)){
      Date date1 = null;
      Date date2 = null;
      try {
        date1 = dateFormat2.parse(fromDate);
        date2 = dateFormat.parse(toDate+" 23:59:59:000+0530");
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
          map.put(">=", dateFormat.format(date1));
          map.put("<=", dateFormat.format(date2));
        
        filters.put(JsonKey.DATE,map);
      }
    }else if(!ProjectUtil.isStringNullOREmpty(fromDate) && ProjectUtil.isStringNullOREmpty(toDate)){
      cal.add(Calendar.DATE, Integer.parseInt(cache.getProperty("default_date_range")));
      Date todate = cal.getTime(); 
      
      map = new HashMap<>();
      try {
        map.put(">=", dateFormat.format(dateFormat2.parse(fromDate)));
      } catch (ParseException e) {
        ProjectLogger.log("Exception occurred while parsing date ", e);
      }
      map.put("<=", dateFormat.format(todate));
      filters.put(JsonKey.DATE,map);
    }else if(ProjectUtil.isStringNullOREmpty(fromDate) && !ProjectUtil.isStringNullOREmpty(toDate)){
      try {
        map = new HashMap<>();
        map.put(">=", dateFormat.format(dateFormat2.parse(toDate)));
        map.put("<=", dateFormat.format(dateFormat.parse(toDate+" 23:59:59:000+0530")));
        filters.put(JsonKey.DATE,map);
      } catch (ParseException e) {
        ProjectLogger.log("Exception occurred while parsing date ", e);
      }
    }
    
    SearchDTO searchDTO = new SearchDTO();
    filters.remove("fromDate");
    filters.remove("toDate");
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS , filters);
    Map<String, Object> result =  ElasticSearchUtil.complexSearch(searchDTO , EsIndex.sunbirdDataAudit.getIndexName(), EsType.history.getTypeName());
    Response response = new Response();
    if (result != null) {
      response.put(JsonKey.RESPONSE, result);
    } else {
      result = new HashMap<>();
      response.put(JsonKey.RESPONSE, result);
    }
    sender().tell(response, self());
  }

}
