package org.sunbird.learner.audit.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.audit.AuditLogService;
import org.sunbird.learner.util.AuditOperation;
import org.sunbird.learner.util.UserUtility;
import akka.actor.UntypedAbstractActor;

public class ActorAuditLogServiceImpl extends UntypedAbstractActor implements AuditLogService {
  
  private PropertiesCache cache = PropertiesCache.getInstance();

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("ActorAuditLogServiceImpl  onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.PROCESS_AUDIT_LOG.getValue())) {
          process(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.SEARCH_AUDIT_LOG.getValue())) {
          searchAuditHistory(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
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
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(Request actorMessage) {
    AuditOperation op = (AuditOperation) actorMessage.get(JsonKey.OPERATION);
    Request message = (Request) actorMessage.get(JsonKey.REQUEST);
    Response result = (Response) actorMessage.get(JsonKey.RESPONSE);
    Map<String, Object> requestedData = createAuditLogReqMap(op, message, result);
    Map<String, Object> logRecord = new HashMap<>();
    String objectType = (String) requestedData.get(JsonKey.OBJECT_TYPE);
    Map<String, Object> requestBody = (Map<String, Object>) requestedData.get(JsonKey.REQUEST);
    if (ProjectUtil.ObjectTypes.user.getValue().equalsIgnoreCase(objectType)) {
      requestBody.putAll((Map<String, Object>) requestBody.get(JsonKey.USER));
      requestBody.remove(JsonKey.USER);
      requestBody.remove(JsonKey.REQUESTED_BY);
      logRecord = processData(requestBody, JsonKey.USER_RELATIONS);
    } else if (ProjectUtil.ObjectTypes.organisation.getValue().equalsIgnoreCase(objectType)) {
      requestBody.putAll((Map<String, Object>) requestBody.get(JsonKey.ORGANISATION));
      requestBody.remove(JsonKey.ORGANISATION);
      requestBody.remove(JsonKey.REQUESTED_BY);
      logRecord = processData(requestBody, JsonKey.ORG_RELATIONS);
    } else if (ProjectUtil.ObjectTypes.batch.getValue().equalsIgnoreCase(objectType)) {
      requestBody.putAll((Map<String, Object>) requestBody.get(JsonKey.BATCH));
      requestBody.remove(JsonKey.BATCH);
      requestBody.remove(JsonKey.REQUESTED_BY);
      logRecord = processData(requestBody, JsonKey.BATCH_RELATIONS);
    }
    requestedData.remove(JsonKey.REQUEST);
    requestedData.put(JsonKey.LOG_RECORD, logRecord);
    Map<String, Object> auditLog = AuditLogGenerator.generateLogs(requestedData);
    save(auditLog);
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> createAuditLogReqMap(AuditOperation op, Request message,
      Response result) {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.REQ_ID, message.getRequestId());
    map.put(JsonKey.OBJECT_TYPE, op.getObjectType());
    map.put(JsonKey.OPERATION_TYPE, op.getOperationType());
    map.put(JsonKey.DATE, ProjectUtil.getFormattedDate());
    map.put(JsonKey.USER_ID, message.getRequest().get(JsonKey.REQUESTED_BY));
    map.put(JsonKey.REQUEST, message.getRequest());
    if (message.getOperation().equals(ActorOperations.CREATE_USER.getValue())) {
      map.put(JsonKey.OBJECT_ID, result.get(JsonKey.USER_ID));
    } else if (message.getOperation().equals(ActorOperations.CREATE_ORG.getValue())) {
      map.put(JsonKey.OBJECT_ID, result.get(JsonKey.ORGANISATION_ID));
    } else if (message.getOperation().equals(ActorOperations.CREATE_BATCH.getValue())) {
      map.put(JsonKey.OBJECT_ID, result.get(JsonKey.BATCH_ID));
    } else if (message.getOperation().equals(ActorOperations.CREATE_NOTE.getValue())) {
      map.put(JsonKey.OBJECT_ID, result.get(JsonKey.ID));
    } else if (message.getOperation().equals(ActorOperations.UPDATE_USER.getValue())
        || message.getOperation().equals(ActorOperations.BLOCK_USER.getValue())
        || message.getOperation().equals(ActorOperations.UNBLOCK_USER.getValue())
        || message.getOperation().equals(ActorOperations.ASSIGN_ROLES.getValue())) {
      if (null != result.get(JsonKey.USER_ID)) {
        map.put(JsonKey.OBJECT_ID,
            ((Map<String, Object>) message.getRequest().get(JsonKey.USER)).get(JsonKey.USER_ID));
      } else {
        map.put(JsonKey.OBJECT_ID, message.getRequest().get(JsonKey.ID));
      }
    } else if (message.getOperation().equals(ActorOperations.UPDATE_ORG.getValue())
        || message.getOperation().equals(ActorOperations.UPDATE_ORG_STATUS.getValue())
        || message.getOperation().equals(ActorOperations.APPROVE_ORG.getValue())
        || message.getOperation().equals(ActorOperations.APPROVE_ORGANISATION.getValue())
        || message.getOperation().equals(ActorOperations.JOIN_USER_ORGANISATION.getValue())
        || message.getOperation().equals(ActorOperations.ADD_MEMBER_ORGANISATION.getValue())
        || message.getOperation().equals(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue())
        || message.getOperation().equals(ActorOperations.APPROVE_USER_ORGANISATION.getValue())
        || message.getOperation().equals(ActorOperations.REJECT_USER_ORGANISATION.getValue())) {
      if(!ProjectUtil.isStringNullOREmpty((String)((Map<String, Object>) message.getRequest().get(JsonKey.ORGANISATION))
          .get(JsonKey.ORGANISATION_ID))){
        map.put(JsonKey.OBJECT_ID,
            ((Map<String, Object>) message.getRequest().get(JsonKey.ORGANISATION))
                .get(JsonKey.ORGANISATION_ID));
      }else {
        map.put(JsonKey.OBJECT_ID,
            ((Map<String, Object>) message.getRequest().get(JsonKey.ORGANISATION))
                .get(JsonKey.ID));
      }
    } else if (message.getOperation().equals(ActorOperations.UPDATE_BATCH.getValue())
        || message.getOperation().equals(ActorOperations.REMOVE_BATCH.getValue())
        || message.getOperation().equals(ActorOperations.ADD_USER_TO_BATCH.getValue())
        || message.getOperation().equals(ActorOperations.REMOVE_USER_FROM_BATCH.getValue())) {
      map.put(JsonKey.OBJECT_ID,
          ((Map<String, Object>) message.getRequest().get(JsonKey.BATCH)).get(JsonKey.BATCH_ID));
    } else if (message.getOperation().equals(ActorOperations.UPDATE_NOTE.getValue())
        || message.getOperation().equals(ActorOperations.DELETE_NOTE.getValue())) {
      map.put(JsonKey.OBJECT_ID, message.getRequest().get(JsonKey.NOTE_ID));
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> processData(Map<String, Object> requestBody, String objectProperty) {
    Map<String, Object> relationsMap = new HashMap<>();
    Map<String, Object> logRecords = new HashMap<>();
    String userRelations = PropertiesCache.getInstance().getProperty(objectProperty);
    String[] relations = userRelations.split(",");
    for (String relation : relations) {
      if (requestBody.containsKey(relation)) {
        List<Map<String, Object>> data = new ArrayList<>();
        Object dataObject = requestBody.get(relation);
        if (dataObject instanceof List) {
          data = (List<Map<String, Object>>) dataObject;
        } else if (dataObject instanceof Map) {
          data.add((Map<String, Object>) dataObject);
        }
        relationsMap.put(relation, data);
        requestBody.remove(relation);
      }
    }
    logRecords.put(JsonKey.PROPERTIES, requestBody);
    logRecords.put(JsonKey.RELATIONS, relationsMap);
    return logRecords;
  }

  @Override
  public void save(Map<String, Object> requestedData) {
    String str = ElasticSearchUtil.createData(EsIndex.sunbirdDataAudit.getIndexName(),
        EsType.history.getTypeName(), ProjectUtil.getUniqueIdFromTimestamp(1), requestedData);
  }
  
  @SuppressWarnings("unchecked")
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
      toDate = dateFormat2.format(new Date());
      cal.add(Calendar.DATE, -(Integer.parseInt(cache.getProperty("default_date_range"))));
      Date toDate1 = cal.getTime();    
      fromDate = dateFormat2.format(toDate1);
      map = new HashMap<>();
      try{
      map.put(">=", dateFormat.format(dateFormat.parse(fromDate+" 23:59:59:000+0530")));
      map.put("<=", dateFormat.format(dateFormat.parse(toDate+" 23:59:59:000+0530")));
      }catch(ParseException e) {
        ProjectLogger.log("Exception occurred while parsing date ", e);
      }
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
    // Decrypt user data
    List<Map<String,Object>> mapList = ((List<Map<String,Object>>)result.get(JsonKey.CONTENT));
    for(Map<String,Object> dataMap : mapList){
      Map<String,Object> record = (Map<String, Object>) dataMap.get(JsonKey.LOG_RECORD);
      Map<String,Object> relationList = (Map<String, Object>) record.get(JsonKey.RELATIONS);
      Map<String,Object> properties = (Map<String, Object>) record.get(JsonKey.PROPERTIES);
      if(((String)dataMap.get(JsonKey.OBJECT_TYPE)).equalsIgnoreCase(JsonKey.USER)){
        UserUtility.decryptUserData(properties);
        if((null != (List<Map<String, Object>>) relationList.get(JsonKey.ADDRESS)) &&(!((List<Map<String, Object>>) relationList.get(JsonKey.ADDRESS)).isEmpty())){
           UserUtility.decryptUserAddressData((List<Map<String, Object>>) relationList.get(JsonKey.ADDRESS));
        }
      }
    }
    
    Response response = new Response();
    if (result != null) {
      response.put(JsonKey.RESPONSE, result);
    } 
    sender().tell(response, self());
  }

}
