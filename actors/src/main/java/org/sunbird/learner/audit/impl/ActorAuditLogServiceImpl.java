package org.sunbird.learner.audit.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.learner.audit.AuditLogService;

public class ActorAuditLogServiceImpl implements AuditLogService{

  @SuppressWarnings("unchecked")
  @Override
  public void process(Map<String, Object> requestedData) {
    Map<String, Object> logRecord = new HashMap<>();
    String objectType = (String) requestedData.get(JsonKey.OBJECT_TYPE);
    Map<String, Object> requestBody = (Map<String, Object>) requestedData.get(JsonKey.REQUEST);
    if(ProjectUtil.ObjectTypes.user.getValue().equalsIgnoreCase(objectType)){
      logRecord = processData(requestBody, JsonKey.USER_RELATIONS);
    } else if(ProjectUtil.ObjectTypes.organisation.getValue().equalsIgnoreCase(objectType)){
      logRecord = processData(requestBody, JsonKey.ORG_RELATIONS);
    } else if(ProjectUtil.ObjectTypes.batch.getValue().equalsIgnoreCase(objectType)){
      logRecord = processData(requestBody, JsonKey.BATCH_RELATIONS);
    }
    requestedData.remove(JsonKey.REQUEST);
    requestedData.put(JsonKey.LOG_RECORD, logRecord);
    Map<String,Object> auditLog = AuditLogGenerator.generateLogs(requestedData);
    save(auditLog);
  }
  
  @SuppressWarnings("unchecked")
  private Map<String, Object> processData(Map<String,Object> requestBody, String objectProperty){
     Map<String,Object> relationsMap = new HashMap<>();
     Map<String,Object> logRecords = new HashMap<>();
     String userRelations = PropertiesCache.getInstance().getProperty(objectProperty);
     String[] relations = userRelations.split(",");
     for(String relation:relations){
       if(requestBody.containsKey(relation)){
         List<Map<String,Object>> data = new ArrayList<>();
         Object dataObject = requestBody.get(relation);
         if(dataObject instanceof List){
           data = (List<Map<String,Object>>)dataObject;
         } else if(dataObject instanceof Map){
           data.add((Map<String,Object>)dataObject);
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
    ElasticSearchUtil.createData(EsIndex.sunbirdDataAudit.getIndexName(), EsType.history.getTypeName(), (String) requestedData.get(JsonKey.OBJECT_ID), requestedData); 
  }

}
