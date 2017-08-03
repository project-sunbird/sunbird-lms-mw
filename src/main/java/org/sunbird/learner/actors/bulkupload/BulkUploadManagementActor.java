package org.sunbird.learner.actors.bulkupload;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

public class BulkUploadManagementActor extends UntypedAbstractActor {

private CassandraOperation cassandraOperation = new CassandraOperationImpl();
  Util.DbInfo  bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);

  private ActorRef bulkUploadBackGroundJobActorRef;

  public BulkUploadManagementActor() {
    bulkUploadBackGroundJobActorRef = getContext().actorOf(Props.create(BulkUploadBackGroundJobActor.class), 
        "bulkUploadBackGroundJobActor");
   }
  
  @Override
  public void onReceive(Object message) throws Throwable {

    if (message instanceof Request) {
      try {
        ProjectLogger.log("BulkUploadManagementActor onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.BULK_UPLOAD.getValue())) {
          upload(actorMessage);
        }else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
          ProjectCommonException exception = new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
              ResponseCode.invalidOperationName.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    }else {
      // Throw exception as message body
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  @SuppressWarnings("unchecked")
  private void upload(Request actorMessage) {
    String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.DATA);
    if(((String)req.get(JsonKey.OBJECT_TYPE)).equals(JsonKey.USER)){
      processBulkUserUpload(req,processId);
    }
    
  }

  @SuppressWarnings("unchecked")
  private void processBulkUserUpload(Map<String, Object> req,String processId) {
    
    DbInfo orgDb = Util.dbInfoMap.get(JsonKey.ORG_DB);
    String orgId = "";
    Response response = null;
    if (!ProjectUtil.isStringNullOREmpty((String) req.get(JsonKey.ORGANISATION_ID))){
      response = cassandraOperation.getRecordById(orgDb.getKeySpace(), orgDb.getTableName(), 
          (String) req.get(JsonKey.ORGANISATION_ID));
    }else{
      Map<String,Object> map = new HashMap<>();
      map.put(JsonKey.EXTERNAL_ID, req.get(JsonKey.EXTERNAL_ID));
      map.put(JsonKey.PROVIDER, req.get(JsonKey.PROVIDER));
      response = cassandraOperation.getRecordsByProperties(orgDb.getKeySpace(), orgDb.getTableName(), map);
    }
    List<Map<String,Object>> responseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    
    if(responseList.isEmpty()){
      throw  new ProjectCommonException(
          ResponseCode.invalidOrgData.getErrorCode(),
          ResponseCode.invalidOrgData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      } else{
        orgId = (String) responseList.get(0).get(JsonKey.ID);
      }
    File file = new File("bulk.csv");
    FileOutputStream fos = null;
    try {
       fos = new FileOutputStream(file);
       fos.write( (byte[]) req.get(JsonKey.FILE));
    } catch (IOException e) {
      ProjectLogger.log("Exception Occurred while reading file in BulkUploadManagementActor", e);
    }finally{
      try {
        fos.close();
      } catch (IOException e) {
        ProjectLogger.log("Exception Occurred while closing fileInputStream in BulkUploadManagementActor", e);
      }
    }
    
    List<String[]> userList = parseCsvFile(file);
    if (null != userList ) {
        if (userList.size() > 201) {
          throw  new ProjectCommonException(
              ResponseCode.dataSizeError.getErrorCode(),
              ResponseCode.dataSizeError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if(!userList.isEmpty()){
          String[] columns = userList.get(0);
          validateUserProperty(columns);
        }else{
          throw  new ProjectCommonException(
              ResponseCode.dataSizeError.getErrorCode(),
              ResponseCode.dataSizeError.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }else{
      throw new ProjectCommonException(
          ResponseCode.dataSizeError.getErrorCode(),
          ResponseCode.dataSizeError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    //save csv file to db
    uploadCsvToDB(userList,processId,orgId,JsonKey.USER,(String)req.get(JsonKey.REQUESTED_BY));
  }

  private void uploadCsvToDB(List<String[]> dataList, String processId, String orgId, String objectType, String requestedBy) {
    List<Map<String,Object>> userMapList = new ArrayList<>();
    if (dataList.size() > 1) {
      String[] columnArr = dataList.get(0);
      Map<String,Object> userMap = null;
      for(int i = 1 ; i < dataList.size() ; i++){
        userMap = new HashMap<>();
        String[] valueArr = dataList.get(i);
        for(int j = 0 ; j < valueArr.length ; j++){
            userMap.put(columnArr[j], valueArr[j]);
         }
        if(!ProjectUtil.isStringNullOREmpty(objectType) && objectType.equalsIgnoreCase(JsonKey.USER)){
          userMap.put(JsonKey.REGISTERED_ORG_ID, orgId);
        }
        userMapList.add(userMap);
      }
    }
    //convert userMapList to json string 
    Map<String,Object> map = new HashMap<>();
    
    ObjectMapper mapper = new ObjectMapper();
    try {
      map.put(JsonKey.DATA,
          mapper.writeValueAsString(userMapList));
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    
    
    map.put(JsonKey.ID, processId);
    map.put(JsonKey.OBJECT_TYPE, objectType);
    map.put(JsonKey.UPLOADED_BY, requestedBy);
    map.put(JsonKey.UPLOADED_DATE, ProjectUtil.getFormattedDate());
    map.put(JsonKey.PROCESS_START_TIME, ProjectUtil.getFormattedDate());
    map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.NEW.getValue());
    Response res = cassandraOperation.insertRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    res.put(JsonKey.PROCESS_ID, processId);
    sender().tell(res, self());
    if(((String)res.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
    //send processId for data processing to background job
      Request request = new Request();
      request.put(JsonKey.PROCESS_ID, processId);
      request.setOperation(ActorOperations.PROCESS_BULK_UPLOAD.getValue());
      bulkUploadBackGroundJobActorRef.tell(request, self());
    }
  }

  private List<String[]> parseCsvFile(File file) {
    CSVReader csvReader = null;
    //Create List for holding objects
    List<String[]> rows = new ArrayList<>();
    try
    {
    //Reading the csv file
      csvReader = new CSVReader(new FileReader(file),',','"',0);
      String [] nextLine;
      //Read one line at a time
      while ((nextLine = csvReader.readNext()) != null) 
      {
          List<String> list = new ArrayList<>();
          for(String token : nextLine)
          {
            list.add(token);
          }
          rows.add(list.toArray(list.toArray(new String[nextLine.length])));
      }
    }catch(Exception e){
      ProjectLogger.log("Exception occured while processing csv file : ", e);
    }finally
    {
      try
      {
          //closing the reader
          csvReader.close();
          file.delete();
      }
      catch(Exception e)
      {
        ProjectLogger.log("Exception occured while closing csv reader : ", e);
      }
    }
    return rows;
  }

  private void validateUserProperty(String[] property) {
    ArrayList<String> properties = new ArrayList<>(
        Arrays.asList(JsonKey.FIRST_NAME.toLowerCase(), JsonKey.LAST_NAME.toLowerCase(), 
            JsonKey.PHONE.toLowerCase(),JsonKey.EMAIL.toLowerCase(),
            JsonKey.PASSWORD.toLowerCase(),JsonKey.USERNAME.toLowerCase(),
            JsonKey.PROVIDER.toLowerCase(),JsonKey.PHONE_VERIFIED.toLowerCase(),
            JsonKey.EMAIL_VERIFIED.toLowerCase(),JsonKey.ROLES.toLowerCase()));
    
    for(String key : property){
      if(! properties.contains(key.toLowerCase())){
        throw new ProjectCommonException(ResponseCode.InvalidColumnError.getErrorCode(),
            ResponseCode.InvalidColumnError.getErrorMessage(), 
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    
  }
  
  
 
}
