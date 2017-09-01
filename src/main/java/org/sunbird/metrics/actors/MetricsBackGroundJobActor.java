package org.sunbird.metrics.actors;

import akka.actor.UntypedAbstractActor;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.velocity.VelocityContext;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.ExcelFileUtil;
import org.sunbird.common.models.util.FileUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.ReportTrackingStatus;
import org.sunbird.common.models.util.azure.CloudService;
import org.sunbird.common.models.util.azure.CloudServiceFactory;
import org.sunbird.common.models.util.mail.SendMail;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

/**
 * Created by arvind on 28/8/17.
 */
public class MetricsBackGroundJobActor extends UntypedAbstractActor {

  Util.DbInfo reportTrackingdbInfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static FileUtil fileUtil = new ExcelFileUtil();
  SimpleDateFormat format = ProjectUtil.format;

  @Override
  public void onReceive(Object message) throws Throwable {

    {

      if (message instanceof Request) {
        try {
          ProjectLogger.log("BackgroundJobManager  onReceive called");

          Request actorMessage = (Request) message;
          String requestedOperation = actorMessage.getOperation();
          ProjectLogger.log("Operation name is ==" + requestedOperation);
          if (requestedOperation
              .equalsIgnoreCase(ActorOperations.FILE_UPLOAD_AND_SEND_MAIL.getValue())) {
            fileUploadAndSendMail(actorMessage);
          } else if (requestedOperation
              .equalsIgnoreCase(ActorOperations.SEND_MAIL.getValue())) {
            sendMail(actorMessage);

          } else {
            ProjectCommonException exception = new ProjectCommonException(
                ResponseCode.invalidOperationName.getErrorCode(),
                ResponseCode.invalidOperationName.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
            ProjectLogger.log("UnSupported operation in Background Job Manager", exception);
          }
        }catch (Exception ex){
          ProjectLogger.log(ex.getMessage(), ex);
        }
      } else {
        ProjectLogger.log("UNSUPPORTED MESSAGE FOR BACKGROUND JOB MANAGER");
      }
    }

  }

  private void sendMail(Request request) {

    Map<String , Object> map = request.getRequest();
    String requestId = (String)map.get(JsonKey.REQUEST_ID);

    //TODO: fetch the DB details from database on basis of requestId ....
    Response response = cassandraOperation.getRecordById(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        requestId);
    List<Map<String,Object>> responseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if(responseList.isEmpty()){
      //TODO: throw exception here ...
    }

    Map<String , Object> reportDbInfo = responseList.get(0);
    Map<String , Object> dbReqMap = new HashMap<>();
    dbReqMap.put(JsonKey.ID , requestId);

    if(processMailSending(reportDbInfo)){
      dbReqMap.put(JsonKey.STATUS, ReportTrackingStatus.SENDING_MAIL_SUCCESS.getValue());
      dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
      cassandraOperation
          .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
              dbReqMap);
    }else{
      increasetryCount(reportDbInfo);
      if((Integer)reportDbInfo.get(JsonKey.TRY_COUNT)>3){
        dbReqMap.put(JsonKey.STATUS, ReportTrackingStatus.FAILED.getValue());
        dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
        cassandraOperation
            .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
                dbReqMap);
      }else{
        dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
        cassandraOperation
            .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
                dbReqMap);
      }
    }


  }

  private void increasetryCount(Map<String, Object> map) {
    if(null == map.get(JsonKey.TRY_COUNT)){
      map.put(JsonKey.TRY_COUNT , 0);
    }else{
      Integer tryCount = (Integer) map.get(JsonKey.TRY_COUNT);
      map.put(JsonKey.TRY_COUNT , tryCount+1);
    }
  }

  private void fileUploadAndSendMail(Request request) throws IOException {

    Map<String , Object> map = request.getRequest();
    String requestId = (String)map.get(JsonKey.REQUEST_ID);

    //TODO: fetch the DB details from database on basis of requestId ....
    Response response = cassandraOperation.getRecordById(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        requestId);
    List<Map<String,Object>> responseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if(responseList.isEmpty()){
      //TODO: throw exception here ...
    }
    Map<String , Object> reportDbInfo = responseList.get(0);
    String fileFormat = (String) reportDbInfo.get(JsonKey.FORMAT);

    Map<String , Object> dbReqMap = new HashMap<>();
    dbReqMap.put(JsonKey.ID , requestId);

    dbReqMap.put(JsonKey.STATUS , ReportTrackingStatus.GENERATING_FILE.getValue());
    dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
    cassandraOperation.updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        dbReqMap);

    List<List<Object>> finalList = (List<List<Object>>) map.get(JsonKey.DATA);
    File file = null;
    try {
      file = fileUtil.writeToFile("File-"+requestId,finalList);
    }catch(Exception ex){
      ProjectLogger.log("PROCESS FAILED WHILE CONVERTING THE DATA TO FILE .", ex);
      // update DB as status failed since unable to convert data to file
      dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
      dbReqMap.put(JsonKey.STATUS , ReportTrackingStatus.FAILED.getValue());
      cassandraOperation.updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
          dbReqMap);
      throw ex;
    }

    // TODO: going to upload file , save this info into the DB ...
    dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
    dbReqMap.put(JsonKey.STATUS , ReportTrackingStatus.UPLOADING_FILE.getValue());
    cassandraOperation.updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        dbReqMap);

    String storageUrl=null;
    try {
      storageUrl = processFileUpload(file ,"testContainer");
      //TODO : once upload success update the state in DB along with storage url and then next move to send mail...

    } catch (IOException e) {
      ProjectLogger.log("Error occured while uploading file on storage for requset "+requestId, e);
      increasetryCount(reportDbInfo);
      if((Integer)reportDbInfo.get(JsonKey.TRY_COUNT)>3){
        dbReqMap.put(JsonKey.STATUS, ReportTrackingStatus.FAILED.getValue());
        dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
        cassandraOperation
            .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
                dbReqMap);
      }else{
        dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
        cassandraOperation
            .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
                dbReqMap);
      }
      throw e;
    }finally {
      if(ProjectUtil.isNotNull(file)){
        file.delete();
      }
    }

    reportDbInfo.put(JsonKey.FILE_URL , storageUrl);
    dbReqMap.put(JsonKey.FILE_URL , storageUrl);
    dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
    //TODO : remove below comment later ...
    dbReqMap.put(JsonKey.DATA, null);
    dbReqMap.put(JsonKey.STATUS , ReportTrackingStatus.UPLOADING_FILE_SUCCESS.getValue());
    cassandraOperation.updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        dbReqMap);

    dbReqMap.put(JsonKey.STATUS , ReportTrackingStatus.SENDING_MAIL.getValue());
    dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
    cassandraOperation.updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
        dbReqMap);

    if(processMailSending(reportDbInfo)){
      dbReqMap.put(JsonKey.STATUS, ReportTrackingStatus.SENDING_MAIL_SUCCESS.getValue());
      dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
      cassandraOperation
          .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
              dbReqMap);
    }else{
      increasetryCount(reportDbInfo);
      if((Integer)reportDbInfo.get(JsonKey.TRY_COUNT)>3){
        dbReqMap.put(JsonKey.STATUS, ReportTrackingStatus.FAILED.getValue());
        dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
        cassandraOperation
            .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
                dbReqMap);
      }else{
        dbReqMap.put(JsonKey.UPDATED_DATE , format.format(new Date()));
        cassandraOperation
            .updateRecord(reportTrackingdbInfo.getKeySpace(), reportTrackingdbInfo.getTableName(),
                dbReqMap);
      }
    }
  }

  private boolean processMailSending(Map<String, Object> reportDbInfo) {

    Map<String , Object> templateMap = new HashMap<>();
    templateMap.put("downloadUrl", reportDbInfo.get(JsonKey.FILE_URL));
    templateMap.put("name",reportDbInfo.get(JsonKey.FIRST_NAME));
    templateMap.put("body", "COURSE PROGRESS FOR BATCH "+reportDbInfo.get(JsonKey.BATCH_ID)+"for Period  "+reportDbInfo.get(JsonKey.PERIOD)+" Requested date : "+reportDbInfo.get(JsonKey.CREATED_DATE));
    templateMap.put("thanks", "Thanks.");

    ProjectUtil.getContext(templateMap);

    VelocityContext context = new VelocityContext();
    context.put("downloadUrl", reportDbInfo.get(JsonKey.FILE_URL));
    context.put("name",reportDbInfo.get(JsonKey.FIRST_NAME));
    context.put("body", "COURSE PROGRESS FOR BATCH "+reportDbInfo.get(JsonKey.BATCH_ID)+"for Period  "+reportDbInfo.get(JsonKey.PERIOD)+" Requested date : "+reportDbInfo.get(JsonKey.CREATED_DATE));
    context.put("thanks", "Thanks.");
    return SendMail.sendMail(new String[]{(String)reportDbInfo.get(JsonKey.EMAIL)},"COURSE PROGRESS FOR BATCH "+reportDbInfo.get(JsonKey.BATCH_ID) ,context,ProjectUtil.getTemplate("defaultTemplate"));
  }

  private String processFileUpload(File file, String container)
      throws IOException {

    String storageUrl = null;
    try {
      CloudService service = (CloudService) CloudServiceFactory.get("Azure");
      if(null == service){
        ProjectLogger.log("The cloud service is not available");
        throw new ProjectCommonException(
            ResponseCode.invalidRequestData.getErrorCode(),
            ResponseCode.invalidRequestData.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
        //sender().tell(exception, self());
      }
      storageUrl = service.uploadFile(container , file);
    } catch (Exception e) {
      ProjectLogger.log("Exception Occurred while reading file in FileUploadServiceActor", e);
      throw e;
    }
    return storageUrl;

  }
}
