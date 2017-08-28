package org.sunbird.learner.actors.notificationservice;

import akka.actor.UntypedAbstractActor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.mail.SendMail;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

public class EmailServiceActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  @Override
  public void onReceive(Object message) throws Throwable {
    try{
       if (message instanceof Request) {
              ProjectLogger.log("EmailServiceActor  onReceive called");
              Request actorMessage = (Request) message;
                  if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.EMAIL_SERVICE.getValue())) {
                    sendMail(actorMessage);
                  } else {
                  ProjectLogger.log("UNSUPPORTED OPERATION");
                  ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                  sender().tell(exception, self());
              }
          } else {
              ProjectLogger.log("UNSUPPORTED MESSAGE");
              ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
              sender().tell(exception, self());
          } 
       } catch (Exception ex) {
         ProjectLogger.log(ex.getMessage(), ex);
         sender().tell(ex, self());
       }
  }

  private void sendMail(Request actorMessage) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> request =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.EMAIL_REQUEST);
    List<String> emails = (List<String>) request.get(JsonKey.RECIPIENT_EMAILS);
    if(null == emails){
      emails = new ArrayList<>();
    }
    checkEmailValidity(emails.toArray(new String[emails.size()]));
    List<String> emailIds = new ArrayList<>(emails);
    if(null != request.get(JsonKey.RECIPIENT_USERIDS)){
      List<String> userIds = (List<String>) request.get(JsonKey.RECIPIENT_USERIDS);
      if(!userIds.isEmpty()){
      Response response = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), JsonKey.ID, new ArrayList<>(userIds));
      List<Map<String,Object>> respMapList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if(userIds.size() != respMapList.size()){
        Iterator<Map<String, Object>> itr = respMapList.iterator();
        List<String> tempUserIdList = new ArrayList<>();
          while(itr.hasNext()){
            Map<String,Object> map = itr.next();
            tempUserIdList.add((String)map.get(JsonKey.ID));
          }
          for(int i=0;i < userIds.size();i++){
            if(!tempUserIdList.contains((String)userIds.get(i))){
              Response resp = new Response();
              resp.put((String)userIds.get(i), "Invalid UserId.");
              sender().tell(resp, self());
              return;
            }
        }
      }else{
        for(Map<String,Object> map : respMapList){
          emailIds.add((String)map.get(JsonKey.EMAIL));
        }
      }
    }
   }
    SendMail.sendMail(emailIds.toArray(new String[emailIds.size()]), (String)request.get(JsonKey.SUBJECT), ProjectUtil.getContext(request), ProjectUtil.getTemplate(""));
    Response res =  new Response();
    res.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(res, self());
  }

  private void checkEmailValidity(String[] emails) {
    for(String email : emails){
      if(!ProjectUtil.isEmailvalid(email)){
        Response response = new Response();
        response.put(email, ResponseCode.emailFormatError.getErrorMessage());
        sender().tell(response, self());
        return;
      }
    }
    
  }

}
