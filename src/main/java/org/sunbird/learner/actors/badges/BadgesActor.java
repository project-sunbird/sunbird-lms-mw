/**
 * 
 */
package org.sunbird.learner.actors.badges;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.BackgroundJobManager;
import org.sunbird.learner.actors.assessment.AssessmentUtil;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;

/**
 * @author Manzarul
 *
 */
public class BadgesActor  extends UntypedAbstractActor {
  


  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo badgesDbInfo = Util.dbInfoMap.get(JsonKey.BADGES_DB);
  private Util.DbInfo userBadgesDbInfo = Util.dbInfoMap.get(JsonKey.USER_BADGES_DB);
  private ActorRef backGroundActorRef;

  public BadgesActor() {
    backGroundActorRef = getContext().actorOf(Props.create(BackgroundJobManager.class), "backGroundActor");
   }
  @Override
  public void onReceive(Object message) throws Throwable {
      if (message instanceof Request) {
          try {
              ProjectLogger.log("AssessmentItemActor onReceive called");
              Request actorMessage = (Request) message;
              if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_ALL_BADGE.getValue())) {
                  getBadges(actorMessage);
              } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ADD_USER_BADGE.getValue())) {
                  saveUserBadges(actorMessage);
              } else {
                  ProjectLogger.log("UNSUPPORTED OPERATION");
                  ProjectCommonException exception = new ProjectCommonException(
                          ResponseCode.invalidOperationName.getErrorCode(),
                          ResponseCode.invalidOperationName.getErrorMessage(),
                          ResponseCode.CLIENT_ERROR.getResponseCode());
                  sender().tell(exception, self());
              }
          }catch(Exception ex){
              ProjectLogger.log(ex.getMessage(), ex);
              sender().tell(ex , self());
          }
      } else {
          // Throw exception as message body
          ProjectLogger.log("UNSUPPORTED MESSAGE");
          ProjectCommonException exception = new ProjectCommonException(
                  ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
      }

  }

  @SuppressWarnings("unchecked")
  private void saveUserBadges(Request actorMessage) {
      Map<String, Object> req = actorMessage.getRequest();
      Response assmntResponse = new Response();
        String receiverId = (String) req.get(JsonKey.RECEIVER_ID);
        String badgeTypeId  =(String) req.get(JsonKey.BADGE_TYPE_ID);
        Map<String,Object> map = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), receiverId);
        if(map == null || map.size()==0) {
          ProjectCommonException ex = new ProjectCommonException(ResponseCode.invalidReceiverId.getErrorCode(), ResponseCode.invalidReceiverId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(ex, self());
          return;
        }
        
        Response response = cassandraOperation.getRecordById(badgesDbInfo.getKeySpace(), badgesDbInfo.getTableName(), badgeTypeId);
        if(response != null && response.get(JsonKey.RESPONSE)!=null)  {
          List<Map<String,Object>> badgesListMap =(List<Map<String,Object>>)response.get(JsonKey.RESPONSE);
          if(badgesListMap == null || badgesListMap.size()==0) {
            ProjectCommonException ex = new ProjectCommonException(ResponseCode.invalidBadgeTypeId.getErrorCode(), ResponseCode.invalidBadgeTypeId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(ex, self());
            return;
          }
         }else {
           ProjectCommonException ex = new ProjectCommonException(ResponseCode.invalidBadgeTypeId.getErrorCode(), ResponseCode.invalidBadgeTypeId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
           sender().tell(ex, self());
           return; 
         }
        try{
          map = new HashMap<>();
          map.put(JsonKey.RECEIVER_ID, receiverId);
          map.put(JsonKey.BADGE_TYPE_ID, (String) req.get(JsonKey.BADGE_TYPE_ID));
          map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv()));
          map.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
          map.put(JsonKey.CREATED_BY, (String)req.get(JsonKey.REQUESTED_BY));
          cassandraOperation.insertRecord(userBadgesDbInfo.getKeySpace(), userBadgesDbInfo.getTableName(),map);
          assmntResponse.put(JsonKey.ID, map.get(JsonKey.ID));
          assmntResponse.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
      }catch(Exception e){
          assmntResponse.put(JsonKey.RESPONSE, JsonKey.FAILURE);
          sender().tell(assmntResponse, self());
      }
       sender().tell(assmntResponse, self());
      try {
         ProjectLogger.log("Start background job to save user badge.");
         Response bckResp = new Response();
         bckResp.getResult().put(JsonKey.OPERATION, ActorOperations.ADD_USER_BADGE_BKG.getValue());
         bckResp.getResult().put(JsonKey.RECEIVER_ID, receiverId);
         backGroundActorRef.tell(bckResp,self());
      } catch (Exception ex) {
        ProjectLogger.log("Exception Occured during saving user badges to ES : ", ex);
      }
  }

  
  @SuppressWarnings("unchecked")
  private void getBadges(Request actorMessage) {
    Map<String, Object> req = actorMessage.getRequest();
    try {
      Response response = cassandraOperation.getAllRecords(
          badgesDbInfo.getKeySpace(), badgesDbInfo.getTableName());
      sender().tell(response, self());
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
      sender().tell(e, self());
    }

  }



}
