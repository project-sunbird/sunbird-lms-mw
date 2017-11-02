package org.sunbird.learner.actors.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
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
import org.sunbird.learner.util.Util;

import akka.actor.UntypedAbstractActor;

public class ClientManagementActor extends UntypedAbstractActor {
  
  private Util.DbInfo clientDbInfo = Util.dbInfoMap.get(JsonKey.CLIENT_INFO_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("ClientManagementActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.REGISTER_CLIENT.getValue())) {
          registerClient(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.UPDATE_CLIENT_KEY.getValue())) {
          updateClientKey(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_CLIENT_KEY.getValue())) {
          getClientKey(actorMessage);
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

  /**
   * Method to register client
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void registerClient(Request actorMessage) {
    ProjectLogger.log("Register client method call start");
    String clientName = (String) actorMessage.getRequest().get(JsonKey.CLIENT_NAME);
    Response data = getDataFromCassandra(JsonKey.CLIENT_NAME, clientName);
    List<Map<String,Object>> dataList = (List<Map<String,Object>>) data.getResult().get(JsonKey.RESPONSE);
    if(!dataList.isEmpty() && dataList.get(0).containsKey(JsonKey.ID)){
      throw new ProjectCommonException(ResponseCode.invalidClientName.getErrorCode(),
          ResponseCode.invalidClientName.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String,Object> req = new HashMap<>();
    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    req.put(JsonKey.CLIENT_NAME, StringUtils.remove(clientName.toLowerCase()," "));
    req.put(JsonKey.ID, uniqueId);
    String masterKey = ProjectUtil.createAuthToken(clientName, uniqueId);
    req.put(JsonKey.MASTER_KEY, masterKey);
    req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    req.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    Response result = cassandraOperation.insertRecord(clientDbInfo.getKeySpace(),
        clientDbInfo.getTableName(), req);
    ProjectLogger.log("Client data saved into cassandra.");
    result.getResult().put(JsonKey.CLIENT_ID, uniqueId);
    result.getResult().put(JsonKey.MASTER_KEY, masterKey);
    result.getResult().remove(JsonKey.RESPONSE);
    sender().tell(result, self());
  }

  /**
   * Method to update client's master key based on client id and master key
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void updateClientKey(Request actorMessage) {
    ProjectLogger.log("Update client key method call start");
    String clientId = (String) actorMessage.getRequest().get(JsonKey.CLIENT_ID);
    String masterKey = (String) actorMessage.getRequest().get(JsonKey.MASTER_KEY);
    Response data = getDataFromCassandra(JsonKey.ID, clientId);
    List<Map<String,Object>> dataList = (List<Map<String,Object>>) data.getResult().get(JsonKey.RESPONSE);
    if( dataList.isEmpty() || !StringUtils.equalsIgnoreCase(masterKey,(String)dataList.get(0).get(JsonKey.MASTER_KEY))){
      throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Map<String,Object> req = new HashMap<>();
    req.put(JsonKey.CLIENT_ID, clientId);
    String newMasterKey = ProjectUtil.createAuthToken((String)dataList.get(0).get(JsonKey.CLIENT_NAME), clientId);
    req.put(JsonKey.MASTER_KEY, newMasterKey);
    req.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    req.put(JsonKey.ID, clientId);
    req.remove(JsonKey.CLIENT_ID);
    Response result = cassandraOperation.updateRecord(clientDbInfo.getKeySpace(),
        clientDbInfo.getTableName(), req);
    ProjectLogger.log("Client data updated into cassandra.");
    result.getResult().put(JsonKey.CLIENT_ID, clientId);
    result.getResult().put(JsonKey.MASTER_KEY, newMasterKey);
    result.getResult().remove(JsonKey.RESPONSE);
    sender().tell(result, self());
  }

  /**
   * Method to get Client details
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void getClientKey(Request actorMessage) {
    ProjectLogger.log("Get client key method call start");
    String clientId = (String) actorMessage.getRequest().get(JsonKey.CLIENT_ID);
    Response data = getDataFromCassandra(JsonKey.ID, clientId);
    List<Map<String,Object>> dataList = (List<Map<String,Object>>) data.getResult().get(JsonKey.RESPONSE);
    if(dataList.isEmpty()){
      throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    sender().tell(data, self());
  }
  
  /**
   * Method to get data from cassandra for Client_Info table
   * @param propertyName
   * @param propertyValue
   * @return
   */
  private Response getDataFromCassandra(String propertyName, String propertyValue){
    ProjectLogger.log("Get data from cassandra method call start");
    Response result = null;
    if(StringUtils.equalsIgnoreCase(JsonKey.CLIENT_NAME, propertyName)){
     result = cassandraOperation.getRecordsByProperty(clientDbInfo.getKeySpace(),
        clientDbInfo.getTableName(), JsonKey.CLIENT_NAME, propertyValue.toLowerCase());
    }else if(StringUtils.equalsIgnoreCase(JsonKey.ID, propertyName)){
      result = cassandraOperation.getRecordsByProperty(clientDbInfo.getKeySpace(),
          clientDbInfo.getTableName(), JsonKey.ID, propertyValue);
    }
    if(null == result || result.getResult().isEmpty()){
      throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    } 
    return result;
  }

  
}
