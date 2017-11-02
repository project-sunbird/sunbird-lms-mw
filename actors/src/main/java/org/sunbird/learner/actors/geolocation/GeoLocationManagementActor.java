package org.sunbird.learner.actors.geolocation;

import akka.actor.UntypedAbstractActor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

/**
 * Created by arvind on 31/10/17.
 */
public class GeoLocationManagementActor extends UntypedAbstractActor {

  Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
  private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("GeoLocationManagementActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.CREATE_GEO_LOCATION.getValue())) {
          createGeoLocation(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_GEO_LOCATION.getValue())) {
          getGeoLocation(actorMessage);
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
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void getGeoLocation(Request actorMessage) {

    ProjectLogger.log("GeoLocationManagementActor-getGeoLocation called");
    String requestedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
    String id = (String) actorMessage.getRequest().get(JsonKey.ID);
    String type = (String) actorMessage.getRequest().get(JsonKey.TYPE);
    Response finalResponse = new Response();

    if(ProjectUtil.isStringNullOREmpty(id) || ProjectUtil.isStringNullOREmpty(type)){
      throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    if(type.equalsIgnoreCase(JsonKey.ORGANISATION)){

      Response response1 = cassandraOperation.getRecordsByProperty(geoLocationDbInfo.getKeySpace(), geoLocationDbInfo.getTableName(), JsonKey.ROOT_ORG_ID, id);
      List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);

      finalResponse.put(JsonKey.RESPONSE , list);

      sender().tell(finalResponse , self());
      return;

    }else if(type.equalsIgnoreCase(JsonKey.LOCATION)){

      Response response1 = cassandraOperation.getRecordById(geoLocationDbInfo.getKeySpace(), geoLocationDbInfo.getTableName(), id);
      List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);
      finalResponse.put(JsonKey.RESPONSE , list);
      sender().tell(finalResponse , self());
      return;
    }
  }

  private void createGeoLocation(Request actorMessage) {

    ProjectLogger.log("GeoLocationManagementActor-createGeoLocation called");
    List<Map<String, Object>> dataList = (List<Map<String, Object>>) actorMessage.getRequest().get(
        JsonKey.DATA);

    Response finalResponse = new Response();
    List<Map<String , Object>> responseList = new ArrayList<>();
    String requestedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
    String rootOrgId = (String) actorMessage.getRequest().get(JsonKey.ROOT_ORG_ID);


    if(ProjectUtil.isStringNullOREmpty(rootOrgId)){
      // throw invalid ord id ,org id should not be null or empty .
      throw  new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
          ResponseCode.invalidOrgId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    //check whether org exist or not
    Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
        orgDbInfo.getTableName(), rootOrgId);
    List<Map<String, Object>> orglist = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if(orglist.isEmpty()){
      throw  new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
          ResponseCode.invalidOrgId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if(dataList.isEmpty()){
      // no need to do anything throw exception invalid request data as list is empty
      throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
          ResponseCode.invalidRequestData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    for(Map<String , Object> dataMap: dataList){

      String location = (String) dataMap.get(JsonKey.LOCATION);
      String type = (String)dataMap.get(JsonKey.TYPE);
      if(ProjectUtil.isStringNullOREmpty(location)){
        continue;
      }

      String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());

      Map<String , Object> dbMap = new HashMap<>();

      dbMap.put(JsonKey.ID , id);
      dbMap.put(JsonKey.CREATED_DATE , format.format(new Date()));
      dbMap.put(JsonKey.CREATED_BY , requestedBy);
      dbMap.put(JsonKey.ROOT_ORG_ID , rootOrgId);
      dbMap.put(JsonKey.LOCATION , location);
      dbMap.put(JsonKey.TOPIC , id);

      cassandraOperation.insertRecord(geoLocationDbInfo.getKeySpace() , geoLocationDbInfo.getTableName() , dbMap);

      Map<String , Object> responseMap = new HashMap();
      responseMap.put(JsonKey.ID , id);
      responseMap.put(JsonKey.LOCATION , location);
      responseMap.put(JsonKey.STATUS , JsonKey.SUCCESS);

      responseList.add(responseMap);

    }

    finalResponse.getResult().put(JsonKey.RESPONSE , responseList);
    sender().tell(finalResponse , self());

  }

}
