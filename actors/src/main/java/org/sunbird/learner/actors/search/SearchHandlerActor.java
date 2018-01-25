package org.sunbird.learner.actors.search;

import akka.actor.UntypedAbstractActor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.learner.util.TelemetryUtil;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;

/**
 * This class will handle search operation for all different type of index and types
 * 
 * @author Manzarul
 *
 */

public class SearchHandlerActor extends UntypedAbstractActor {

  private String topn = PropertiesCache.getInstance().getProperty(JsonKey.SEARCH_TOP_N);

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      ProjectLogger.log("CompositeSearch  onReceive called");
      Request actorMessage = (Request) message;
      Util.initializeContext(actorMessage, JsonKey.USER);
      //set request id fto thread loacl...
      ExecutionContext.setRequestId(actorMessage.getRequestId());

      if (actorMessage.getOperation()
          .equalsIgnoreCase(ActorOperations.COMPOSITE_SEARCH.getValue())) {
        Map<String, Object> searchQueryMap = actorMessage.getRequest();
        Object objectType =
            ((Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS)).get(JsonKey.OBJECT_TYPE);
        String[] types = null;
        if (objectType != null && objectType instanceof List) {
          List<String> list = (List) objectType;
          types = list.toArray(new String[list.size()]);
        }
        ((Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS)).remove(JsonKey.OBJECT_TYPE);
        String filterObjectType = "";
        for (String type : types) {
          if (EsType.user.getTypeName().equalsIgnoreCase(type)) {
            filterObjectType = EsType.user.getTypeName();
            UserUtility.encryptUserSearchFilterQueryData(searchQueryMap);
          }
        }
        SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
        if (filterObjectType.equalsIgnoreCase(EsType.user.getTypeName())) {
          searchDto.setExcludedFields(Arrays.asList(ProjectUtil.excludes));
        }
        Map<String, Object> result = ElasticSearchUtil.complexSearch(searchDto,
            ProjectUtil.EsIndex.sunbird.getIndexName(), types);
        // Decrypt the data
        if (EsType.user.getTypeName().equalsIgnoreCase(filterObjectType)) {
          List<Map<String, Object>> userMapList =
              (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
          for (Map<String, Object> userMap : userMapList) {
            UserUtility.decryptUserDataFrmES(userMap);
            userMap.remove(JsonKey.ENC_EMAIL);
            userMap.remove(JsonKey.ENC_PHONE);
          }
        }
        Response response = new Response();
        if (result != null) {
          response.put(JsonKey.RESPONSE, result);
        } else {
          result = new HashMap<>();
          response.put(JsonKey.RESPONSE, result);
        }
        sender().tell(response, self());
        // create search telemetry event here ...
        generateSearchTelemetryEvent(searchDto , types, result);
      } else {
        ProjectLogger.log("UNSUPPORTED OPERATION");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                ResponseCode.invalidOperationName.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
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

  private void generateSearchTelemetryEvent(SearchDTO searchDto, String[] types,
      Map<String, Object> result) {

    Map<String, Object> telemetryContext = TelemetryUtil.getTelemetryContext();

    Map<String, Object> params = new HashMap<>();
    params.put(JsonKey.TYPE, String.join(",",types));
    params.put(JsonKey.QUERY , searchDto.getQuery());
    params.put(JsonKey.FILTERS, searchDto.getAdditionalProperties().get(JsonKey.FILTERS));
    params.put(JsonKey.SORT ,searchDto.getSortBy());
    params.put(JsonKey.SIZE , result.get(JsonKey.COUNT));
    params.put(JsonKey.TOPN ,generateTopnResult(result)); // need to get topn value from response

    //
    Request req = new Request();
    req.setRequest(telemetryRequestForSearch(telemetryContext, params));
    req.setOperation(ActorOperations.TELEMETRY_PROCESSING.getValue());
    ActorUtil.tell(req);
   // lmaxWriter.submitMessage(req);

  }

  private List<Map<String , Object>> generateTopnResult(Map<String, Object> result) {

    List<Map<String, Object>> userMapList =
        (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
    Integer topN = Integer.parseInt(topn);

    List<Map<String , Object>> list = new ArrayList<>();
    if(topN < userMapList.size()){
      for(int i=0;i<topN; i++){
        Map<String, Object> m = new HashMap<>();
        m.put(JsonKey.ID , userMapList.get(i).get(JsonKey.ID));
        list.add(m);
      }
    }else{

      for(int i=0;i<userMapList.size(); i++){
        Map<String, Object> m = new HashMap<>();
        m.put(JsonKey.ID , userMapList.get(i).get(JsonKey.ID));
        list.add(m);
      }
    }
    return list;
  }

  private static Map<String,Object> telemetryRequestForSearch(Map<String, Object> telemetryContext,
      Map<String, Object> params) {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.CONTEXT , telemetryContext);
    map.put(JsonKey.PARAMS, params);
    map.put(JsonKey.TELEMETRY_EVENT_TYPE , "SEARCH");
    return map;
  }

}
