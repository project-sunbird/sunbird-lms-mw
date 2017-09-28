package org.sunbird.learner.actors.search;

import akka.actor.UntypedAbstractActor;
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
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;

/**
 * This class will handle search operation for all different type of index and types
 * 
 * @author Manzarul
 *
 */

public class SearchHandlerActor extends UntypedAbstractActor {

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      ProjectLogger.log("CompositeSearch  onReceive called");
      Request actorMessage = (Request) message;
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

}
