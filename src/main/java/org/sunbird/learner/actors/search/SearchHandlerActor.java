package org.sunbird.learner.actors.search;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.Util;

import akka.actor.UntypedAbstractActor;

/**
 * This class will handle search operation for all different type
 * of index and types
 * @author Manzarul
 *
 */

public class SearchHandlerActor extends UntypedAbstractActor {
    private LogHelper logger = LogHelper.getInstance(SearchHandlerActor.class.getName());

	@Override
	public void onReceive(Object message) throws Throwable {
		 if (message instanceof Request) {
	            logger.debug("CompositeSearch  onReceive called");
	            Request actorMessage = (Request) message;
	            if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.COMPOSITE_SEARCH.getValue())) {
	              Map<String , Object> searchQueryMap = actorMessage.getRequest();
	              Object objectType = searchQueryMap.get(JsonKey.OBJECT_TYPE);
	              String [] types =  null;
	              if(objectType != null && objectType instanceof List) {
 	                  List<String> list =(List)objectType;
 	                  types = list.toArray(new String[list.size()]);
	               }
	              SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
                  Map<String, List<Map<String, Object>>> result = ElasticSearchUtil.complexSearch(searchDto, ProjectUtil.EsIndex.sunbird.name(), types);
                  Response response = new Response();
                  if(result !=null) {
                  response.put(JsonKey.RESPONSE, result);
                  } else {
                       result = new HashMap<String, List<Map<String, Object>>>();
                       response.put(JsonKey.RESPONSE, result);    
                  }
                  sender().tell(response, self());
	            } else {
	                logger.info("UNSUPPORTED OPERATION");
	                ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
	                sender().tell(exception, self());
	            }
	        } else {
	            logger.info("UNSUPPORTED MESSAGE");
	            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
	            sender().tell(exception, self());
	        }		
	}

}
