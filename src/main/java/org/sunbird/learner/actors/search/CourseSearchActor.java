package org.sunbird.learner.actors.search;

import akka.actor.UntypedAbstractActor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

/**
 * This class will handle search operation for course.
 * @author Amit Kumar
 */
public class CourseSearchActor extends UntypedAbstractActor {
    private LogHelper logger = LogHelper.getInstance(CourseSearchActor.class.getName());

	@Override
	public void onReceive(Object message) throws Throwable {
		 if (message instanceof Request) {
		 	try {
				logger.info("CourseSearchActor  onReceive called");
				Request actorMessage = (Request) message;
				if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.SEARCH_COURSE.getValue())) {
					Map<String, Object> req = actorMessage.getRequest();
					@SuppressWarnings("unchecked")
					Map<String, Object> searchQueryMap = (Map<String, Object>) req.get(JsonKey.SEARCH);
					Map<String, Object> ekStepSearchQuery = new HashMap<String, Object>();
					ekStepSearchQuery.put(JsonKey.REQUEST, searchQueryMap);

					String json = null;
					try {
						json = new ObjectMapper().writeValueAsString(ekStepSearchQuery);
						Map<String, Object> query = new HashMap<String, Object>();
						query.put(JsonKey.SEARCH_QUERY, json);
						Util.getContentData(query);
						Object ekStepResponse = query.get(JsonKey.CONTENTS);
						Response response = new Response();
						response.put(JsonKey.RESPONSE, ekStepResponse);
						sender().tell(response, self());
					} catch (JsonProcessingException e) {
						ProjectCommonException projectCommonException = new ProjectCommonException(ResponseCode.internalError.getErrorCode(), ResponseCode.internalError.getErrorMessage(), ResponseCode.internalError.getResponseCode());
						sender().tell(projectCommonException, self());

					}
				} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_COURSE_BY_ID.getValue())) {
					Map<String, Object> req = actorMessage.getRequest();
					String courseId = (String) req.get(JsonKey.ID);
					Map<String, Object> result = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.course.getTypeName(), courseId);
					Response response = new Response();
					if (result != null) {
						response.put(JsonKey.RESPONSE, result);
					} else {
						result = new HashMap<String, Object>();
						response.put(JsonKey.RESPONSE, result);
					}
					sender().tell(response, self());
				} else {
					logger.info("UNSUPPORTED OPERATION");
					ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
					sender().tell(exception, self());
				}
			}catch (Exception ex){
		 		logger.error(ex);
		 		sender().tell(ex , self());
			}
	        } else {
	            // Throw exception as message body
	            logger.info("UNSUPPORTED MESSAGE");
	            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
	            sender().tell(exception, self());
	        }		
	}

}
