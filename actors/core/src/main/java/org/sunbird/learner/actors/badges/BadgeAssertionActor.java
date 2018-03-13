/**
 * 
 */
package org.sunbird.learner.actors.badges;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.badges.service.BadgingService;
import org.sunbird.learner.actors.badges.service.impl.BadgingFactory;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.UntypedAbstractActor;

/**
 * @author Manzarul
 *
 */
public class BadgeAssertionActor extends UntypedAbstractActor {

  private ObjectMapper mapper = new ObjectMapper();
  BadgingService service  = BadgingFactory.getInstance();
  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
    	  
        ProjectLogger.log("BadgeAssertionActor  onReceive called",LoggerEnum.INFO.name());
        Request actorMessage = (Request) message;
        Util.initializeContext(actorMessage, JsonKey.USER);
        // set request id fto thread loacl...
        ExecutionContext.setRequestId(actorMessage.getRequestId());
        if (actorMessage.getOperation()
            .equalsIgnoreCase(BadgingActorOperations.CREATE_BADGE_ASSERTION.getValue())) {
         createAssertion(actorMessage);	
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(BadgingActorOperations.GET_BADGE_ASSERTION.getValue())) {
        	getAssertionDetails(actorMessage);	
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(BadgingActorOperations.GET_BADGE_ASSERTION_LIST.getValue())) {
        	getAssertionList(actorMessage);
        } else if (actorMessage.getOperation().equalsIgnoreCase(BadgingActorOperations.REVOKE_BADGE.getValue())) {
        }
        else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
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
   * This method will call the badger server to create badge assertion.
   * @param actorMessage Request
   */
   @SuppressWarnings("unchecked")
	private void createAssertion(Request actorMessage) {
		try {
			Response result = service.badgeAssertion(actorMessage);
			Map<String, Object> res = mapper.readValue((String)result.getResult().get(JsonKey.RESPONSE), HashMap.class);
			result = new Response();
			result.getResult().putAll(res);
			sender().tell(result, self());
		} catch (IOException e) {
			ProjectCommonException ex = new ProjectCommonException(ResponseCode.badgingserverError.getErrorCode(),
					ResponseCode.badgingserverError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			sender().tell(ex, self());
			ProjectLogger.log(e.getMessage(), e);
		}
	}
   
   /**
    * This method will get single assertion details based on
    * issuerSlug, badgeClassSlug and assertionSlug
    * @param request Request
    */
   @SuppressWarnings("unchecked")
	private void getAssertionDetails(Request request) {
		try {
			Response result = service.getAssertionDetails(request);
			if (ProjectUtil.isStringNullOREmpty((String)result.getResult().get(JsonKey.RESPONSE))) {
				sender().tell(ProjectUtil.createResourceNotFoundException(), self());
				return;
			}
			Map<String, Object> res = mapper.readValue((String)result.getResult().get(JsonKey.RESPONSE), HashMap.class);
			result = new Response();
			result.getResult().putAll(res);
			sender().tell(result, self());
		} catch (IOException e) {
			ProjectCommonException ex = new ProjectCommonException(ResponseCode.badgingserverError.getErrorCode(),
					ResponseCode.badgingserverError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			sender().tell(ex, self());
			ProjectLogger.log(e.getMessage(), e);
		}
	}
   
   /**
    * This method will get single assertion details based on
    * issuerSlug, badgeClassSlug and assertionSlug
    * @param request Request
    */
   @SuppressWarnings("unchecked")
	private void getAssertionList(Request request) {
		try {
			Response result = service.getAssertionList(request);
			if (ProjectUtil.isStringNullOREmpty((String)result.getResult().get(JsonKey.RESPONSE))) {
				sender().tell(ProjectUtil.createResourceNotFoundException(), self());
				return;
			}
			Map<String, Object> res = mapper.readValue((String)result.getResult().get(JsonKey.RESPONSE), HashMap.class);
			result = new Response();
			result.getResult().putAll(res);
			sender().tell(result, self());
		} catch (IOException e) {
			ProjectCommonException ex = new ProjectCommonException(ResponseCode.badgingserverError.getErrorCode(),
					ResponseCode.badgingserverError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			sender().tell(ex, self());
			ProjectLogger.log(e.getMessage(), e);
		}
	}
   
    /**
     * This method will make a call for revoking the badges.
     * @param request Request
     */
	private void revokeAssertion(Request request) {
		try {
			Response result = service.revokeAssertion(request);
			if (ProjectUtil.isStringNullOREmpty((String) result.getResult().get(JsonKey.RESPONSE))) {
				sender().tell(ProjectUtil.createResourceNotFoundException(), self());
				return;
			}
			Map<String, Object> res = mapper.readValue((String) result.getResult().get(JsonKey.RESPONSE),
					HashMap.class);
			result = new Response();
			result.getResult().putAll(res);
			sender().tell(result, self());
		} catch (IOException e) {
			ProjectCommonException ex = new ProjectCommonException(ResponseCode.badgingserverError.getErrorCode(),
					ResponseCode.badgingserverError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			sender().tell(ex, self());
			ProjectLogger.log(e.getMessage(), e);
		}
	}
}
