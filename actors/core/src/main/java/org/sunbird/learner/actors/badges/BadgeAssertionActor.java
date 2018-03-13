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
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.badges.service.BadgingService;
import org.sunbird.learner.actors.badges.service.impl.BadgingFactory;
import org.sunbird.learner.actors.badging.BadgingUtil;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.UntypedAbstractActor;

/**
 * @author Manzarul
 *
 */
public class BadgeAssertionActor extends UntypedAbstractActor {

  private ObjectMapper mapper = new ObjectMapper();

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
        	System.out.println("get assertion details called");
        	getAssertionDetails(actorMessage);	
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(BadgingActorOperations.GET_BADGE_ASSERTION_LIST.getValue())) {
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
		BadgingService service  = BadgingFactory.getInstance();
		try {
			Response result = service.badgeAssertion(actorMessage);
			Map<String, Object> res = mapper.readValue((String)result.getResult().get(JsonKey.RESPONSE), HashMap.class);
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
		String url = BadgingUtil.createBadgerUrl(request.getRequest(), BadgingUtil.SUNBIRD_BADGER_GETASSERTION_URL, 3);
		try {
			System.out.println("requested url " + url );
			System.out.println("Header values "+BadgingUtil.getBadgrHeaders().get("Authorization"));
			String response = HttpUtil.sendGetRequest(url, BadgingUtil.getBadgrHeaders());
			System.out.println(" Response ==" + response);
			if (ProjectUtil.isStringNullOREmpty(response)) {
				sender().tell(ProjectUtil.createResourceNotFoundException(), self());
				return;
			}
			Response result = new Response();
			Map<String, Object> res = mapper.readValue(response, HashMap.class);
			// TODO need to fetch created by and put into response
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
