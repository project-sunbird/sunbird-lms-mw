/**
 * 
 */
package org.sunbird.badge.actors;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.badge.util.BadgingUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Manzarul
 *
 */
public class BadgeAssertion extends BaseActor {

  private ObjectMapper mapper = new ObjectMapper();
   
	public static void init() {
		BackgroundRequestRouter.registerActor(BadgeAssertion.class,
				Arrays.asList(BadgeOperations.createBadgeAssertion.name(), BadgeOperations.getBadgeAssertion.name(),
						BadgeOperations.getBadgeAssertionList.name(), BadgeOperations.revokeBadge.name()));
	}
	
   @Override
	public void onReceive(Request request) throws Throwable {
		ProjectLogger.log("BadgeAssertionActor  onReceive called", LoggerEnum.INFO.name());
		Util.initializeContext(request, JsonKey.USER);
		// set request id fto thread loacl...
		ExecutionContext.setRequestId(request.getRequestId());
		String operation = request.getOperation();
		switch (operation) {
		case "createBadgeAssertion":
			createAssertion(request);
			break;
		case "getBadgeAssertion":
			getAssertionDetails(request);
			break;
		case "getBadgeAssertionList":
			break;
		case "revokeBadge":
			break;
		default:
			ProjectLogger.log("UNSUPPORTED OPERATION");
			ProjectCommonException exception = new ProjectCommonException(
					ResponseCode.invalidOperationName.getErrorCode(),
					ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
		}
	}
   
   /**
    * This method will call the badger server to create badge assertion.
    * @param actorMessage Request
    */
    @SuppressWarnings("unchecked")
	private void createAssertion(Request actorMessage) {
		Map<String, Object> requestedData = actorMessage.getRequest();
		String requestBody = BadgingUtil.createAssertionReqData(requestedData);
		String url = BadgingUtil.createBadgerUrl(requestedData, BadgingUtil.SUNBIRD_BADGER_CREATE_ASSERTION_URL, 2);
		try {
			System.out.println("Testing..." + url);
			String response = HttpUtil.sendPostRequest(url, requestBody, BadgingUtil.createBadgerHeader());
			System.out.println("Response====" + response);
			Response result = new Response();
			Map<String, Object> res = mapper.readValue(response, HashMap.class);
			//res.put(BadgingJsonKey.CREATED_BY, actorMessage.getRequestId());
			result.getResult().putAll(res);
			sender().tell(result, self());
		} catch (IOException e) {
			e.printStackTrace();
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
			String response = HttpUtil.sendGetRequest(url, BadgingUtil.createBadgerHeader());
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
