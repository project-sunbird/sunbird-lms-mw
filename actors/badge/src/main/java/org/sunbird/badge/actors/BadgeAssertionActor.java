/**
 * 
 */
package org.sunbird.badge.actors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.service.impl.BadgingFactory;
import org.sunbird.badge.util.BadgingUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Manzarul
 *
 */
public class BadgeAssertionActor extends BaseActor {

  private ObjectMapper mapper = new ObjectMapper();
  BadgingService service  = BadgingFactory.getInstance();
  public static void init() {
      RequestRouter.registerActor(BadgeAssertionActor.class, Arrays.asList(
              BadgeOperations.createBadgeAssertion.name(),
              BadgeOperations.getBadgeAssertion.name(),
              BadgeOperations.getBadgeAssertionList.name(),
              BadgeOperations.revokeBadge.name()));
  }
  
  @Override
  public void onReceive(Request request) throws Throwable {
	  ProjectLogger.log("BadgeAssertionActor  onReceive called",LoggerEnum.INFO.name());
	  String operation = request.getOperation();

      switch (operation) {
          case "createBadgeAssertion":
        	  createAssertion(request);	
              break;
          case "getBadgeAssertion":
        	  getAssertionDetails(request);	
              break;
          case "getBadgeAssertionList":
        	  getAssertionList(request);
              break;
          case "revokeBadge":
        	  revokeAssertion(request);
              break;
          default:
              onReceiveUnsupportedOperation("BadgeClassActor");
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
			HttpUtilResponse httpUtilResponse = (HttpUtilResponse) result.getResult().get(JsonKey.RESPONSE);
			int statusCode = httpUtilResponse.getStatusCode();
			if (statusCode >= 200 && statusCode < 300) {
				Map<String, Object> res = mapper.readValue(httpUtilResponse.getBody(),
						HashMap.class);
				//calling to create response as per sunbird 
				 res = BadgingUtil.prepareAssertionResponse(res, new HashMap<String,Object>());
				result = new Response();
				result.getResult().putAll(res);
				sender().tell(result, self());
				Map<String,Object> map = BadgingUtil.createBadgeNotifierMap(res);
				Request  request = new Request();
				map.put(JsonKey.OBJECT_TYPE, actorMessage.getRequest().get(JsonKey.OBJECT_TYPE));
				map.put(JsonKey.ID, actorMessage.getRequest().get(BadgingJsonKey.RECIPIENT_ID));
				request.getRequest().putAll(map);
				request.setOperation(BadgeOperations.assignBadgeMessage.name());
				tellToAnother(request);
			} else {
				sender().tell(BadgingUtil.createExceptionForBadger(statusCode), self());
			}
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
			HttpUtilResponse httpUtilResponse = (HttpUtilResponse) result.getResult().get(JsonKey.RESPONSE);
			int statusCode = httpUtilResponse.getStatusCode();
			if (statusCode >= 200 && statusCode < 300) {
				Map<String, Object> res = mapper.readValue(httpUtilResponse.getBody(), HashMap.class);
				// calling to create response as per sunbird
				res = BadgingUtil.prepareAssertionResponse(res, new HashMap<String, Object>());
				boolean response = BadgingUtil.matchAssertionData(
						(String) request.getRequest().get(BadgingJsonKey.ISSUER_ID),
						(String) request.getRequest().get(BadgingJsonKey.BADGE_CLASS_ID), res);
				if (response) {
					result = new Response();
					result.getResult().putAll(res);
					sender().tell(result, self());
				} else {
					ProjectCommonException ex = new ProjectCommonException(ResponseCode.resourceNotFound.getErrorCode(),
							ResponseCode.resourceNotFound.getErrorMessage(),
							ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
					sender().tell(ex, self());
				}
			} else {
				sender().tell(BadgingUtil.createExceptionForBadger(statusCode), self());
			}
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
			List<HttpUtilResponse> list = (List) result.getResult().get(JsonKey.RESPONSE);
			if (list != null && list.size() > 0) {
				List<Map<String, Object>> responseMap = new ArrayList<>();
				for (HttpUtilResponse data : list) {
					if (data.getStatusCode() >= 200 && data.getStatusCode() < 300
							&& !ProjectUtil.isStringNullOREmpty(data.getBody())) {
						Map<String, Object> res = mapper.readValue(data.getBody(), HashMap.class);
						// calling to create response as per sunbird
						res = BadgingUtil.prepareAssertionResponse(res, new HashMap<String, Object>());
						responseMap.add(res);
					}
				}
				result = new Response();
				result.getResult().put(BadgingJsonKey.ASSERTIONS, responseMap);
				sender().tell(result, self());
			} else {
				ProjectCommonException ex = new ProjectCommonException(ResponseCode.resourceNotFound.getErrorCode(),
						ResponseCode.resourceNotFound.getErrorMessage(),
						ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
				sender().tell(ex, self());
			}
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
			HttpUtilResponse httpUtilResponse = (HttpUtilResponse) result.getResult().get(JsonKey.RESPONSE);
			int statusCode = httpUtilResponse.getStatusCode();
			if (statusCode >= 200 && statusCode < 300) {
				result = new Response();
				result.getResult().put(JsonKey.STATUS, JsonKey.SUCCESS);
				sender().tell(result, self());
				Map<String, Object> map = BadgingUtil.createRevokeBadgeNotifierMap(request.getRequest());
				Request notificationReq = new Request();
				map.put(JsonKey.OBJECT_TYPE, request.getRequest().get(JsonKey.OBJECT_TYPE));
				map.put(JsonKey.ID, request.getRequest().get(BadgingJsonKey.RECIPIENT_ID));
				notificationReq.getRequest().putAll(map);
				notificationReq.setOperation(BadgeOperations.revokeBadgeMessage.name());
				tellToAnother(notificationReq);
			} else if (statusCode == 400) {
				ProjectCommonException ex = new ProjectCommonException(
						ResponseCode.badgeAssertionAlreadyRevoked.getErrorCode(),
						ResponseCode.badgeAssertionAlreadyRevoked.getErrorMessage(),
						ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(ex, self());
			} else {
				sender().tell(BadgingUtil.createExceptionForBadger(statusCode), self());
			}

		} catch (IOException e) {
			ProjectCommonException ex = new ProjectCommonException(ResponseCode.badgingserverError.getErrorCode(),
					ResponseCode.badgingserverError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			sender().tell(ex, self());
			ProjectLogger.log(e.getMessage(), e);
		}
	}
}
