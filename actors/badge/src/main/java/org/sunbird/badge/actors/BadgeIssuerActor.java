package org.sunbird.badge.actors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.service.impl.BadgingFactory;
import org.sunbird.badge.util.BadgingUtil;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.ActorRef;

/**
 * Created by arvind on 5/3/18.
 */

@ActorConfig(tasks = { "createBadgeIssuer", "getBadgeIssuer", "getAllIssuer", "deleteIssuer" }, asyncTasks = {})
public class BadgeIssuerActor extends BaseActor {

	private ObjectMapper mapper = new ObjectMapper();
	private BadgingService badgingService;

	public BadgeIssuerActor() {
		this.badgingService = BadgingFactory.getInstance();
	}

	public BadgeIssuerActor(BadgingService badgingService) {
		this.badgingService = badgingService;
	}

	@Override
	public void onReceive(Request request) throws Throwable {
		ProjectLogger.log("BadgeIssuerActor  onReceive called", LoggerEnum.INFO.name());
		String operation = request.getOperation();
		System.out.println("OPERATION IS " + operation);

		switch (operation) {
		case "createBadgeIssuer":
			createBadgeIssuer(request);
			break;
		case "getBadgeIssuer":
			getBadgeIssuer(request);
			break;
		case "getAllIssuer":
			getAllIssuer(request);
			break;
		case "deleteIssuer":
			deleteIssuer(request);
			break;
		default:
			onReceiveUnsupportedOperation("BadgeIssuerActor");
		}
	}

	/**
	 * Actor mathod to create the issuer of badge .
	 * 
	 * @param actorMessage
	 *
	 **/
	private void createBadgeIssuer(Request actorMessage) throws IOException {

		Response result = badgingService.createIssuer(actorMessage);
		HttpUtilResponse httpUtilResponse = (HttpUtilResponse) result.getResult().get(JsonKey.RESPONSE);
		int statusCode = httpUtilResponse.getStatusCode();
		if (statusCode >= 200 && statusCode < 300) {
			Response response = new Response();
			BadgingUtil.prepareBadgeIssuerResponse(httpUtilResponse.getBody(), response.getResult());
			sender().tell(response, self());
		} else {
			sender().tell(BadgingUtil.createExceptionForBadger(statusCode), self());
		}
	}

	private void getBadgeIssuer(Request actorMessage) throws IOException {

		Response result = badgingService.getIssuerDetails(actorMessage);
		HttpUtilResponse httpUtilResponse = (HttpUtilResponse) result.getResult().get(JsonKey.RESPONSE);
		int statusCode = httpUtilResponse.getStatusCode();

		if (statusCode >= 200 && statusCode < 300) {
			Response response = new Response();
			BadgingUtil.prepareBadgeIssuerResponse(httpUtilResponse.getBody(), response.getResult());
			sender().tell(response, ActorRef.noSender());
		} else {
			sender().tell(BadgingUtil.createExceptionForBadger(statusCode), self());
		}
	}

	private void getAllIssuer(Request actorMessage) throws IOException {

		Response result = badgingService.getIssuerList(actorMessage);
		HttpUtilResponse httpUtilResponse = (HttpUtilResponse) result.getResult().get(JsonKey.RESPONSE);
		int statusCode = httpUtilResponse.getStatusCode();

		if (statusCode >= 200 && statusCode < 300) {
			Response response = new Response();
			List<Map<String, Object>> issuers = new ArrayList<>();
			List<Map<String, Object>> data = mapper.readValue(httpUtilResponse.getBody(),
					new TypeReference<List<Map<String, Object>>>() {
					});
			for (Object badge : data) {
				Map<String, Object> mappedBadge = new HashMap<>();
				BadgingUtil.prepareBadgeIssuerResponse((Map<String, Object>) badge, mappedBadge);
				issuers.add(mappedBadge);
			}
			Map<String, Object> res = new HashMap<>();
			res.put(BadgingJsonKey.ISSUERS, issuers);
			response.getResult().putAll(res);
			sender().tell(response, ActorRef.noSender());
		} else {
			sender().tell(BadgingUtil.createExceptionForBadger(statusCode), self());
		}
	}

	private void deleteIssuer(Request request) throws IOException {

		Response result = badgingService.deleteIssuer(request);
		HttpUtilResponse httpUtilResponse = (HttpUtilResponse) result.getResult().get(JsonKey.RESPONSE);
		int statusCode = httpUtilResponse.getStatusCode();

		if (statusCode >= 200 && statusCode < 300) {
			Response response = new Response();
			// since the response from badger service contains " at beging and end so remove
			// that from response string
			response.put(JsonKey.MESSAGE, StringUtils.strip(httpUtilResponse.getBody(), "\""));
			sender().tell(response, self());
		} else {
			sender().tell(BadgingUtil.createExceptionForBadger(statusCode), self());
		}

	}

}
