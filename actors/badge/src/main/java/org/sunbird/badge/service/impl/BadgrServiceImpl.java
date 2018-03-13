package org.sunbird.badge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.util.BadgingUtil;

/**
 * 
 * @author Manzarul
 *
 */
public class BadgrServiceImpl implements BadgingService {

	@Override
	public Response createIssuer(Request request) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Response getIssuerDetails(Request request) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Response getIssuerList(Request request) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Response removeIssuer(Request request) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Response createBadgeClass(Request request) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Response getBadgeClassDetails(Request request) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Response getBadgeClassList(Request request) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Response removeBadgeClass(Request request) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Response badgeAssertion(Request request) throws IOException {
		Map<String, Object> requestedData = request.getRequest();
		String requestBody = BadgingUtil.createAssertionReqData(requestedData);
		String url = BadgingUtil.createBadgerUrl(requestedData, BadgingUtil.SUNBIRD_BADGER_CREATE_ASSERTION_URL, 2);
		String httpResponse = HttpUtil.sendPostRequest(url, requestBody, BadgingUtil.getBadgrHeaders());
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
	}

	@Override
	public Response getAssertionDetails(Request request) throws IOException {
		String url = BadgingUtil.createBadgerUrl(request.getRequest(), BadgingUtil.SUNBIRD_BADGER_GETASSERTION_URL, 3);
		String httpResponse = HttpUtil.sendGetRequest(url, BadgingUtil.getBadgrHeaders());
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
	}

	@Override
	public Response getAssertionList(Request request) throws IOException {
		List<Map<String, Object>> requestData = (List) request.getRequest().get(BadgingJsonKey.ASSERTIONS);
		List<String> responseList = new ArrayList<>();
		for (Map<String, Object> map : requestData) {
			String requestBody = BadgingUtil.createAssertionReqData(map);
			String url = BadgingUtil.createBadgerUrl(map, BadgingUtil.SUNBIRD_BADGER_CREATE_ASSERTION_URL, 2);
			String httpResponse = HttpUtil.sendPostRequest(url, requestBody, BadgingUtil.getBadgrHeaders());
			responseList.add(httpResponse);
		}
		Response response = new Response();
		response.put(JsonKey.RESPONSE, responseList);
		return response;
	}

	@Override
	public Response revokeAssertion(Request request) throws IOException {
		String url = BadgingUtil.createBadgerUrl(request.getRequest(), BadgingUtil.SUNBIRD_BADGER_GETASSERTION_URL, 3);
		String httpResponse = HttpUtil.sendGetRequest(url, BadgingUtil.getBadgrHeaders());
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
	}

}
