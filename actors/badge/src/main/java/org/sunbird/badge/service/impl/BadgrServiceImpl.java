package org.sunbird.badge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.util.BadgingUtil;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;

/**
 * 
 * @author Manzarul
 *
 */
public class BadgrServiceImpl implements BadgingService {

	@Override
	public Response createIssuer(Request request) throws IOException {
		// TODO Auto-generated method stub
		Map<String, Object> req = request.getRequest();
		byte[] image = null;
		Map<String, byte[]> fileData = new HashMap<>();
		if (req.containsKey(JsonKey.IMAGE) && null != req.get(JsonKey.IMAGE)) {
			image = (byte[]) req.get(JsonKey.IMAGE);
		}
		Map<String, String> requestData = new HashMap<>();
		requestData.put(JsonKey.NAME, (String) req.get(JsonKey.NAME));
		requestData.put(JsonKey.DESCRIPTION, (String) req.get(JsonKey.DESCRIPTION));
		requestData.put(JsonKey.URL, (String) req.get(JsonKey.URL));
		requestData.put(JsonKey.EMAIL, (String) req.get(JsonKey.EMAIL));

		String url = "/v1/issuer/issuers";
		Map<String, String> headers = BadgingUtil.getBadgrHeaders();
		// since the content type here is not application/json so removing this key.
		headers.remove("Content-Type");
		if (null != image) {
			fileData.put(JsonKey.IMAGE, image);
		}
		HttpUtilResponse httpResponse = HttpUtil.postFormData(requestData, fileData, headers, url);
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
	}

	@Override
	public Response getIssuerDetails(Request request) throws IOException {
		// TODO Auto-generated method stub
		Map<String, Object> req = request.getRequest();
		String slug = (String) req.get(JsonKey.SLUG);
		String url = "/v1/issuer/issuers" + "/" + slug;
		Map<String, String> headers = BadgingUtil.getBadgrHeaders();

		HttpUtilResponse httpResponse =HttpUtil.doGetRequest(BadgingUtil.getBadgrBaseUrl() + url, headers);
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
	}

	@Override
	public Response getIssuerList(Request request) throws IOException {
		// TODO Auto-generated method stub
		String url = "/v1/issuer/issuers";
		Map<String, String> headers = BadgingUtil.getBadgrHeaders();
		HttpUtilResponse httpResponse =HttpUtil.doGetRequest(BadgingUtil.getBadgrBaseUrl() + url, headers);
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
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
		HttpUtilResponse httpResponse = HttpUtil.doPostRequest(url, requestBody, BadgingUtil.getBadgrHeaders());
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
	}

	@Override
	public Response getAssertionDetails(Request request) throws IOException {
		String url = BadgingUtil.createBadgerUrl(request.getRequest(), BadgingUtil.SUNBIRD_BADGER_GETASSERTION_URL, 3);
		HttpUtilResponse httpResponse = HttpUtil.doGetRequest(url, BadgingUtil.getBadgrHeaders());
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
	}

	@Override
	public Response getAssertionList(Request request) throws IOException {
		List<Map<String, Object>> requestData = (List) request.getRequest().get(BadgingJsonKey.ASSERTIONS);
		List<HttpUtilResponse> responseList = new ArrayList<>();
		for (Map<String, Object> map : requestData) {
			String url = BadgingUtil.createBadgerUrl(map, BadgingUtil.SUNBIRD_BADGER_GETASSERTION_URL, 3);
			HttpUtilResponse httpResponse = HttpUtil.doGetRequest(url,  BadgingUtil.getBadgrHeaders());
			responseList.add(httpResponse);
		}
		Response response = new Response();
		response.put(JsonKey.RESPONSE, responseList);
		return response;
	}

	@Override
	public Response revokeAssertion(Request request) throws IOException {
		String url = BadgingUtil.createBadgerUrl(request.getRequest(), BadgingUtil.SUNBIRD_BADGER_GETASSERTION_URL, 3);
		String requestBody = BadgingUtil.createAssertionRevokeData(request.getRequest());
		System.out.println("request data and url ==" + requestBody + " " + url);
		HttpUtilResponse httpResponse = HttpUtil.sendDeleteRequest(requestBody,  BadgingUtil.getBadgrHeaders(), url);
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;
	}

	@Override
	public Response deleteIssuer(Request request) throws IOException{
		Map<String, Object> req = request.getRequest();
		String slug = (String) req.get(JsonKey.SLUG);
		String url = "/v1/issuer/issuers" + "/" + slug;
		Map<String, String> headers = BadgingUtil.getBadgrHeaders();
		HttpUtilResponse httpResponse =HttpUtil.sendDeleteRequest(headers , url);
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;

	}

}
