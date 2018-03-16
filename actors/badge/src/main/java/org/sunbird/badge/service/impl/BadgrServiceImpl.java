package org.sunbird.badge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.badge.model.BadgeClassExtension;
import org.sunbird.badge.service.BadgeClassExtensionService;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.badge.util.BadgingUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

/**
 * 
 * @author Manzarul
 *
 */
public class BadgrServiceImpl implements BadgingService {
	BadgeClassExtensionService badgeClassExtensionService = new BadgeClassExtensionServiceImpl();

	public BadgrServiceImpl() {
		this.badgeClassExtensionService = new BadgeClassExtensionServiceImpl();
	}

	public BadgrServiceImpl(BadgeClassExtensionService badgeClassExtensionService) {
		this.badgeClassExtensionService = badgeClassExtensionService;
	}

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
		HttpUtilResponse httpResponse = HttpUtil.postFormData(requestData, fileData, headers, BadgingUtil.getBadgrBaseUrl() + url);
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
	public Response createBadgeClass(Request request) throws ProjectCommonException {
		Response response = new Response();

		try {
			Map<String, Object> requestData = request.getRequest();

			Map<String, String> formParams = (Map<String, String>) requestData.get(JsonKey.FORM_PARAMS);
			Map<String, byte[]> fileParams = (Map<String, byte[]>) requestData.get(JsonKey.FILE_PARAMS);

			String issuerId = formParams.remove(BadgingJsonKey.ISSUER_ID);
			String rootOrgId = formParams.remove(JsonKey.ROOT_ORG_ID);
			String type = formParams.remove(JsonKey.TYPE);
			String roles = formParams.remove(JsonKey.ROLES);
			List<String> rolesList = new ArrayList<>();
			if (roles != null) {
				ObjectMapper mapper = new ObjectMapper();
				rolesList = mapper.readValue(roles, ArrayList.class);
			}

			String subtype = "";
			if (formParams.containsKey(JsonKey.SUBTYPE)) {
				subtype = formParams.remove(JsonKey.SUBTYPE);
			}

			Map<String, String> headers = BadgingUtil.getBadgrHeaders(false);

			HttpUtilResponse httpUtilResponse = HttpUtil.postFormData(formParams, fileParams, headers, BadgingUtil.getBadgeClassUrl(issuerId));
			String badgrResponseStr = httpUtilResponse.getBody();

			BadgingUtil.throwBadgeClassExceptionOnErrorStatus(httpUtilResponse.getStatusCode(), badgrResponseStr);

			ObjectMapper mapper = new ObjectMapper();
			Map<String, Object> badgrResponseMap = (Map<String, Object>) mapper.readValue(badgrResponseStr, HashMap.class);
			String badgeId = (String) badgrResponseMap.get(BadgingJsonKey.SLUG);

			BadgeClassExtension badgeClassExt = new BadgeClassExtension(badgeId, issuerId, rootOrgId, type, subtype, rolesList);
			badgeClassExtensionService.save(badgeClassExt);

			BadgingUtil.prepareBadgeClassResponse(badgrResponseStr, badgeClassExt, response.getResult());
		} catch (IOException e) {
			BadgingUtil.throwBadgeClassExceptionOnErrorStatus(ResponseCode.SERVER_ERROR.getResponseCode(), e.getMessage());
		}

		return response;
	}

	@Override
	public Response getBadgeClassDetails(Request request) throws ProjectCommonException {
		Response response = new Response();

		try {
			Map<String, Object> requestData = request.getRequest();

			String issuerId = (String) requestData.get(BadgingJsonKey.ISSUER_ID);
			String badgeId = (String) requestData.get(BadgingJsonKey.BADGE_ID);

			Map<String, String> headers = BadgingUtil.getBadgrHeaders();
			String badgrUrl = BadgingUtil.getBadgeClassUrl(issuerId, badgeId);

			HttpUtilResponse httpUtilResponse = HttpUtil.doGetRequest(badgrUrl, headers);
			String badgrResponseStr = httpUtilResponse.getBody();

			BadgingUtil.throwBadgeClassExceptionOnErrorStatus(httpUtilResponse.getStatusCode(), badgrResponseStr);

			BadgeClassExtension badgeClassExtension = badgeClassExtensionService.get(badgeId);

			BadgingUtil.prepareBadgeClassResponse(badgrResponseStr, badgeClassExtension, response.getResult());
		} catch (IOException e) {
			BadgingUtil.throwBadgeClassExceptionOnErrorStatus(ResponseCode.SERVER_ERROR.getResponseCode(), e.getMessage());
		}

		return response;
	}

	@Override
	public Response getBadgeClassList(Request request) throws ProjectCommonException {
		Response response = new Response();

		Map<String, Object> requestData = request.getRequest();
		List<String> issuerList = (List<String>) requestData.get(BadgingJsonKey.ISSUER_LIST);
		Map<String, Object> context = (Map<String, Object>) requestData.get(BadgingJsonKey.CONTEXT);
		String rootOrgId = (String) context.get(JsonKey.ROOT_ORG_ID);
		String type = (String) context.get(JsonKey.TYPE);
		String subtype = (String) context.get(JsonKey.SUBTYPE);
		List<String> allowedRoles = (List<String>) context.get(JsonKey.ROLES);

		List<BadgeClassExtension> badgeClassExtList = badgeClassExtensionService.get(issuerList,
				rootOrgId, type, subtype, allowedRoles);
		List<String> filteredIssuerList = badgeClassExtList.stream()
				.map(badge -> badge.getIssuerId()).distinct().collect(Collectors.toList());

		List<Object> badges = new ArrayList<>();

		for (String issuerSlug : filteredIssuerList) {
			badges.addAll(listBadgeClassForIssuer(issuerSlug, badgeClassExtList));
		}

		response.put(BadgingJsonKey.BADGES, badges);

		return response;
	}

	private List<Object> listBadgeClassForIssuer(String issuerSlug, List<BadgeClassExtension> badgeClassExtensionList) throws ProjectCommonException {
		List<Object> filteredBadges = new ArrayList<>();

		try {
			Map<String, String> headers = BadgingUtil.getBadgrHeaders();
			String badgrUrl = BadgingUtil.getBadgeClassUrl(issuerSlug);

			HttpUtilResponse httpUtilResponse = HttpUtil.doGetRequest(badgrUrl, headers);
			String badgrResponseStr = httpUtilResponse.getBody();

			BadgingUtil.throwBadgeClassExceptionOnErrorStatus(httpUtilResponse.getStatusCode(), badgrResponseStr);

			ObjectMapper mapper = new ObjectMapper();
			List<Map<String, Object>> badges = mapper.readValue(badgrResponseStr, ArrayList.class);

			for (Map<String, Object> badge : badges) {
				BadgeClassExtension matchedBadgeClassExt = badgeClassExtensionList.stream()
						.filter(x -> x.getBadgeId().equals(badge.get(BadgingJsonKey.SLUG))).findFirst()
						.orElse(null);

				if (matchedBadgeClassExt != null) {
					Map<String, Object> mappedBadge = new HashMap<>();
					BadgingUtil.prepareBadgeClassResponse(badge, matchedBadgeClassExt, mappedBadge);
					filteredBadges.add(mappedBadge);
				}
			}
		} catch (IOException e) {
			BadgingUtil.throwBadgeClassExceptionOnErrorStatus(ResponseCode.SERVER_ERROR.getResponseCode(), e.getMessage());
		}

		return filteredBadges;
	}

	@Override
	public Response removeBadgeClass(Request requestMsg) throws ProjectCommonException {
		Response response = new Response();

		try {
			Map<String, Object> requestData = requestMsg.getRequest();

			String issuerId = (String) requestData.get(BadgingJsonKey.ISSUER_ID);
			String badgeId = (String) requestData.get(BadgingJsonKey.BADGE_ID);

			Map<String, String> headers = BadgingUtil.getBadgrHeaders();
			String badgrUrl = BadgingUtil.getBadgeClassUrl(issuerId, badgeId);

			HttpUtilResponse httpUtilResponse = HttpUtil.sendDeleteRequest(headers, badgrUrl);
			String badgrResponseStr = httpUtilResponse.getBody();

			BadgingUtil.throwBadgeClassExceptionOnErrorStatus(httpUtilResponse.getStatusCode(), badgrResponseStr);

			badgeClassExtensionService.delete(badgeId);
			response.put(JsonKey.MESSAGE, badgrResponseStr.replaceAll("^\"|\"$", ""));
		} catch (IOException e) {
			BadgingUtil.throwBadgeClassExceptionOnErrorStatus(ResponseCode.SERVER_ERROR.getResponseCode(), e.getMessage());
		}

		return response;
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
		HttpUtilResponse httpResponse =HttpUtil.sendDeleteRequest(headers , BadgingUtil.getBadgrBaseUrl() + url);
		Response response = new Response();
		response.put(JsonKey.RESPONSE, httpResponse);
		return response;

	}

}
