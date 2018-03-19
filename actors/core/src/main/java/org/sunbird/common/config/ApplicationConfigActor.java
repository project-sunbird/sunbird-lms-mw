package org.sunbird.common.config;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RouterConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

@RouterConfig(request = {"updateSystemSettings"}, bgRequest = {})
public class ApplicationConfigActor extends BaseActor {

	private CassandraOperation cassandraOperation = ServiceFactory.getInstance();


	@Override
	public void onReceive(Request request) throws Throwable {
		String requestedOperation = request.getOperation();
		ProjectLogger.log("Operation name is ==" + requestedOperation);
		if (requestedOperation.equalsIgnoreCase(ActorOperations.UPDATE_SYSTEM_SETTINGS.getValue())) {
			updateSystemSettings(request);
			Response response = new Response();
			response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
			sender().tell(response, self());
		} else {
			onReceiveUnsupportedOperation(request.getOperation());
		}
	}

	private void updateSystemSettings(Request message) {

		ProjectLogger.log("Update System Settings started ");
		long startTime = System.currentTimeMillis();
		Map<String, Object> req = message.getRequest();
		Map<String, Object> dataMap = (Map<String, Object>) req.get(JsonKey.DATA);
		boolean dbPhoneUniqueValue = true;
		boolean dbEmailUniqueValue = false;
		boolean reqPhoneUniqueValue = true;
		boolean reqEmailUniqueValue = false;

		Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.SYSTEM_SETTINGS_DB);
		Response response = cassandraOperation.getAllRecords(dbInfo.getKeySpace(), dbInfo.getTableName());
		List<Map<String, Object>> responseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
		if (null != responseList && !responseList.isEmpty()) {
			for (Map<String, Object> resultMap : responseList) {
				if ((JsonKey.PHONE_UNIQUE).equalsIgnoreCase((String) resultMap.get(JsonKey.FIELD))
						&& !ProjectUtil.isStringNullOREmpty((String) resultMap.get(JsonKey.FIELD))) {
					dbPhoneUniqueValue = Boolean.parseBoolean((String) resultMap.get(JsonKey.FIELD));
				} else if ((JsonKey.EMAIL_UNIQUE).equalsIgnoreCase((String) resultMap.get(JsonKey.FIELD))
						&& !ProjectUtil.isStringNullOREmpty((String) resultMap.get(JsonKey.FIELD))) {
					dbEmailUniqueValue = Boolean.parseBoolean((String) resultMap.get(JsonKey.FIELD));
				}
			}
		}

		Set<String> keys = dataMap.keySet();
		for (String str : keys) {
			if ((JsonKey.PHONE_UNIQUE).equalsIgnoreCase(str)) {
				reqPhoneUniqueValue = Boolean.parseBoolean(String.valueOf(dataMap.get(str)));
			} else if ((JsonKey.EMAIL_UNIQUE).equalsIgnoreCase(str)) {
				reqEmailUniqueValue = Boolean.parseBoolean(String.valueOf(dataMap.get(str)));
			}
		}
		SearchDTO searchDto = null;
		if (keys.contains(JsonKey.PHONE_UNIQUE) && (!dbPhoneUniqueValue) && (reqPhoneUniqueValue)) {
			searchDto = new SearchDTO();
			searchDto.setLimit(0);
			Map<String, String> facets = new HashMap<>();
			facets.put(JsonKey.ENC_PHONE, null);
			List<Map<String, String>> list = new ArrayList<>();
			list.add(facets);
			searchDto.setFacets(list);
			Map<String, Object> esResponse = ElasticSearchUtil.complexSearch(searchDto,
					ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName());
			if (null != esResponse) {
				List<Map<String, Object>> facetsRes = (List<Map<String, Object>>) esResponse.get(JsonKey.FACETS);
				if (null != facetsRes && !facetsRes.isEmpty()) {
					Map<String, Object> map = facetsRes.get(0);
					List<Map<String, Object>> values = (List<Map<String, Object>>) map.get("values");
					for (Map<String, Object> result : values) {
						long count = (long) result.get(JsonKey.COUNT);
						if (count > 1) {
							throw new ProjectCommonException(
									ResponseCode.duplicatePhoneData.getErrorCode(), MessageFormat
											.format(ResponseCode.duplicatePhoneData.getErrorMessage(), JsonKey.PHONE),
									ResponseCode.CLIENT_ERROR.getResponseCode());
						}
					}
				}
			}
		}

		if (keys.contains(JsonKey.EMAIL_UNIQUE) && (!dbEmailUniqueValue) && (reqEmailUniqueValue)) {
			searchDto = new SearchDTO();
			searchDto.setLimit(0);
			Map<String, String> facets = new HashMap<>();
			facets.put(JsonKey.ENC_EMAIL, null);
			List<Map<String, String>> list = new ArrayList<>();
			list.add(facets);
			searchDto.setFacets(list);
			Map<String, Object> esResponse = ElasticSearchUtil.complexSearch(searchDto,
					ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName());
			if (null != esResponse) {
				List<Map<String, Object>> facetsRes = (List<Map<String, Object>>) esResponse.get(JsonKey.FACETS);
				if (null != facetsRes && !facetsRes.isEmpty()) {
					Map<String, Object> map = facetsRes.get(0);
					List<Map<String, Object>> values = (List<Map<String, Object>>) map.get("values");
					for (Map<String, Object> result : values) {
						long count = (long) result.get(JsonKey.COUNT);
						if (count > 1) {
							throw new ProjectCommonException(
									ResponseCode.duplicateEmailData.getErrorCode(), MessageFormat
											.format(ResponseCode.duplicateEmailData.getErrorMessage(), JsonKey.EMAIL),
									ResponseCode.CLIENT_ERROR.getResponseCode());
						}
					}
				}
			}
		}

		for (String str : keys) {
			String value = String.valueOf(dataMap.get(str));
			Map<String, Object> map = new HashMap<>();
			map.put(JsonKey.ID, str);
			map.put(JsonKey.FIELD, str);
			map.put(JsonKey.VALUE, value);
			cassandraOperation.insertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), map);
		}

		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		ProjectLogger.log("total time taken to Update System Settings  " + elapsedTime + " ms.");

	}

}
