package org.sunbird.learner.actors.tenantpreference;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

/**
 * Class for T&C . Created by arvind on 27/10/17.
 */

@ActorConfig(tasks = { "createTanentPreference", "updateTenantPreference", "getTenantPreference",
		"updateTCStatusOfUser" }, asyncTasks = {})
public class TenantPreferenceManagementActor extends BaseActor {

	private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
	private Util.DbInfo tenantPreferenceDbInfo = Util.dbInfoMap.get(JsonKey.TENANT_PREFERENCE_DB);
	private Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
	private Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

	@Override
	public void onReceive(Request request) throws Throwable {
		if (request.getOperation().equalsIgnoreCase(ActorOperations.CREATE_TENANT_PREFERENCE.getValue())) {
			createTenantPreference(request);
		} else if (request.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_TENANT_PREFERENCE.getValue())) {
			updateTenantPreference(request);
		} else if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_TENANT_PREFERENCE.getValue())) {
			getTenantPreference(request);
		} else if (request.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_TC_STATUS_OF_USER.getValue())) {
			updateTcStatusOfUser(request);
		} else {
			onReceiveUnsupportedOperation(request.getOperation());
		}
	}

	/**
	 * Method to get the all Tenal preference of the given root org id .
	 * 
	 * @param actorMessage
	 */
	private void getTenantPreference(Request actorMessage) {

		ProjectLogger.log("TenantPreferenceManagementActor-createTenantPreference called");

		String orgId = (String) actorMessage.getRequest().get(JsonKey.ORG_ID);

		if (ProjectUtil.isStringNullOREmpty(orgId)) {
			// throw invalid ord id ,org id should not be null or empty .
			throw new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
					ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		}

		Response finalResponse = new Response();

		Response response1 = cassandraOperation.getRecordsByProperty(tenantPreferenceDbInfo.getKeySpace(),
				tenantPreferenceDbInfo.getTableName(), JsonKey.ORG_ID, orgId);
		List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);

		if (list.isEmpty()) {
			// throw exception org does not exist...
			throw new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
					ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		}

		finalResponse.getResult().put(JsonKey.TENANT_PREFERENCE, list);
		sender().tell(finalResponse, self());

	}

	/**
	 * Method to update the Tenant preference on basis of id or (role and org id).
	 * 
	 * @param actorMessage
	 */
	private void updateTenantPreference(Request actorMessage) {

		ProjectLogger.log("TenantPreferenceManagementActor-createTenantPreference called");
		List<Map<String, Object>> reqList = (List<Map<String, Object>>) actorMessage.getRequest()
				.get(JsonKey.TENANT_PREFERENCE);

		Response finalResponse = new Response();
		List<Map<String, Object>> responseList = new ArrayList<>();
		String orgId = (String) actorMessage.getRequest().get(JsonKey.ROOT_ORG_ID);

		if (ProjectUtil.isStringNullOREmpty(orgId)) {
			// throw invalid ord id ,org id should not be null or empty .
			throw new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
					ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		}
		if (reqList.isEmpty()) {
			// no need to do anything throw exception invalid request data as list is empty
			throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
					ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		}
		Response response1 = cassandraOperation.getRecordsByProperty(tenantPreferenceDbInfo.getKeySpace(),
				tenantPreferenceDbInfo.getTableName(), JsonKey.ORG_ID, orgId);
		List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);

		for (Map<String, Object> map : reqList) {

			Map<String, Object> dbMap = null;
			String id = (String) map.get(JsonKey.ID);

			if (ProjectUtil.isStringNullOREmpty(id)) {

				Map<String, Object> responseMap = new HashMap<>();
				responseMap.put(JsonKey.ID, id);
				responseMap.put(JsonKey.STATUS, JsonKey.FAILURE);
				responseList.add(responseMap);
			} else {
				boolean flag = false;
				for (Map<String, Object> m : list) {
					if (id.equals((String) m.get(JsonKey.ID))) {
						dbMap = m;
						flag = true;
						break;
					}
				}
				if (!flag) {
					// means data not found
					Map<String, Object> responseMap = new HashMap<>();
					responseMap.put(JsonKey.ID, id);
					responseMap.put(JsonKey.STATUS, "Invalid Id");
					responseList.add(responseMap);
				}
			}

			if (null != dbMap) {
				if (map.containsKey(JsonKey.ROLE)) {
					dbMap.put(JsonKey.ROLE, map.get(JsonKey.ROLE));
				}
				if (map.containsKey(JsonKey.DATA)) {
					dbMap.put(JsonKey.DATA, map.get(JsonKey.DATA));
				}
				cassandraOperation.updateRecord(tenantPreferenceDbInfo.getKeySpace(),
						tenantPreferenceDbInfo.getTableName(), dbMap);
				Map<String, Object> responseMap = new HashMap<>();
				responseMap.put(JsonKey.ID, id);
				responseMap.put(JsonKey.STATUS, JsonKey.SUCCESS);
				responseList.add(responseMap);
			}

		}
		finalResponse.getResult().put(JsonKey.RESPONSE, responseList);
		sender().tell(finalResponse, self());

	}

	/**
	 * Method to create tenant preference on basis of role and org , if already
	 * exist it will not create new one .
	 * 
	 * @param actorMessage
	 */
	private void createTenantPreference(Request actorMessage) {

		ProjectLogger.log("TenantPreferenceManagementActor-createTenantPreference called");
		List<Map<String, Object>> req = (List<Map<String, Object>>) actorMessage.getRequest()
				.get(JsonKey.TENANT_PREFERENCE);

		Response finalResponse = new Response();
		List<Map<String, Object>> responseList = new ArrayList<>();
		String orgId = (String) actorMessage.getRequest().get(JsonKey.ROOT_ORG_ID);

		if (ProjectUtil.isStringNullOREmpty(orgId)) {
			// throw invalid ord id ,org id should not be null or empty .
			throw new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
					ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		}
		// check whether org exist or not
		Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgId);
		List<Map<String, Object>> orglist = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
		if (orglist.isEmpty()) {
			throw new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
					ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		}
		if (req.isEmpty()) {
			// no need to do anything throw exception invalid request data as list is empty
			throw new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
					ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		}

		Response response1 = cassandraOperation.getRecordsByProperty(tenantPreferenceDbInfo.getKeySpace(),
				tenantPreferenceDbInfo.getTableName(), JsonKey.ORG_ID, orgId);
		List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);

		for (Map<String, Object> map : req) {
			String role = (String) map.get(JsonKey.ROLE);

			if (ProjectUtil.isStringNullOREmpty(orgId) || ProjectUtil.isStringNullOREmpty(role)) {
				finalResponse.getResult().put(role, "OrgId , role can not be null .");
			}
			// check whether already tenant pref has created for the given org and role if
			// yes show error
			boolean flag = true;
			for (Map<String, Object> m : list) {
				if (role.equalsIgnoreCase((String) m.get(JsonKey.ROLE))) {
					Map<String, Object> responseMap = new HashMap<>();
					responseMap.put(JsonKey.ROLE, role);
					responseMap.put(JsonKey.STATUS, "Tenant preference already exist for role ");
					responseList.add(responseMap);
					flag = false;
					break;
				}
			}

			if (flag) {
				Map<String, Object> dbMap = new HashMap<String, Object>();
				String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
				dbMap.put(JsonKey.ID, id);
				dbMap.put(JsonKey.ORG_ID, orgId);
				dbMap.put(JsonKey.ROLE, role);
				dbMap.put(JsonKey.DATA, (String) map.get(JsonKey.DATA));
				cassandraOperation.insertRecord(tenantPreferenceDbInfo.getKeySpace(),
						tenantPreferenceDbInfo.getTableName(), dbMap);
				Map<String, Object> responseMap = new HashMap<>();
				responseMap.put(JsonKey.ID, id);
				responseMap.put(JsonKey.ROLE, role);
				responseMap.put(JsonKey.STATUS, JsonKey.SUCCESS);
				responseList.add(responseMap);
				finalResponse.getResult().put(role, JsonKey.SUCCESS);
			}
		}
		finalResponse.getResult().put(JsonKey.RESPONSE, responseList);
		sender().tell(finalResponse, self());
	}

	/**
	 * Methos to update the Terms and condition status of user , the status may be
	 * ACCEPTED or REJECTED .
	 * 
	 * @param actorMessage
	 */
	private void updateTcStatusOfUser(Request actorMessage) {

		ProjectLogger.log("TenantPreferenceManagementActor-updateTcStatusOfUser called");
		Map<String, Object> reqBody = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.TENANT_PREFERENCE);
		String requestedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

		Response response1 = cassandraOperation.getRecordById(userDbInfo.getKeySpace(), userDbInfo.getTableName(),
				requestedBy);

		String tcStatus = (String) reqBody.get(JsonKey.TERM_AND_CONDITION_STATUS);
		Map<String, Object> userMap = ((List<Map<String, Object>>) response1.get(JsonKey.RESPONSE)).get(0);

		Map<String, Object> dbMap = new HashMap<>();
		dbMap.put(JsonKey.TERM_AND_CONDITION_STATUS, tcStatus);
		dbMap.put(JsonKey.TC_UPDATED_DATE, format.format(new Date()));
		dbMap.put(JsonKey.ID, userMap.get(JsonKey.ID));

		// update cassandra
		cassandraOperation.updateRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), dbMap);
		// update elastic search
		dbMap.remove(JsonKey.ID);
		ElasticSearchUtil.updateData(ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), requestedBy,
				dbMap);
		Response finalResponse = new Response();
		finalResponse.getResult().put(JsonKey.RESPONSE, JsonKey.SUCCESS);
		sender().tell(finalResponse, self());

	}

}
