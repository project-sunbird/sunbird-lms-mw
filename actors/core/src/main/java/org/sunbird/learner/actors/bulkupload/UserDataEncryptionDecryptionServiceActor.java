package org.sunbird.learner.actors.bulkupload;

import java.util.List;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RouterConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;

@RouterConfig(request = { "encryptUserData", "decryptUserData" }, bgRequest = {})
public class UserDataEncryptionDecryptionServiceActor extends BaseActor {

	private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
	private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
	private Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);

	@Override
	public void onReceive(Request actorMessage) throws Throwable {
		if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.ENCRYPT_USER_DATA.getValue())) {
			encryptUserData(actorMessage);
		} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.DECRYPT_USER_DATA.getValue())) {
			decryptUserData(actorMessage);
		} else {
			onReceiveUnsupportedOperation(actorMessage.getOperation());
		}
	}

	private void decryptUserData(Request actorMessage) {
		ProjectLogger.log("DecryptUserData API called by " + actorMessage.getRequest().get(JsonKey.REQUESTED_BY));
		long start = System.currentTimeMillis();
		Response resp = new Response();
		resp.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
		sender().tell(resp, self());
		Response response = cassandraOperation.getAllRecords(usrDbInfo.getKeySpace(), usrDbInfo.getTableName());
		List<Map<String, Object>> userList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
		int i = 0;
		for (Map<String, Object> userMap : userList) {
			if (!ProjectUtil.isEmailvalid((String) userMap.get(JsonKey.EMAIL))) {
				decryptUserDataAndUpdateDb(userMap);
				i++;
			}
		}
		ProjectLogger.log("Total No. of user data to decrypt ::: " + i);
		long end = System.currentTimeMillis();
		ProjectLogger.log("total time taken by application to decrypt user data:::: " + (end - start));
	}

	private void decryptUserDataAndUpdateDb(Map<String, Object> userMap) {
		try {
			UserUtility.decryptUserData(userMap);
			cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userMap);
			getUserAddressDataDecryptAndUpdateDb((String) userMap.get(JsonKey.ID));
		} catch (Exception e) {
			ProjectLogger.log(
					"Exception Occurred while decrypting user data for userId " + ((String) userMap.get(JsonKey.ID)),
					e);
		}

	}

	private void getUserAddressDataDecryptAndUpdateDb(String userId) {
		Response response = cassandraOperation.getRecordsByProperty(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(),
				JsonKey.USER_ID, userId);
		List<Map<String, Object>> addressList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
		try {
			UserUtility.decryptUserAddressData(addressList);
			for (Map<String, Object> address : addressList) {
				cassandraOperation.updateRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
			}
		} catch (Exception e) {
			ProjectLogger.log("Exception Occurred while decrypting user address data for userId " + (userId), e);
		}

	}

	private void encryptUserData(Request actorMessage) {
		ProjectLogger.log("EncryptUserData API called by " + actorMessage.getRequest().get(JsonKey.REQUESTED_BY));
		long start = System.currentTimeMillis();
		Response resp = new Response();
		resp.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
		sender().tell(resp, self());
		Response response = cassandraOperation.getAllRecords(usrDbInfo.getKeySpace(), usrDbInfo.getTableName());
		List<Map<String, Object>> userList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
		int i = 0;
		for (Map<String, Object> userMap : userList) {
			if (ProjectUtil.isEmailvalid((String) userMap.get(JsonKey.EMAIL))) {
				encryptUserDataAndUpdateDb(userMap);
				i++;
			}
		}
		ProjectLogger.log("Total No. of user data to encrypt ::: " + i);
		long end = System.currentTimeMillis();
		ProjectLogger.log("total time taken by application to encrypt user data:::: " + (end - start));
	}

	private void encryptUserDataAndUpdateDb(Map<String, Object> userMap) {
		try {
			UserUtility.encryptUserData(userMap);
			cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userMap);
			getUserAddressDataEncryptAndUpdateDb((String) userMap.get(JsonKey.ID));
		} catch (Exception e) {
			ProjectLogger.log("Exception Occurred while encrypting user data ", e);
		}

	}

	private void getUserAddressDataEncryptAndUpdateDb(String userId) {
		Response response = cassandraOperation.getRecordsByProperty(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(),
				JsonKey.USER_ID, userId);
		List<Map<String, Object>> addressList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
		try {
			UserUtility.encryptUserAddressData(addressList);
			for (Map<String, Object> address : addressList) {
				cassandraOperation.updateRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
			}
		} catch (Exception e) {
			ProjectLogger.log("Exception Occurred while encrypting user data ", e);
		}
	}
}
