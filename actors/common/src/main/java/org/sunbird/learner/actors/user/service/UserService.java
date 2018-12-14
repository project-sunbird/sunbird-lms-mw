/**
 * 
 */
package org.sunbird.learner.actors.user.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

/**
 * @author Rahul Kumar
 *
 */
public class UserService {
	
	private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
	  private EncryptionService encryptionService =
	      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
	          null);
	  private DbInfo userDb = Util.dbInfoMap.get(JsonKey.USER_DB);

	@SuppressWarnings("unchecked")
	  public void checkPhoneUniqueness(String phone) {
	    // Get Phone configuration if not found , by default phone will be unique across
	    // the application
	    String phoneSetting = DataCacheHandler.getConfigSettings().get(JsonKey.PHONE_UNIQUE);
	    if (StringUtils.isNotBlank(phoneSetting)
	        && Boolean.parseBoolean(phoneSetting)
	        && StringUtils.isNotBlank(phone)) {
	      String encPhone = null;
	      try {
	        encPhone = encryptionService.encryptData(phone);
	      } catch (Exception e) {
	        ProjectLogger.log("Exception occurred while encrypting phone number ", e);
	      }
	      Response result =
	          cassandraOperation.getRecordsByIndexedProperty(
	              userDb.getKeySpace(), userDb.getTableName(), (JsonKey.PHONE), encPhone);
	      List<Map<String, Object>> userMapList =
	          (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
	      if (!userMapList.isEmpty()) {
	        ProjectCommonException.throwClientErrorException(ResponseCode.PhoneNumberInUse, null);
	      }
	    }
	  }

	  @SuppressWarnings("unchecked")
	  public void checkEmailUniqueness(String email) {
	    // Get Email configuration if not found , by default Email can be duplicate
	    // across the
	    // application
	    String emailSetting = DataCacheHandler.getConfigSettings().get(JsonKey.EMAIL_UNIQUE);
	    if (StringUtils.isNotBlank(emailSetting)
	        && Boolean.parseBoolean(emailSetting)
	        && StringUtils.isNotBlank(email)) {
	      String encEmail = null;
	      try {
	        encEmail = encryptionService.encryptData(email);
	      } catch (Exception e) {
	        ProjectLogger.log("Exception occurred while encrypting Email ", e);
	      }
	      Response result =
	              cassandraOperation.getRecordsByIndexedProperty(
	                  userDb.getKeySpace(), userDb.getTableName(), (JsonKey.EMAIL), encEmail);
	      List<Map<String, Object>> userMapList =
	              (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
	      if (!userMapList.isEmpty()) {
	        ProjectCommonException.throwClientErrorException(ResponseCode.emailInUse, null);
	      }
	    }
	  }
}
