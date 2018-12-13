package org.sunbird.learner.util;

import com.warrenstrange.googleauth.GoogleAuthenticator;
import com.warrenstrange.googleauth.GoogleAuthenticatorKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util.DbInfo;

public class OTPUtil {

  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);
  private static DbInfo userDb = Util.dbInfoMap.get(JsonKey.USER_DB);

  private OTPUtil() {}

  @SuppressWarnings("unchecked")
  public static void checkPhoneUniqueness(String phone) {
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
  public static void checkEmailUniqueness(String email) {
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
      Map<String, Object> filters = new HashMap<>();
      filters.put(JsonKey.ENC_EMAIL, encEmail);
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.FILTERS, filters);
      SearchDTO searchDto = Util.createSearchDto(map);
      Map<String, Object> result =
          ElasticSearchUtil.complexSearch(
              searchDto,
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.user.getTypeName());
      List<Map<String, Object>> userMapList =
          (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
      if (!userMapList.isEmpty()) {
        ProjectCommonException.throwClientErrorException(ResponseCode.emailInUse, null);
      }
    }
  }

  public static String generateOTP() {
    GoogleAuthenticator gAuth = new GoogleAuthenticator();
    GoogleAuthenticatorKey key = gAuth.createCredentials();
    String secret = key.getKey();
    int code = gAuth.getTotpPassword(secret);
    return String.valueOf(code);
  }
}
