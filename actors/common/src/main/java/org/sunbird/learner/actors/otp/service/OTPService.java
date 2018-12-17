package org.sunbird.learner.actors.otp.service;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

public class OTPService {
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private DbInfo otpDb = Util.dbInfoMap.get(JsonKey.OTP);

  public Map<String, Object> getOTPDetailsByKey(String type, String key) {
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.TYPE, type);
    request.put(JsonKey.KEY, key);

    Response result =
        cassandraOperation.getRecordById(otpDb.getKeySpace(), otpDb.getTableName(), request);

    List<Map<String, Object>> otpMapList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(otpMapList)) {
      return null;
    }

    return otpMapList.get(0);
  }

  public void insertOTPDetails(String type, String key, String otp) {
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.TYPE, type);
    request.put(JsonKey.KEY, key);
    request.put(JsonKey.OTP, otp);
    request.put(JsonKey.CREATED_ON, new Timestamp(Calendar.getInstance().getTimeInMillis()));

    String expirationInSeconds =
        PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_OTP_EXPIRATION);
    int ttl = Integer.valueOf(expirationInSeconds);

    Response response =
        cassandraOperation.insertRecordWithTTL(
            otpDb.getKeySpace(), otpDb.getTableName(), request, ttl);
  }

}
