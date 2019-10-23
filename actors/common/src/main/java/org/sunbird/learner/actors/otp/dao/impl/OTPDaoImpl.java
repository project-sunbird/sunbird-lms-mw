package org.sunbird.learner.actors.otp.dao.impl;

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
import org.sunbird.learner.actors.otp.dao.OTPDao;
import org.sunbird.learner.util.Util;

public class OTPDaoImpl implements OTPDao {

  private static final String TABLE_NAME = JsonKey.OTP;
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static volatile OTPDao otpDao;

  public static OTPDao getInstance() {
    if (otpDao == null) {
      synchronized (OTPDaoImpl.class) {
        if (otpDao == null) {
          otpDao = new OTPDaoImpl();
        }
      }
    }
    return otpDao;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Object> getOTPDetails(String type, String key) {
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.TYPE, type);
    request.put(JsonKey.KEY, key);
    Response result = cassandraOperation.getRecordById(Util.KEY_SPACE_NAME, TABLE_NAME, request);
    List<Map<String, Object>> otpMapList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(otpMapList)) {
      return null;
    }
    return otpMapList.get(0);
  }

  @Override
  public void insertOTPDetails(String type, String key, String otp) {
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.TYPE, type);
    request.put(JsonKey.KEY, key);
    request.put(JsonKey.OTP, otp);
    request.put(JsonKey.CREATED_ON, new Timestamp(Calendar.getInstance().getTimeInMillis()));
    String expirationInSeconds =
        PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_OTP_EXPIRATION);
    int ttl = Integer.valueOf(expirationInSeconds);
    cassandraOperation.insertRecordWithTTL(Util.KEY_SPACE_NAME, TABLE_NAME, request, ttl);
  }

  @Override
  public void deleteOtp(String type, String key) {
    Map<String,String>compositeKeyMap=new HashMap<>();
    compositeKeyMap.put(JsonKey.TYPE,type);
    compositeKeyMap.put(JsonKey.KEY,key);
    cassandraOperation.deleteRecord(JsonKey.SUNBIRD,TABLE_NAME,compositeKeyMap);
    ProjectLogger.log("OTPDaoImpl:deleteOtp:otp deleted", LoggerEnum.INFO.name());
  }

}
