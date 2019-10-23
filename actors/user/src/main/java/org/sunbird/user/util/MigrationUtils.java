package org.sunbird.user.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.bean.ClaimStatus;
import org.sunbird.bean.ShadowUser;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.helper.ServiceFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrationUtils {

    private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private static ObjectMapper mapper = new ObjectMapper();

    public static ShadowUser getRecordByUserId(String userId) {
        ShadowUser shadowUser=null;
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.USER_ID,userId);
        Response response = cassandraOperation.getRecordsByProperties(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap, null);
        if(!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            shadowUser = mapper.convertValue(((List) response.getResult().get(JsonKey.RESPONSE)).get(0), ShadowUser.class);
        }
        return shadowUser;
    }

    public static void updateRecord(Map<String, Object> propertiesMap, String channel, String userExtId) {
        Map<String, Object> compositeKeysMap = new HashMap<>();
        compositeKeysMap.put(JsonKey.USER_EXT_ID, userExtId);
        compositeKeysMap.put(JsonKey.CHANNEL, channel);
        Response response = cassandraOperation.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap, compositeKeysMap);
        ProjectLogger.log("MigrationUtils:updateRecord:update in cassandra  with userExtId" + userExtId + ":and response is:" + response, LoggerEnum.INFO.name());
    }

    public static void markUserAsRejected(ShadowUser shadowUser) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.CLAIM_STATUS, ClaimStatus.REJECTED.getValue());
        propertiesMap.put(JsonKey.UPDATED_ON, new Timestamp(System.currentTimeMillis()));
        updateRecord(propertiesMap, shadowUser.getChannel(), shadowUser.getUserExtId());
        ProjectLogger.log("MigrationUtils:markUserAsRejected:update in cassandra  with userExtId" + shadowUser.getUserExtId(),LoggerEnum.INFO.name());
    }

    public static ShadowUser getRecord(String userId,String extUserId) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.USER_EXT_ID, extUserId);
        propertiesMap.put(JsonKey.USER_ID, userId);
        Response response = cassandraOperation.getRecordsByProperties(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap, null);
        ShadowUser shadowUser = mapper.convertValue(((List) response.getResult().get(JsonKey.RESPONSE)).get(0), ShadowUser.class);
        return shadowUser;
    }
}