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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrationUtils {

    private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private static ObjectMapper mapper = new ObjectMapper();

    /**
     * this method will search user in userids attribute in shadow_user table
     * @param userId
     * @return
     */
    public static ShadowUser getRecordByUserId(String userId) {
        ShadowUser shadowUser=null;
        Response response = cassandraOperation.searchValueInList(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, JsonKey.USERIDS, userId);
        if(!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            shadowUser = mapper.convertValue(((List) response.getResult().get(JsonKey.RESPONSE)).get(0), ShadowUser.class);
        }
        return shadowUser;
    }

    /**
     * this method will update the record in the shadow_user table
     * @param propertiesMap
     * @param channel
     * @param userExtId
     */
    public static void updateRecord(Map<String, Object> propertiesMap, String channel, String userExtId) {
        Map<String, Object> compositeKeysMap = new HashMap<>();
        compositeKeysMap.put(JsonKey.USER_EXT_ID, userExtId);
        compositeKeysMap.put(JsonKey.CHANNEL, channel);
        Response response = cassandraOperation.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap, compositeKeysMap);
        ProjectLogger.log("MigrationUtils:updateRecord:update in cassandra  with userExtId" + userExtId + ":and response is:" + response, LoggerEnum.INFO.name());
    }

    /**
     * this method will mark the user rejected(2) in shadow_user table
     * if the user doesn't want to migrate
     * @param shadowUser
     */
    public static void markUserAsRejected(ShadowUser shadowUser) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.CLAIM_STATUS, ClaimStatus.REJECTED.getValue());
        propertiesMap.put(JsonKey.UPDATED_ON, new Timestamp(System.currentTimeMillis()));
        updateRecord(propertiesMap, shadowUser.getChannel(), shadowUser.getUserExtId());
        ProjectLogger.log("MigrationUtils:markUserAsRejected:update in cassandra  with userExtId" + shadowUser.getUserExtId(),LoggerEnum.INFO.name());
    }


    /**
     * this method will return all the ELIGIBLE user with same userId in shadow_user table
     * @param userId
     * @return
     */
    public  static List<ShadowUser> getEligibleUsersById(String userId) {
        List<ShadowUser>shadowUsersList=new ArrayList<>();
        Response response = cassandraOperation.searchValueInList(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, JsonKey.USERIDS, userId);
        if(!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            ((List) response.getResult().get(JsonKey.RESPONSE)).stream().forEach(shadowMap->{
                ShadowUser shadowUser=mapper.convertValue(shadowMap,ShadowUser.class);
                if(shadowUser.getClaimStatus()==ClaimStatus.ELIGIBLE.getValue()) {
                    shadowUsersList.add(shadowUser);
                }
            });
        }
        return shadowUsersList;
    }
}