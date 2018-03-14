package org.sunbird.badge.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

public class UserBadgeAssertion extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private DbInfo dbInfo = Util.dbInfoMap.get(BadgingJsonKey.USER_BADGE_ASSERTION_DB);

    public static void init() {
        BackgroundRequestRouter.registerActor(BadgeNotifier.class,
                Arrays.asList(BadgeOperations.addBadgeDataToUser.name()));
    }

    @Override
    public void onReceive(Request request) throws Throwable {
        String operation = request.getOperation();
        if (BadgeOperations.addBadgeDataToUser.name().equalsIgnoreCase(operation)) {
            updateBadgeData(request);
        }
    }

    @SuppressWarnings("unchecked")
    private void updateBadgeData(Request request) {
        try {
            Map<String, Object> map = request.getRequest();
            String userId = (String) map.get(JsonKey.USER_ID);
            Map<String, Object> badge = (Map<String, Object>) map.get("badge");
            String id = ((String) badge.get("assertionId") + JsonKey.PRIMARY_KEY_DELIMETER
                    + (String) badge.get("issuerId") + JsonKey.PRIMARY_KEY_DELIMETER
                    + (String) badge.get("badgeClassId"));
            Response response = cassandraOperation.getRecordById(dbInfo.getKeySpace(),
                    dbInfo.getTableName(), id);
            List<Map<String, Object>> resList =
                    (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

            if (null != resList && !resList.isEmpty()) {
                // request came to revoke the badge from user
                // Delete the badge details
                cassandraOperation.deleteRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), id);
                badge.put(JsonKey.ID, id);
                badge.put(JsonKey.USER_ID, userId);
                updateUserBadgeDataToES(badge);
            } else {
                // request came to assign the badge to user
                badge.put(JsonKey.ID, id);
                badge.put(JsonKey.USER_ID, userId);
                cassandraOperation.insertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), badge);
                updateUserBadgeDataToES(badge);
            }
        } catch (Exception ex) {
            ProjectLogger.log("Exception occurred while adding badge data to user.", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void updateUserBadgeDataToES(Map<String, Object> map) {
        Map<String, Object> result =
                ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.user.getTypeName(), (String) map.get(JsonKey.USER_ID));
        if (result.containsKey(BadgingJsonKey.BADGE_ASSERTIONS)
                && null != result.get(BadgingJsonKey.BADGE_ASSERTIONS)) {
            List<Map<String, Object>> badgeAssertionsList =
                    (List<Map<String, Object>>) result.get(BadgingJsonKey.BADGE_ASSERTIONS);

            boolean bool = true;
            for (Map<String, Object> tempMap : badgeAssertionsList) {
                if (((String) tempMap.get(JsonKey.ID))
                        .equalsIgnoreCase((String) map.get(JsonKey.ID))) {
                    badgeAssertionsList.remove(tempMap);
                    bool = false;
                }
            }
            if (bool) {
                badgeAssertionsList.add(map);
            }
        } else {
            List<Map<String, Object>> mapList = new ArrayList<>();
            mapList.add(map);
            result.put(BadgingJsonKey.BADGE_ASSERTIONS, mapList);
        }
        updateDataToElastic(ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName(), (String) result.get(JsonKey.IDENTIFIER),
                result);

    }

    private boolean updateDataToElastic(String indexName, String typeName, String identifier,
            Map<String, Object> data) {
        boolean response = ElasticSearchUtil.updateData(indexName, typeName, identifier, data);
        if (response) {
            return true;
        }
        ProjectLogger.log("unbale to save the data inside ES for user badge " + identifier,
                LoggerEnum.INFO.name());
        return false;

    }
}
