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
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;

public class UserBadgeAssertion extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

    public static void init() {
        BackgroundRequestRouter.registerActor(BadgeNotifier.class,
                Arrays.asList(BadgeOperations.addBadgeDataToUser.name()));
    }

    @Override
    public void onReceive(Request request) throws Throwable {
        String operation = request.getOperation();
        if (BadgeOperations.addBadgeDataToUser.name().equalsIgnoreCase(operation)) {
            addBadgeDataToUser(request);
        }
    }

    private void addBadgeDataToUser(Request request) {
        try {
            Map<String, Object> map = request.getRequest();
            map.put("id", map.get("assertionId"));
            Response response = cassandraOperation.upsertRecord("keyspaceName", "tableName", map);
            if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
                updateUserBadgeData(map);
            }
        } catch (Exception ex) {
            ProjectLogger.log("Exception occurred while adding badge data to user.", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void updateUserBadgeData(Map<String, Object> map) {
        Map<String, Object> result =
                ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.user.getTypeName(), (String) map.get(JsonKey.USER_ID));
        if (result.containsKey("badgeAssertions") && null != result.get("badgeAssertions")) {
            List<Map<String, Object>> badgeAssertionsList =
                    (List<Map<String, Object>>) result.get("badgeAssertions");
            if (((boolean) map.get("isRevoked"))) {
                for (Map<String, Object> tempMap : badgeAssertionsList) {
                    if (((String) tempMap.get("id")).equalsIgnoreCase((String) map.get("id"))) {
                        badgeAssertionsList.remove(tempMap);
                    }
                }
            } else {
                badgeAssertionsList.add(map);
            }
        } else {
            List<Map<String, Object>> mapList = new ArrayList<>();
            mapList.add(map);
            result.put("badgeAssertions", mapList);
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
        ProjectLogger.log("unbale to save the data inside ES with identifier " + identifier,
                LoggerEnum.INFO.name());
        return false;

    }
}
