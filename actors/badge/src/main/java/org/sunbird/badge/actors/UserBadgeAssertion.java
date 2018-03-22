package org.sunbird.badge.actors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.TelemetryUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

@ActorConfig(tasks = {}, asyncTasks = {"assignBadgeToUser", "revokeBadgeFromUser"})
public class UserBadgeAssertion extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private DbInfo dbInfo = Util.dbInfoMap.get(BadgingJsonKey.USER_BADGE_ASSERTION_DB);

    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, JsonKey.USER);
        ExecutionContext.setRequestId(request.getRequestId());
        String operation = request.getOperation();
        if (BadgeOperations.assignBadgeToUser.name().equalsIgnoreCase(operation)) {
            addBadgeData(request);
        } else if (BadgeOperations.revokeBadgeFromUser.name().equalsIgnoreCase(operation)) {
            revokeBadgeData(request);
        }
    }

    private void revokeBadgeData(Request request) {
        // request came to revoke the badge from user
        Map<String, Object> badge = getBadgeAssertion(request);
        cassandraOperation.deleteRecord(dbInfo.getKeySpace(), dbInfo.getTableName(),
                (String) badge.get(BadgingJsonKey.ASSERTION_ID));
        updateUserBadgeDataToES(badge);
        tellToSender(request, badge);
    }

    private void addBadgeData(Request request) {
        // request came to assign the badge from user
        Map<String, Object> badge = getBadgeAssertion(request);
        cassandraOperation.insertRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), badge);
        updateUserBadgeDataToES(badge);
        tellToSender(request, badge);
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
            Iterator<Map<String, Object>> itr = badgeAssertionsList.iterator();
            while (itr.hasNext()) {
                Map<String, Object> tempMap = itr.next();
                if (((String) tempMap.get(JsonKey.ID))
                        .equalsIgnoreCase((String) map.get(JsonKey.ID))) {
                    itr.remove();
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
        if (!response) {
            ProjectLogger.log("unbale to save the data inside ES for user badge " + identifier,
                    LoggerEnum.INFO.name());
        }
        return response;

    }

    private Map<String, Object> getBadgeAssertion(Request request) {
        String userId = (String) request.getRequest().get(JsonKey.ID);
        @SuppressWarnings("unchecked")
        Map<String, Object> badge =
                (Map<String, Object>) request.getRequest().get(BadgingJsonKey.BADGE_ASSERTION);
        badge.put(JsonKey.USER_ID, userId);
        badge.put(JsonKey.ID, badge.get(BadgingJsonKey.ASSERTION_ID));
        // removing status from map
        badge.remove(JsonKey.STATUS);
        return badge;
    }

    private void tellToSender(Request request, Map<String, Object> badge) {
        Response reponse = new Response();
        reponse.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
        sender().tell(reponse, self());
        sendTelemetry(request.getRequest(), badge);
    }

    private void sendTelemetry(Map<String, Object> map, Map<String, Object> badge) {
        List<Map<String, Object>> correlatedObject = new ArrayList<>();
        Map<String, Object> targetObject;
        String userId = (String) badge.get(JsonKey.USER_ID);
        targetObject =
                TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        TelemetryUtil.generateCorrelatedObject((String) badge.get(BadgingJsonKey.ASSERTION_ID),
                BadgingJsonKey.BADGE_ASSERTION, null, correlatedObject);
        TelemetryUtil.generateCorrelatedObject(userId, JsonKey.USER, null, correlatedObject);
        TelemetryUtil.telemetryProcessingCall(map, targetObject, correlatedObject);
    }

}
