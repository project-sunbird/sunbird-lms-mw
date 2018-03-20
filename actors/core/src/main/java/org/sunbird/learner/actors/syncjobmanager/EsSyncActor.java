package org.sunbird.learner.actors.syncjobmanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.DataMaskingService;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.services.ProfileCompletenessService;
import org.sunbird.common.services.impl.ProfileCompletenessFactory;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

/**
 * This class is used to sync the ElasticSearch and DB.
 * 
 * @author Amit Kumar
 *
 */
public class EsSyncActor extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo userSkillDbInfo = Util.dbInfoMap.get(JsonKey.USER_SKILL_DB);
    private EncryptionService service =
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                    .getEncryptionServiceInstance(null);
    private DataMaskingService maskingService =
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                    .getMaskingServiceInstance(null);
    private DecryptionService decService =
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                    .getDecryptionServiceInstance(null);

    public static void init() {
        RequestRouter.registerActor(EsSyncActor.class,
                Arrays.asList(ActorOperations.SYNC.getValue()));

    }

    @Override
    public void onReceive(Request request) throws Throwable {
        String requestedOperation = request.getOperation();
        ProjectLogger.log("Operation name is ==" + requestedOperation);
        if (requestedOperation.equalsIgnoreCase(ActorOperations.SYNC.getValue())) {
            // return SUCCESS to controller and run the sync process in background
            Response response = new Response();
            response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
            sender().tell(response, self());
            syncData(request);
        } else {
            onReceiveUnsupportedOperation(request.getOperation());
        }
    }

    private void syncData(Request message) {
        ProjectLogger.log("DB data sync operation to elastic search started ");
        long startTime = System.currentTimeMillis();
        Map<String, Object> req = message.getRequest();
        Map<String, Object> responseMap = new HashMap<>();
        List<Map<String, Object>> reponseList = null;
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> dataMap = (Map<String, Object>) req.get(JsonKey.DATA);
        String objectType = (String) dataMap.get(JsonKey.OBJECT_TYPE);
        List<Object> objectIds = null;
        if (dataMap.containsKey(JsonKey.OBJECT_IDS) && null != dataMap.get(JsonKey.OBJECT_IDS)) {
            objectIds = (List<Object>) dataMap.get(JsonKey.OBJECT_IDS);
        }
        Util.DbInfo dbInfo = getDbInfoObj(objectType);
        if (null == dbInfo) {
            throw new ProjectCommonException(ResponseCode.invalidObjectType.getErrorCode(),
                    ResponseCode.invalidObjectType.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        if (null != objectIds && !objectIds.isEmpty()) {
            ProjectLogger.log("fetching data for " + objectType + " for these ids "
                    + Arrays.toString(objectIds.toArray()) + " started");
            Response response = cassandraOperation.getRecordsByProperty(dbInfo.getKeySpace(),
                    dbInfo.getTableName(), JsonKey.ID, objectIds);
            reponseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
            ProjectLogger.log("fetching data for " + objectType + " for these ids "
                    + Arrays.toString(objectIds.toArray()) + " done");
        }
        if (null != reponseList && !reponseList.isEmpty()) {
            for (Map<String, Object> map : reponseList) {
                responseMap.put((String) map.get(JsonKey.ID), map);
            }
        } else {
            ProjectLogger.log("fetching all data for " + objectType + " started");
            Response response =
                    cassandraOperation.getAllRecords(dbInfo.getKeySpace(), dbInfo.getTableName());
            reponseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
            ProjectLogger.log("fetching all data for " + objectType + " done");
            ProjectLogger.log("total db data to sync for " + objectType + " to Elastic search "
                    + reponseList.size());
            if (null != reponseList) {
                for (Map<String, Object> map : reponseList) {
                    responseMap.put((String) map.get(JsonKey.ID), map);
                }
            }
        }
        Iterator<Entry<String, Object>> itr = responseMap.entrySet().iterator();
        while (itr.hasNext()) {
            if (objectType.equals(JsonKey.USER)) {
                Map<String, Object> userMap = (Map<String, Object>) itr.next().getValue();
                if (!((boolean) userMap.get(JsonKey.IS_DELETED))) {
                    result.add(getUserDetails(itr.next()));
                }
            } else if (objectType.equals(JsonKey.ORGANISATION)) {
                result.add(getOrgDetails(itr.next()));
            } else if (objectType.equals(JsonKey.BATCH) || objectType.equals(JsonKey.USER_COURSE)) {
                result.add((Map<String, Object>) (itr.next().getValue()));
            }
        }

        ElasticSearchUtil.bulkInsertData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                getType(objectType), result);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        ProjectLogger.log("total time taken to sync db data for " + objectType
                + " to Elastic search " + elapsedTime + " ms.");
    }

    private String getType(String objectType) {
        String type = "";
        if (objectType.equals(JsonKey.USER)) {
            type = ProjectUtil.EsType.user.getTypeName();
        } else if (objectType.equals(JsonKey.ORGANISATION)) {
            type = ProjectUtil.EsType.organisation.getTypeName();
        } else if (objectType.equals(JsonKey.BATCH)) {
            type = ProjectUtil.EsType.course.getTypeName();
        } else if (objectType.equals(JsonKey.USER_COURSE)) {
            type = ProjectUtil.EsType.usercourses.getTypeName();
        }
        return type;
    }

    private Map<String, Object> getOrgDetails(Entry<String, Object> entry) {
        ProjectLogger.log("fetching org data started");
        Map<String, Object> orgMap = (Map<String, Object>) entry.getValue();
        orgMap.remove(JsonKey.ORG_TYPE);
        if (orgMap.containsKey(JsonKey.ADDRESS_ID)
                && !ProjectUtil.isStringNullOREmpty((String) orgMap.get(JsonKey.ADDRESS_ID))) {
            orgMap.put(JsonKey.ADDRESS, getDetailsById(Util.dbInfoMap.get(JsonKey.ADDRESS_DB),
                    (String) orgMap.get(JsonKey.ADDRESS_ID)));
        }
        ProjectLogger.log("fetching org data completed");
        return orgMap;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getUserDetails(Entry<String, Object> entry) {
        String userId = entry.getKey();
        ProjectLogger.log("fetching user data started");
        Map<String, Object> userMap = (Map<String, Object>) entry.getValue();
        Util.removeAttributes(userMap, Arrays.asList(JsonKey.PASSWORD, JsonKey.UPDATED_BY));
        if (ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.COUNTRY_CODE))) {
            userMap.put(JsonKey.COUNTRY_CODE,
                    PropertiesCache.getInstance().getProperty("sunbird_default_country_code"));
        }
        ProjectLogger.log("fetching user address data started");
        String encryption = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_ENCRYPTION);
        String uid = userId;
        if ("ON".equalsIgnoreCase(encryption)) {
            try {
                uid = service.encryptData(uid);
            } catch (Exception e) {
                ProjectLogger.log("Exception Occurred while encrypting userId in user search api ",
                        e);
            }
        }
        userMap.put(JsonKey.ADDRESS,
                getDetails(Util.dbInfoMap.get(JsonKey.ADDRESS_DB), uid, JsonKey.USER_ID));
        ProjectLogger.log("fetching user education data started");
        List<Map<String, Object>> eduMap =
                getDetails(Util.dbInfoMap.get(JsonKey.EDUCATION_DB), userId, JsonKey.USER_ID);
        for (Map<String, Object> map : eduMap) {
            if (map.containsKey(JsonKey.ADDRESS_ID)
                    && !ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.ADDRESS_ID))) {
                map.put(JsonKey.ADDRESS, getDetailsById(Util.dbInfoMap.get(JsonKey.ADDRESS_DB),
                        (String) map.get(JsonKey.ADDRESS_ID)));
            }
        }
        userMap.put(JsonKey.EDUCATION, eduMap);
        ProjectLogger.log("fetching user job profile data started");
        List<Map<String, Object>> jobMap =
                getDetails(Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB), userId, JsonKey.USER_ID);
        for (Map<String, Object> map : jobMap) {
            if (map.containsKey(JsonKey.ADDRESS_ID)
                    && !ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.ADDRESS_ID))) {
                map.put(JsonKey.ADDRESS, getDetailsById(Util.dbInfoMap.get(JsonKey.ADDRESS_DB),
                        (String) map.get(JsonKey.ADDRESS_ID)));
            }
        }
        userMap.put(JsonKey.JOB_PROFILE, jobMap);
        ProjectLogger.log("fetching user org data started");
        List<Map<String, Object>> orgMapList =
                getDetails(Util.dbInfoMap.get(JsonKey.USER_ORG_DB), userId, JsonKey.USER_ID);
        List<Map<String, Object>> mapList = new ArrayList<>();
        for (Map<String, Object> temp : orgMapList) {
            if (null != temp.get(JsonKey.IS_DELETED) && !(boolean) temp.get(JsonKey.IS_DELETED)) {
                mapList.add(temp);
            }
        }
        userMap.put(JsonKey.ORGANISATIONS, mapList);

        ProjectLogger.log("fetching user Badge data  started");
        List<Map<String, Object>> badgesMap =
                getDetails(Util.dbInfoMap.get(BadgingJsonKey.USER_BADGE_ASSERTION_DB), userId,
                        JsonKey.USER_ID);
        userMap.put(BadgingJsonKey.BADGE_ASSERTIONS, badgesMap);

        // save masked email and phone number
        String phone = (String) userMap.get(JsonKey.PHONE);
        String email = (String) userMap.get(JsonKey.EMAIL);
        if (!ProjectUtil.isStringNullOREmpty(phone)) {
            userMap.put(JsonKey.ENC_PHONE, phone);
            userMap.put(JsonKey.PHONE, maskingService.maskPhone(decService.decryptData(phone)));
        }
        if (!ProjectUtil.isStringNullOREmpty(email)) {
            userMap.put(JsonKey.ENC_EMAIL, email);
            userMap.put(JsonKey.EMAIL, maskingService.maskEmail(decService.decryptData(email)));
        }
        // add the skills column into ES
        Response skillresponse =
                cassandraOperation.getRecordsByProperty(userSkillDbInfo.getKeySpace(),
                        userSkillDbInfo.getTableName(), JsonKey.USER_ID, userId);
        List<Map<String, Object>> responseList =
                (List<Map<String, Object>>) skillresponse.get(JsonKey.RESPONSE);
        userMap.put(JsonKey.SKILLS, responseList);
        // compute profile completeness and error field.
        ProfileCompletenessService profileService = ProfileCompletenessFactory.getInstance();
        Map<String, Object> profileResponse = profileService.computeProfile(userMap);
        userMap.putAll(profileResponse);
        Map<String, String> profileVisibility =
                (Map<String, String>) userMap.get(JsonKey.PROFILE_VISIBILITY);
        if (null != profileVisibility && !profileVisibility.isEmpty()) {
            Map<String, Object> profileVisibilityMap = new HashMap<>();
            for (String field : profileVisibility.keySet()) {
                profileVisibilityMap.put(field, userMap.get(field));
            }
            ElasticSearchUtil.upsertData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                    ProjectUtil.EsType.userprofilevisibility.getTypeName(), userId,
                    profileVisibilityMap);
            UserUtility.updateProfileVisibilityFields(profileVisibilityMap, userMap);
        } else {
            userMap.put(JsonKey.PROFILE_VISIBILITY, new HashMap<String, String>());
        }
        ProjectLogger.log("fetching user data completed");
        return userMap;
    }

    private Map<String, Object> getDetailsById(DbInfo dbInfo, String userId) {
        try {
            Response response = cassandraOperation.getRecordById(dbInfo.getKeySpace(),
                    dbInfo.getTableName(), userId);
            return ((((List<Map<String, Object>>) response.get(JsonKey.RESPONSE)).isEmpty())
                    ? new HashMap<>()
                    : ((List<Map<String, Object>>) response.get(JsonKey.RESPONSE)).get(0));
        } catch (Exception ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        }
        return null;
    }

    private List<Map<String, Object>> getDetails(DbInfo dbInfo, String id, String property) {
        try {
            Response response = cassandraOperation.getRecordsByProperty(dbInfo.getKeySpace(),
                    dbInfo.getTableName(), property, id);
            return (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        } catch (Exception ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        }
        return Collections.emptyList();
    }

    private DbInfo getDbInfoObj(String objectType) {
        if (objectType.equals(JsonKey.USER)) {
            return Util.dbInfoMap.get(JsonKey.USER_DB);
        } else if (objectType.equals(JsonKey.ORGANISATION)) {
            return Util.dbInfoMap.get(JsonKey.ORG_DB);
        } else if (objectType.equals(JsonKey.BATCH)) {
            return Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
        } else if (objectType.equals(JsonKey.USER_COURSE)) {
            return Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
        }

        return null;
    }
}
