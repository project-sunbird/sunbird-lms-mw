package org.sunbird.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.bean.ClaimStatus;
import org.sunbird.bean.ShadowUser;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.UserType;
import scala.concurrent.Future;

import java.sql.Timestamp;
import java.util.*;

public class ShadowUserProcessor {
    private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private ObjectMapper mapper = new ObjectMapper();
    private Map<String, String> hashTagIdMap = new HashMap<>();
    private Map<String, String> extOrgIdMap = new HashMap<>();
    private Map<String,String>channelOrgIdMap=new HashMap<>();
    private String custodianOrgId;

    private EncryptionService encryptionService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(null);
    private ElasticSearchService elasticSearchService = EsClientFactory.getInstance(JsonKey.REST);

    public void process() {
        ProjectLogger.log("ShadowUserProcessor:process:process started for updating users", LoggerEnum.INFO.name());
        List<ShadowUser> shadowUserList = getShadowUserFromDb();
        ProjectLogger.log("ShadowUserProcessor:process:list of shadow user got ".concat(shadowUserList.size() + ""), LoggerEnum.INFO.name());
        shadowUserList.stream().forEach(singleShadowUser -> {
            processSingleShadowUser(singleShadowUser);
        });
        ProjectLogger.log("ShadowUserProcessor:process:successfully processed shadow user ".concat(shadowUserList.size() + ""), LoggerEnum.INFO.name());
    }

    private void processSingleShadowUser(ShadowUser shadowUser) {
        updateUser(shadowUser);
    }


    /**
     * this method will be called when the user is already claimed need to update the user
     *
     * @param shadowUser
     */
    public void processClaimedUser(ShadowUser shadowUser) {
        ProjectLogger.log("ShadowUserProcessor:processClaimedUser:started claming shadow user with processId:"+shadowUser.getProcessId()+":with shadow user:"+shadowUser.toString(),LoggerEnum.INFO.name());
        String orgId = getOrgId(shadowUser);
        Map<String, Object> esUser = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(elasticSearchService.getDataByIdentifier(ProjectUtil.EsType.user.getTypeName(), shadowUser.getUserId()));
        String userId = (String) esUser.get(JsonKey.ID);
        String rootOrgId = (String) esUser.get(JsonKey.ROOT_ORG_ID);
        if (!((String) esUser.get(JsonKey.FIRST_NAME)).equalsIgnoreCase(shadowUser.getName()) || ((int) esUser.get(JsonKey.STATUS)) != shadowUser.getUserStatus()) {
            updateUserInUserTable(shadowUser.getUserId(), rootOrgId, shadowUser);
        }
        deleteUserFromOrganisations(shadowUser, rootOrgId, (List<Map<String, Object>>) esUser.get(JsonKey.ORGANISATIONS));
        if (StringUtils.isNotBlank(orgId) && !getOrganisationIds(esUser).contains(orgId)) {
            registerUserToOrg(userId, orgId);
        }
        syncUserToES(userId);
        updateUserInShadowDb(userId, shadowUser, ClaimStatus.CLAIMED.getValue(), null);
    }

    private boolean isRootOrgMatchedWithOrgId(String rootOrgId, String orgId) {
        if (StringUtils.equalsIgnoreCase(rootOrgId, orgId)) {
            return true;
        }
        return false;
    }

    private void deleteUserFromOrganisations(ShadowUser shadowUser, String rootOrgId, List<Map<String, Object>> organisations) {
        organisations.stream().forEach(organisation -> {
            String orgId = (String) organisation.get(JsonKey.ORGANISATION_ID);
            if (!isRootOrgMatchedWithOrgId(rootOrgId, orgId)) {
                String id = (String) organisation.get(JsonKey.ID);
                updateStatusInUserOrg(shadowUser, id);
            }
        });
    }

    private void updateStatusInUserOrg(ShadowUser shadowUser, String id) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.ID, id);
        propertiesMap.put(JsonKey.IS_DELETED, true);
        propertiesMap.put(JsonKey.UPDATED_BY, shadowUser.getAddedBy());
        propertiesMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        Response response = cassandraOperation.updateRecord(JsonKey.SUNBIRD, JsonKey.USER_ORG, propertiesMap);
        ProjectLogger.log("ShadowUserProcessor:updateStatusInUserOrg:response from cassandra in updating user org ".concat(response + ""),LoggerEnum.INFO.name());
    }

    private List<Map<String, Object>> getUserMatchedIdentifierFromES(ShadowUser shadowUser) {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        Map<String, Object> or = new HashMap<>();
        if(StringUtils.isNotBlank(shadowUser.getEmail())) {
            or.put(JsonKey.EMAIL,getEncryptedValue(shadowUser.getEmail().toLowerCase()));
        }
        if(StringUtils.isNotBlank(shadowUser.getPhone())) {
            or.put(JsonKey.PHONE,getEncryptedValue(shadowUser.getPhone()));
        }
        filters.put(JsonKey.ES_OR_OPERATION, or);
        filters.put(JsonKey.ROOT_ORG_ID, getCustodianOrgId());
        request.put(JsonKey.FILTERS, filters);
        ProjectLogger.log("ShadowUserProcessor:getUserMatchedIdentifierFromES:the filter prepared for elastic search with processId: "+shadowUser.getProcessId()+" :filters are:"+filters,LoggerEnum.INFO.name());
        SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
        Map<String, Object> response = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(elasticSearchService.search(searchDTO, JsonKey.USER));
        ProjectLogger.log("ShadowUserProcessor:getUserMatchedIdentifierFromES:response got from elasticSearch is with processId: "+shadowUser.getProcessId()+" :respone is"+response,LoggerEnum.INFO.name());
        return (List<Map<String, Object>>) response.get(JsonKey.CONTENT);
    }

    private void updateUser(ShadowUser shadowUser) {
            List<Map<String, Object>> esUser = getUserMatchedIdentifierFromES(shadowUser);
            ProjectLogger.log("ShadowUserProcessor:updateUser:GOT ES RESPONSE FOR USER WITH SIZE " + esUser.size(), LoggerEnum.INFO.name());
            if (CollectionUtils.isNotEmpty(esUser)) {
                if (esUser.size() == 1) {
                    ProjectLogger.log("ShadowUserProcessor:updateUser:Got single user:" + esUser+" :with processId"+shadowUser.getProcessId(),LoggerEnum.INFO.name());
                    Map<String, Object> userMap = esUser.get(0);
                    if (!isSame(shadowUser, userMap)) {
                        ProjectLogger.log("ShadowUserProcessor:updateUser: provided user details doesn't match with existing user details with processId"+shadowUser.getProcessId() + userMap, LoggerEnum.INFO.name());
                        String rootOrgId = getRootOrgIdFromChannel(shadowUser.getChannel());
                        updateUserInUserTable((String) userMap.get(JsonKey.ID), rootOrgId, shadowUser);
                        String orgIdFromOrgExtId = getOrgId(shadowUser);
                        updateUserOrg(orgIdFromOrgExtId, rootOrgId, userMap);
                        createUserExternalId((String) userMap.get(JsonKey.ID), shadowUser);
                        updateUserInShadowDb((String) userMap.get(JsonKey.ID), shadowUser, ClaimStatus.CLAIMED.getValue(), null);
                        syncUserToES((String) userMap.get(JsonKey.ID));
                    }
                } else if (esUser.size() >1) {
                    ProjectLogger.log("ShadowUserProcessor:updateUser:GOT response from ES :" + esUser, LoggerEnum.INFO.name());
                    updateUserInShadowDb(null, shadowUser, ClaimStatus.MULTIMATCH.getValue(), getMatchingUserIds(esUser));
                }
            } else {
                ProjectLogger.log("ShadowUserProcessor:updateUser:SKIPPING SHADOW USER:" + shadowUser.toString(), LoggerEnum.INFO.name());
            }

    }

    private List<String> getMatchingUserIds(List<Map<String, Object>> esUser) {
        ProjectLogger.log("ShadowUserProcessor:getMatchingUserIds:GOT response from counting matchingUserIds:"+esUser.size(),LoggerEnum.INFO.name());
        List<String> matchingUserIds = new ArrayList<>();
        esUser.stream().forEach(singleEsUser -> {
            matchingUserIds.add((String) singleEsUser.get(JsonKey.ID));
        });
        return matchingUserIds;
    }


    private void updateUserOrg(String orgIdFromOrgExtId, String rootOrgId, Map<String, Object> userMap) {
        deleteUserOrganisations(userMap);
        ProjectLogger.log("ShadowUserProcessor:updateUserOrg:deleting user organisation completed no started registering user to org",LoggerEnum.INFO.name());
        registerUserToOrg((String) userMap.get(JsonKey.ID), rootOrgId);
        if (StringUtils.isNotBlank(orgIdFromOrgExtId) && !StringUtils.equalsIgnoreCase(rootOrgId, orgIdFromOrgExtId)) {
            ProjectLogger.log("ShadowUserProcessor:updateUserOrg:user also needs to register with sub org",LoggerEnum.INFO.name());
            registerUserToOrg((String) userMap.get(JsonKey.ID), orgIdFromOrgExtId);
        }
    }

    private void updateUserInUserTable(String userId, String rootOrgId, ShadowUser shadowUser) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.FIRST_NAME, shadowUser.getName());
        propertiesMap.put(JsonKey.ID, userId);
        propertiesMap.put(JsonKey.UPDATED_BY, shadowUser.getAddedBy());
        propertiesMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        if (shadowUser.getUserStatus() == ProjectUtil.Status.ACTIVE.getValue()) {
            propertiesMap.put(JsonKey.IS_DELETED, false);
            propertiesMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
        } else {
            propertiesMap.put(JsonKey.IS_DELETED, true);
            propertiesMap.put(JsonKey.STATUS, ProjectUtil.Status.INACTIVE.getValue());
        }
        propertiesMap.put(JsonKey.USER_TYPE, UserType.TEACHER.getTypeName());
        propertiesMap.put(JsonKey.CHANNEL, shadowUser.getChannel());
        propertiesMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
        ProjectLogger.log("ShadowUserProcessor:updateUserInUserTable: properties map formed for user update:"+propertiesMap,LoggerEnum.INFO.name());
        Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), propertiesMap);
        ProjectLogger.log("ShadowUserProcessor:updateUserInUserTable:user is updated with shadow user:"+shadowUser.toString()+":RESPONSE FROM CASSANDRA IS:"+response+":with processId:"+shadowUser.getProcessId(),LoggerEnum.INFO.name());
    }


    private String getRootOrgIdFromChannel(String channel) {
        String rootOrgId=channelOrgIdMap.get(channel);
        if(StringUtils.isNotBlank(rootOrgId)){
            ProjectLogger.log("ShadowUserProcessor:getRootOrgIdFromChannel: found rootorgid in cache "+rootOrgId, LoggerEnum.INFO.name());
            return rootOrgId;
        }
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(JsonKey.CHANNEL, channel);
        filters.put(JsonKey.IS_ROOT_ORG, true);
        request.put(JsonKey.FILTERS, filters);
        SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
        searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
        Future<Map<String, Object>> esResultF = elasticSearchService.search(searchDTO, ProjectUtil.EsType.organisation.getTypeName());
        Map<String, Object> esResult = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(esResultF);
        if (MapUtils.isNotEmpty(esResult) && CollectionUtils.isNotEmpty((List) esResult.get(JsonKey.CONTENT))) {
            Map<String, Object> esContent = ((List<Map<String, Object>>) esResult.get(JsonKey.CONTENT)).get(0);
            channelOrgIdMap.put(channel,(String) esContent.get(JsonKey.ID));
            return (String) esContent.get(JsonKey.ID);
        }
        return StringUtils.EMPTY;
    }

    private String getEncryptedValue(String key) {
        try {
            return encryptionService.encryptData(key);
        } catch (Exception e) {
            return key;
        }
    }


    /**
     * this method
     *
     * @return
     */
    private String getCustodianOrgId() {
        if (StringUtils.isNotBlank(custodianOrgId)) {
            ProjectLogger.log("ShadowUserProcessor:getCustodianOrgId:CUSTODIAN ORD ID FOUND in cache:"+custodianOrgId, LoggerEnum.INFO.name());
            return custodianOrgId;
        }
        Response response = cassandraOperation.getRecordById(JsonKey.SUNBIRD, JsonKey.SYSTEM_SETTINGS_DB, JsonKey.CUSTODIAN_ORG_ID);
        List<Map<String, Object>> result = new ArrayList<>();
        if (!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            result = ((List) response.getResult().get(JsonKey.RESPONSE));
            Map<String, Object> resultMap = result.get(0);
            custodianOrgId = (String) resultMap.get(JsonKey.VALUE);
            ProjectLogger.log("ShadowUserProcessor:getCustodianOrgId:CUSTODIAN ORD ID FOUND in DB:"+custodianOrgId, LoggerEnum.INFO.name());

        }

        if (StringUtils.isBlank(custodianOrgId)) {
            ProjectLogger.log("ShadowUserProcessor:getCustodianOrgId:No CUSTODIAN ORD ID FOUND PLEASE HAVE THAT IN YOUR ENVIRONMENT", LoggerEnum.ERROR.name());
            System.exit(-1);
        }
        return custodianOrgId;
    }

    private List<ShadowUser> getShadowUserFromDb() {
        List<Map<String, Object>> shadowUserList = getUnclaimedRowsFromShadowUserDb();
        List<ShadowUser> shadowUsers = mapper.convertValue(shadowUserList, new TypeReference<List<ShadowUser>>() {
        });
        return shadowUsers;
    }

    /**
     * this method will read rows from the shadow_user table who has status unclaimed
     *
     * @return list
     */
    private List<Map<String, Object>> getUnclaimedRowsFromShadowUserDb() {
        Map<String, Object> proertiesMap = new HashMap<>();
        proertiesMap.put(JsonKey.CLAIM_STATUS, ClaimStatus.UNCLAIMED.getValue());
        Response response = cassandraOperation.getRecordsByProperties(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, proertiesMap);
        List<Map<String, Object>> result = new ArrayList<>();
        if (!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            result = ((List) response.getResult().get(JsonKey.RESPONSE));
        }
        ProjectLogger.log("ShadowUserMigrationScheduler:getRowsFromBulkUserDb:got rows from Bulk user table is:".concat(result.size() + ""), LoggerEnum.INFO.name());
        return result;
    }

    private boolean isSame(ShadowUser shadowUser, Map<String, Object> esUserMap) {
        String orgId = getOrgId(shadowUser);
        if (!shadowUser.getName().equalsIgnoreCase((String) esUserMap.get(JsonKey.FIRST_NAME))) {
            return false;
        }
        if (StringUtils.isNotBlank(orgId) && !getOrganisationIds(esUserMap).contains(orgId)) {
            return false;
        }
        if (shadowUser.getUserStatus() != (int) (esUserMap.get(JsonKey.STATUS))) {
            return false;
        }
        if(StringUtils.isBlank(orgId)){
            return false;
        }
        return true;
    }


    private void updateUserInShadowDb(String userId, ShadowUser shadowUser, int claimStatus, List<String> matchingUserIds) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.CLAIM_STATUS, claimStatus);
        propertiesMap.put(JsonKey.PROCESS_ID,shadowUser.getProcessId());
        if (claimStatus == ClaimStatus.CLAIMED.getValue()) {
            propertiesMap.put(JsonKey.CLAIMED_ON, new Timestamp(System.currentTimeMillis()));
            propertiesMap.put(JsonKey.USER_ID, userId);
        }
        if (null != matchingUserIds && CollectionUtils.isNotEmpty(matchingUserIds)) {
            propertiesMap.put(JsonKey.USER_IDs, matchingUserIds);
        }
        Map<String, Object> compositeKeysMap = new HashMap<>();
        compositeKeysMap.put(JsonKey.CHANNEL, shadowUser.getChannel());
        compositeKeysMap.put(JsonKey.USER_EXT_ID, shadowUser.getUserExtId());
        Response response = cassandraOperation.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap, compositeKeysMap);
        ProjectLogger.log("ShadowUserProcessor:updateUserInShadowDb:update:with processId:"+shadowUser.getProcessId()+":and response is:"+response, LoggerEnum.INFO.name());
    }

    private String getOrgId(ShadowUser shadowUser) {
        if (StringUtils.isNotBlank(shadowUser.getOrgExtId())) {
            String orgId = extOrgIdMap.get(shadowUser.getChannel().concat(":").concat(shadowUser.getOrgExtId()));
            if (StringUtils.isNotBlank(orgId)) {
                return orgId;
            }
            Map<String, Object> request = new HashMap<>();
            Map<String, Object> filters = new HashMap<>();
            filters.put(JsonKey.EXTERNAL_ID, shadowUser.getOrgExtId().toLowerCase());
            filters.put(JsonKey.CHANNEL, shadowUser.getChannel());
            request.put(JsonKey.FILTERS, filters);
            ProjectLogger.log("ShadowUserProcessor:getOrgId: request map prepared to query elasticsearch for org id :"+filters+"with processId"+shadowUser.getProcessId(),LoggerEnum.INFO.name());
            SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
            Map<String, Object> response = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(elasticSearchService.search(searchDTO, ProjectUtil.EsType.organisation.getTypeName()));
            List<Map<String, Object>> orgData = ((List<Map<String, Object>>) response.get(JsonKey.CONTENT));
            if (CollectionUtils.isNotEmpty(orgData)) {
                Map<String, Object> orgMap = orgData.get(0);
                extOrgIdMap.put(shadowUser.getChannel().concat(":").concat(shadowUser.getOrgExtId()), (String) orgMap.get(JsonKey.ID));
                return (String) orgMap.get(JsonKey.ID);
            }
        }
        return StringUtils.EMPTY;
    }


    private List<String> getOrganisationIds(Map<String, Object> dbUser) {
        List<String> organisationsIds = new ArrayList<>();
        ((List<Map<String, Object>>) dbUser.get(JsonKey.ORGANISATIONS)).stream().forEach(organisation -> {
            organisationsIds.add((String) organisation.get(JsonKey.ORGANISATION_ID));
        });
        return organisationsIds;
    }


    private void syncUserToES(String userId) {
        Map<String, Object> fullUserDetails = Util.getUserDetails(userId, null);
        try {
            Future<Boolean> future = elasticSearchService.update(JsonKey.USER, userId, fullUserDetails);
            if ((boolean) ElasticSearchHelper.getResponseFromFuture(future)) {
                ProjectLogger.log("ShadowUserMigrationScheduler:updateUserStatus: data successfully updated to elastic search with userId:".concat(userId + ""), LoggerEnum.INFO.name());
            }
        } catch (Exception e) {
            e.printStackTrace();
            ProjectLogger.log("ShadowUserMigrationScheduler:syncUserToES: data failed to updates in elastic search with userId:".concat(userId + ""), LoggerEnum.ERROR.name());
        }
    }


    private void deleteUserOrganisations(Map<String, Object> esUserMap) {
        ((List<Map<String, Object>>) esUserMap.get(JsonKey.ORGANISATIONS)).stream().forEach(organisation -> {
            String id = (String) organisation.get(JsonKey.ID);
            deleteOrgFromUserOrg(id);
        });
    }

    private void deleteOrgFromUserOrg(String id) {
        Response response = cassandraOperation.deleteRecord(JsonKey.SUNBIRD, JsonKey.USER_ORG, id);
        ProjectLogger.log("ShadowUserProcessor:deleteOrgFromUserOrg:user org is deleted ".concat(response.getResult() + ""),LoggerEnum.INFO.name());
    }


    private void registerUserToOrg(String userId, String organisationId) {
        Map<String, Object> reqMap = new WeakHashMap<>();
        List<String> roles = new ArrayList<>();
        roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
        reqMap.put(JsonKey.ROLES, roles);
        String hashTagId = hashTagIdMap.get(organisationId);
        if (StringUtils.isBlank(hashTagId)) {
            hashTagId = Util.getHashTagIdFromOrgId(organisationId);
            hashTagIdMap.put(organisationId, hashTagId);
        }
        reqMap.put(JsonKey.HASHTAGID, hashTagId);
        reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        reqMap.put(JsonKey.USER_ID, userId);
        reqMap.put(JsonKey.ORGANISATION_ID, organisationId);
        reqMap.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
        reqMap.put(JsonKey.IS_DELETED, false);
        Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        try {
            Response response=cassandraOperation.insertRecord(usrOrgDb.getKeySpace(), usrOrgDb.getTableName(), reqMap);
            ProjectLogger.log("ShadowUserProcessor:registerUserToOrg:user status while registration with org is:"+response.getResult(),LoggerEnum.INFO.name());

        } catch (Exception e) {
            ProjectLogger.log("ShadowUserProcessor:registerUserToOrg:user is failed to register with org"+userId,LoggerEnum.ERROR.name());
        }
    }

    private void createUserExternalId(String userId, ShadowUser shadowUser) {
        Map<String, Object> externalId = new HashMap<>();
        externalId.put(JsonKey.ID_TYPE, shadowUser.getChannel().toLowerCase());
        externalId.put(JsonKey.PROVIDER, shadowUser.getChannel().toLowerCase());
        externalId.put(JsonKey.EXTERNAL_ID, shadowUser.getUserExtId().toLowerCase());
        externalId.put(JsonKey.ORIGINAL_EXTERNAL_ID, externalId.get(JsonKey.EXTERNAL_ID));
        externalId.put(JsonKey.ORIGINAL_PROVIDER, externalId.get(JsonKey.PROVIDER));
        externalId.put(JsonKey.ORIGINAL_ID_TYPE, externalId.get(JsonKey.ID_TYPE));
        externalId.put(JsonKey.USER_ID, userId);
        externalId.put(JsonKey.CREATED_BY, shadowUser.getAddedBy());
        externalId.put(JsonKey.CREATED_ON, new Timestamp(System.currentTimeMillis()));
        ProjectLogger.log("ShadowUserProcessor:createUserExternalId:map prepared for user externalid is "+externalId+"with processId"+shadowUser.getProcessId(),LoggerEnum.INFO.name());
        saveUserExternalId(externalId);
    }


    /**
     * this method will save user data in usr_external_identity table
     * @param externalId
     */
    private void saveUserExternalId(Map<String, Object> externalId) {
        Response response = cassandraOperation.insertRecord(JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, externalId);
        ProjectLogger.log("ShadowUserProcessor:createUserExternalId:response from cassandra ".concat(response.getResult() + ""),LoggerEnum.INFO.name());
    }


}

