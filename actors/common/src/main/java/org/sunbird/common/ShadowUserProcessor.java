package org.sunbird.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
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
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShadowUserProcessor {
    private Util.DbInfo bulkUploadDbInfo = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private ObjectMapper mapper = new ObjectMapper();
    private static SystemSetting systemSetting;
    private EncryptionService encryptionService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(null);
    private ElasticSearchService elasticSearchService = EsClientFactory.getInstance(JsonKey.REST);
    private DecryptionService decryptionService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(null);

    public void process() {
        List<ShadowUser> shadowUserList = getShadowUserFromDb();
        shadowUserList.stream().forEach(singleShadowUser -> {
            processSingleShadowUser(singleShadowUser);
        });
    }

    private void processSingleShadowUser(ShadowUser shadowUser) {
        getUserFromEs(shadowUser);


    }


    private List<Map<String, Object>> getUserFromEs(ShadowUser shadowUser) {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        Map<String, Object> or = new HashMap<>();
        or.put(JsonKey.EMAIL, StringUtils.isNotBlank(shadowUser.getEmail()) ? getEncryptedValue(shadowUser.getEmail()) : "");
        or.put(JsonKey.PHONE, StringUtils.isNotBlank(shadowUser.getPhone()) ? getEncryptedValue(shadowUser.getPhone()) : "");
        filters.put(JsonKey.ES_OR_OPERATION, or);
        filters.put(JsonKey.ROOT_ORG_ID, getCustodianOrgId());
        request.put(JsonKey.FILTERS, filters);
        SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
        Map<String, Object> response = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(elasticSearchService.search(searchDTO, JsonKey.USER));
        return (List<Map<String, Object>>) response.get(JsonKey.CONTENT);
    }

    private void updateUser(ShadowUser shadowUser) {
        List<Map<String, Object>> dbUser = (List<Map<String, Object>>) getUserFromEs(shadowUser);
        if (CollectionUtils.isNotEmpty(dbUser)) {
            if (dbUser.size() == 1) {
                Map<String, Object> userMap = dbUser.get(0);
                if(!isSame(shadowUser,userMap)){

                }
            } else {
                updateUserInShadowDb(shadowUser, ClaimStatus.REJECTED.getValue());
            }
        }

    }


    private void updateUserInUserTable(ShadowUser shadowUser){






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
     * @return
     */
    private String getCustodianOrgId() {
        String custodianOrgId = null;
        Response response = cassandraOperation.getRecordById(JsonKey.SUNBIRD, JsonKey.SYSTEM_SETTINGS_DB, JsonKey.CUSTODIAN_ORG_ID);
        List<Map<String, Object>> result = new ArrayList<>();
        if (!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            result = ((List) response.getResult().get(JsonKey.RESPONSE));
            Map<String, Object> resultMap = result.get(0);
            custodianOrgId = (String) resultMap.get(JsonKey.VALUE);
        }

        if (StringUtils.isBlank(custodianOrgId)) {
            ProjectLogger.log("ShadowUserProcessor:getCustodianOrgId:No CUSTODIAN ORD ID FOUND PLEASE HAVE THAT IN YOUR ENVIRONMENT", LoggerEnum.ERROR.name());
            System.exit(-1);
        }
        return custodianOrgId;
    }

    private List<ShadowUser> getShadowUserFromDb() {
        List<Map<String, Object>> shadowUserList = getRowsFromShadowUserDb();
        List<ShadowUser> shadowUsers = mapper.convertValue(shadowUserList, new TypeReference<List<ShadowUser>>() {
        });
        return shadowUsers;
    }

    /**
     * this method will read rows from the shadow_user table who has status unclaimed
     *
     * @return list
     */
    private List<Map<String, Object>> getRowsFromShadowUserDb() {
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
        String orgId=getOrgId(shadowUser);
        if (!shadowUser.getName().equalsIgnoreCase((String) esUserMap.get(JsonKey.FIRST_NAME))) {
            return false;
        }
        if(StringUtils.isNotBlank(orgId) && !getOrganisationIds(esUserMap).contains(orgId)) {
            return false;
        }
        if (shadowUser.getUserStatus()!=(int)(esUserMap.get(JsonKey.STATUS))) {
            return false;
        }
        return true;
    }


    private void updateUserInShadowDb(ShadowUser shadowUser, int claimStatus) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.UPDATED_ON, new Timestamp(System.currentTimeMillis()));
        propertiesMap.put(JsonKey.CLAIM_STATUS, claimStatus);
        Map<String, Object> compositeKeysMap = new HashMap<>();
        compositeKeysMap.put(JsonKey.CHANNEL, shadowUser.getChannel());
        compositeKeysMap.put(JsonKey.USER_EXT_ID, shadowUser.getUserExtId());
        Response response = cassandraOperation.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap, compositeKeysMap);
        ProjectLogger.log("ShadowUserProcessor:updateUserInShadowDb:update ".concat(response.getResult() + ""), LoggerEnum.INFO.name());
    }

    private String getOrgId(ShadowUser shadowUser) {
        if (StringUtils.isNotBlank(shadowUser.getOrgExtId())){
            Map<String, Object> request = new HashMap<>();
            Map<String, Object> filters = new HashMap<>();
            filters.put(JsonKey.EXTERNAL_ID, shadowUser.getOrgExtId());
            filters.put(JsonKey.CHANNEL, shadowUser.getChannel());
            request.put(JsonKey.FILTERS, filters);
            SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
            Map<String, Object> response = (Map<String, Object>) elasticSearchService.search(searchDTO, ProjectUtil.EsType.organisation.getTypeName());
            List<Map<String, Object>> orgData = ((List<Map<String, Object>>) response.get(JsonKey.CONTENT));
            if (CollectionUtils.isNotEmpty(orgData)) {
                Map<String, Object> orgMap = orgData.get(0);
                return (String) orgMap.get(JsonKey.ID);
            }
        }
        return StringUtils.EMPTY;
    }


    private List<String> getOrganisationIds(Map<String, Object> dbUser){
        List<String>organisationsIds= new ArrayList<>();
        ((List<Map<String,Object>>)dbUser.get(JsonKey.ORGANISATIONS)).stream().forEach(organisation->{
            organisationsIds.add((String) organisation.get(JsonKey.ORGANISATION_ID));
        });
        return organisationsIds;
    }
}



