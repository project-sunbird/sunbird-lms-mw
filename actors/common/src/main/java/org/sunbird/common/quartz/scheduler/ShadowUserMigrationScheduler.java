package org.sunbird.common.quartz.scheduler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.bean.ClaimStatus;
import org.sunbird.bean.MigrationUser;
import org.sunbird.bean.ShadowUser;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.ShadowUserProcessor;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.bulkupload.model.BulkMigrationUser;
import org.sunbird.learner.util.Util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShadowUserMigrationScheduler {

    private Util.DbInfo bulkUploadDbInfo = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private ObjectMapper mapper = new ObjectMapper();
    private ElasticSearchService elasticSearchService = EsClientFactory.getInstance(JsonKey.REST);
    private DecryptionService decryptionService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(null);
    ShadowUserProcessor object=new  ShadowUserProcessor();



//    @Override
//    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
//        getMigrationUser();
//    }


    /**
     * - fetch rows from bulk upload table whose status is less than 2
     * - update the bulk upload table row status to processing
     * - start processing single row
     * - fetch the single row data
     * - Get List of Migration(csv) user from the row
     * - process each Migration(csv) user
     * - update the bulk upload table row status to completed and add message to success
     * - if fails update the bulk upload row status to failed and add failureResult
     */
    public void processRecords() {
        List<Map<String, Object>> bulkDbRows = getRowsFromBulkUserDb();
        bulkDbRows.stream().forEach(row -> {
            BulkMigrationUser bulkMigrationUser = convertRowToObject(row);
            try {
                updateStatusInUserBulkTable(bulkMigrationUser.getId(), ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
                List<MigrationUser> migrationUserList = getMigrationUserAsList(bulkMigrationUser);
                migrationUserList.stream().forEach(singleMigrationUser -> {
                    processSingleMigUser(bulkMigrationUser.getCreatedBy(),bulkMigrationUser.getId(), singleMigrationUser);
                });
                updateMessageInBulkUserTable(bulkMigrationUser.getId(), JsonKey.SUCCESS_RESULT, JsonKey.SUCCESS);
            } catch (Exception e) {
                ProjectLogger.log("ShadowUserMigrationScheduler:processRecords:error occurred ".concat(e+""),LoggerEnum.ERROR.name());
                updateMessageInBulkUserTable(bulkMigrationUser.getId(), JsonKey.FAILURE_RESULT, e.getMessage());
            }
        });
        ProjectLogger.log("ShadowUserMigrationScheduler:processRecords:started stage3__________________________-",LoggerEnum.INFO.name());
        object.process();
    }

    private void processSingleMigUser(String createdBy,String processId, MigrationUser singleMigrationUser) {
            Map<String, Object> existingUserDetails = getShadowExistingUserDetails(singleMigrationUser.getChannel(), singleMigrationUser.getUserExternalId());
            if (MapUtils.isEmpty(existingUserDetails)) {
                insertShadowUserToDb(createdBy,processId, singleMigrationUser);
            } else {
                ShadowUser shadowUser = mapper.convertValue(existingUserDetails, ShadowUser.class);
                updateUser(singleMigrationUser, shadowUser);
            }
    }


    /**
     * this method will read rows from the bulk_upload_process table who has status less than 2
     * @return list
     */
    private List<Map<String, Object>> getRowsFromBulkUserDb() {
        Response response = cassandraOperation.getRecordWithCondition(bulkUploadDbInfo.getKeySpace(), bulkUploadDbInfo.getTableName(), JsonKey.STATUS, ProjectUtil.BulkProcessStatus.INTERRUPT.getValue());
        List<Map<String, Object>> result = new ArrayList<>();
        if (!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            result = ((List) response.getResult().get(JsonKey.RESPONSE));
        }
        ProjectLogger.log("ShadowUserMigrationScheduler:getRowsFromBulkUserDb:got rows from Bulk user table is:".concat(result.size()+""),LoggerEnum.INFO.name());
        return result;
    }


    private BulkMigrationUser convertRowToObject(Map<String, Object> row) {
        BulkMigrationUser bulkMigrationUser = null;
        try {
            bulkMigrationUser = mapper.convertValue(row, BulkMigrationUser.class);
        } catch (Exception e) {
            e.printStackTrace();
            ProjectLogger.log("ShadowUserMigrationScheduler:convertMapToMigrationObject:error occurred while converting map to pojo".concat(e.getMessage() + ""), LoggerEnum.ERROR.name());
        }
        return bulkMigrationUser;
    }

    private List<MigrationUser> getMigrationUserAsList(BulkMigrationUser bulkMigrationUser) {
        List<MigrationUser> migrationUserList = new ArrayList<>();
        try {
            String decryptedData = decryptionService.decryptData(bulkMigrationUser.getData());
            migrationUserList = mapper.readValue(decryptedData, new TypeReference<List<MigrationUser>>() {
            });
        } catch (Exception e) {
            e.printStackTrace();
            ProjectLogger.log("ShadowUserMigrationScheduler:getMigrationUserAsList:error occurred while converting map to pojo".concat(e.getMessage() + ""), LoggerEnum.ERROR.name());
        }
        return migrationUserList;
    }

    private Map<String, Object> getShadowExistingUserDetails(String channel, String userExtId) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.CHANNEL, channel);
        propertiesMap.put("userExtId", userExtId);
        Map<String, Object> result = new HashMap<>();
        Response response = cassandraOperation.getRecordsByProperties(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap);
        if (!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            result = ((Map) ((List) response.getResult().get(JsonKey.RESPONSE)).get(0));
        }
        return result;
    }

    private void insertShadowUserToDb(String createdBy,String processId, MigrationUser migrationUser) {
        List<String> userRoles = new ArrayList<>();
        userRoles.add(ProjectUtil.UserRole.PUBLIC.getValue());
        ShadowUser shadowUser = new ShadowUser.ShadowUserBuilder()
                .setUserExtId(migrationUser.getUserExternalId())
                .setOrgExtId(migrationUser.getOrgExternalId())
                .setEmail(migrationUser.getEmail())
                .setPhone(migrationUser.getPhone())
                .setUserId(null)
                .setAddedBy(createdBy)
                .setChannel(migrationUser.getChannel())
                .setName(migrationUser.getName())
                .setProcessId(processId)
                .setClaimedOn(null)
                .setClaimStatus(ClaimStatus.UNCLAIMED.getValue())
                .setRoles(userRoles)
                .setUserStatus(getInputStatus(migrationUser.getInputStatus()))
                .setUpdatedOn(null)
                .build();
        Map<String, Object> dbMap = mapper.convertValue(shadowUser, Map.class);
        dbMap.put(JsonKey.CREATED_ON, new Timestamp(System.currentTimeMillis()));
        if(!isOrgExternalIdValid(migrationUser)) {
            dbMap.put(JsonKey.CLAIM_STATUS,ClaimStatus.REJECTED.getValue());
        }
            Response response = cassandraOperation.insertRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, dbMap);
            ProjectLogger.log("ShadowUserMigrationScheduler:insertShadowUser: record status in cassandra ".concat(response.getResult() + ""), LoggerEnum.INFO.name());

    }

    private int getInputStatus(String inputStatus) {
        if (inputStatus.equalsIgnoreCase(JsonKey.ACTIVE)) {
            return ProjectUtil.Status.ACTIVE.getValue();
        }
        return ProjectUtil.Status.INACTIVE.getValue();
    }

    private void updateUser(MigrationUser migrationUser, ShadowUser shadowUser) {
        updateUserInShadowDb(migrationUser,shadowUser);
    }

    private void updateUserInShadowDb(MigrationUser migrationUser, ShadowUser shadowUser) {
        if(!isSame(shadowUser,migrationUser)){
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(JsonKey.EMAIL, migrationUser.getEmail());
            propertiesMap.put(JsonKey.PHONE, migrationUser.getPhone());
            propertiesMap.put(JsonKey.NAME, migrationUser.getName());
            propertiesMap.put(JsonKey.ORG_EXT_ID, migrationUser.getOrgExternalId());
            propertiesMap.put(JsonKey.UPDATED_ON, new Timestamp(System.currentTimeMillis()));
            propertiesMap.put(JsonKey.USER_STATUS,getInputStatus(migrationUser.getInputStatus()));
            if(!isOrgExternalIdValid(migrationUser)) {
                propertiesMap.put(JsonKey.CLAIM_STATUS, ClaimStatus.REJECTED.getValue());
            }
            Map<String,Object>compositeKeysMap=new HashMap<>();
            compositeKeysMap.put(JsonKey.CHANNEL, migrationUser.getChannel());
            compositeKeysMap.put(JsonKey.USER_EXT_ID,migrationUser.getUserExternalId());
            Response response = cassandraOperation.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap,compositeKeysMap);
            ProjectLogger.log("ShadowUserMigrationScheduler:updateUserInShadowDb: record status in cassandra ".concat(response.getResult() + ""), LoggerEnum.INFO.name());
            object.processClaimedUser(getUpdatedShadowUser(compositeKeysMap));
        }
    }
    private ShadowUser getUpdatedShadowUser( Map<String,Object>compositeKeysMap){
        Response response=cassandraOperation.getRecordsByCompositeKey(JsonKey.SUNBIRD, JsonKey.SHADOW_USER,compositeKeysMap);
        Map<String,Object>resultmap=((List<Map<String, Object>>)response.getResult().get(JsonKey.RESPONSE)).get(0);
        ShadowUser shadowUser=mapper.convertValue(resultmap,ShadowUser.class);
        return shadowUser;
    }


    private void updateStatusInUserBulkTable(String processId, int statusVal) {
        try {
            ProjectLogger.log("ShadowUserMigrationScheduler:updateStatusInUserBulkTable: got status to change in bulk upload table".concat(statusVal + ""), LoggerEnum.INFO.name());
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(JsonKey.ID, processId);
            propertiesMap.put(JsonKey.STATUS, statusVal);
            updateBulkUserTable(propertiesMap);
        } catch (Exception e) {
            ProjectLogger.log("ShadowUserMigrationScheduler:updateStatusInUserBulkTable: status update failed".concat(e + ""), LoggerEnum.ERROR.name());
        }
    }

    private void updateBulkUserTable(Map<String, Object> propertiesMap) {
        Response response = cassandraOperation.updateRecord(bulkUploadDbInfo.getKeySpace(), bulkUploadDbInfo.getTableName(), propertiesMap);
        ProjectLogger.log("ShadowUserMigrationScheduler:updateBulkUserTable: status update result".concat(response.getResult() + ""), LoggerEnum.INFO.name());
    }

    private void updateMessageInBulkUserTable(String processId, String key, String value) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.ID, processId);
        propertiesMap.put(key, value);
        if (value.equalsIgnoreCase(JsonKey.SUCCESS)) {
            propertiesMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
            propertiesMap.put(JsonKey.FAILURE_RESULT, "");
        } else {
            propertiesMap.put(JsonKey.SUCCESS_RESULT, "");
            propertiesMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.FAILED.getValue());
        }
        updateBulkUserTable(propertiesMap);
    }


    /**
     *
     * @param shadowUser
     * @param migrationUser
     * @return
     */
    private boolean isSame(ShadowUser shadowUser,MigrationUser migrationUser){
        if(!shadowUser.getName().equalsIgnoreCase(migrationUser.getName())){
        return false;
        }
        if(StringUtils.isNotBlank(migrationUser.getEmail()) && !shadowUser.getEmail().equalsIgnoreCase(migrationUser.getEmail())){
            return false;
        }
        if(StringUtils.isNotBlank(migrationUser.getOrgExternalId()) && !shadowUser.getOrgExtId().equalsIgnoreCase(migrationUser.getOrgExternalId())){
            return false;
        }
        if(StringUtils.isNotBlank(migrationUser.getPhone()) && !shadowUser.getPhone().equalsIgnoreCase(migrationUser.getPhone()))
        {
            return false;
        }
        if((getInputStatus(migrationUser.getInputStatus())!=shadowUser.getUserStatus())){
            return false;
        }

        if(!migrationUser.getName().equalsIgnoreCase(shadowUser.getName())){
            return false;
        }
        return true;
    }

    private boolean isOrgExternalIdValid(MigrationUser migrationUser) {
        if (StringUtils.isBlank(migrationUser.getOrgExternalId())) {
        return true;
        }
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(JsonKey.EXTERNAL_ID, migrationUser.getOrgExternalId());
        filters.put(JsonKey.CHANNEL, migrationUser.getChannel());
        request.put(JsonKey.FILTERS, filters);
        SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
        Map<String, Object> response = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(elasticSearchService.search(searchDTO, ProjectUtil.EsType.organisation.getTypeName()));
        return CollectionUtils.isNotEmpty((List<Map<String, Object>>) response.get(JsonKey.CONTENT));
        }
}