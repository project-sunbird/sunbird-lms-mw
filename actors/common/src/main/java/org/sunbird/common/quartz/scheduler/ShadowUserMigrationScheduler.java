package org.sunbird.common.quartz.scheduler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.sunbird.bean.ClaimStatus;
import org.sunbird.bean.MigrationUser;
import org.sunbird.bean.ShadowUser;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.bulkupload.model.BulkMigrationUser;
import org.sunbird.learner.util.Util;
import scala.concurrent.Future;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShadowUserMigrationScheduler {

    private Util.DbInfo bulkUploadDbInfo = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private ObjectMapper mapper = new ObjectMapper();
    private ElasticSearchService elasticSearchService = EsClientFactory.getInstance(JsonKey.REST);
    private DecryptionService decryptionService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(null);


//    @Override
//    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
//        getMigrationUser();
//    }


    /**
     *  - fetch rows from bulk upload table whose status is less than 2
     *  - update the bulk upload table row status to processing
     *  - start processing single row
     *  - fetch the single row data
     *  - Get List of Migration(csv) user from the row
     *  - process each Migration(csv) user
     *  - update the bulk upload table row status to completed
     */
    public void processRecords() {
        List<Map<String, Object>> bulkDbRows = getRowsFromBulkUserDb();
        bulkDbRows.stream().forEach(row -> {
            BulkMigrationUser bulkMigrationUser = convertRowToObject(row);
            updateStatusInUserBulkTable(bulkMigrationUser.getId(),ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
            List<MigrationUser> migrationUserList = getMigrationUserAsList(bulkMigrationUser);
            migrationUserList.stream().forEach(singleMigrationUser -> {
                processSingleMigUser(bulkMigrationUser.getId(), singleMigrationUser);
            });
            updateStatusInUserBulkTable(bulkMigrationUser.getId(),ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
        });
    }
    private void processSingleMigUser(String processId, MigrationUser singleMigrationUser) {
        Map<String, Object> existingUserDetails = getShadowExistingUserDetails(singleMigrationUser.getChannel(), singleMigrationUser.getUserExternalId());
        if (MapUtils.isEmpty(existingUserDetails)) {
            insertShadowUserToDb(processId, singleMigrationUser);
        } else {
            ShadowUser shadowUser = mapper.convertValue(existingUserDetails, ShadowUser.class);
            updateUserInShadowDb(singleMigrationUser, shadowUser);
        }
    }


    private List<Map<String, Object>> getRowsFromBulkUserDb() {
        Response response = cassandraOperation.getRecordWithCondition(bulkUploadDbInfo.getKeySpace(), bulkUploadDbInfo.getTableName(), JsonKey.STATUS, ProjectUtil.BulkProcessStatus.INTERRUPT.getValue());
        List<Map<String, Object>> result = null;
        if (!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            result = ((List) response.getResult().get(JsonKey.RESPONSE));
        }
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

    private void insertShadowUserToDb(String processId, MigrationUser migrationUser) {
        String id = ProjectUtil.generateUniqueId();
        List<String> userRoles = new ArrayList<>();
        userRoles.add(ProjectUtil.UserRole.PUBLIC.getValue());
        ShadowUser shadowUser = new ShadowUser.ShadowUserBuilder()
                .setUserExtId(migrationUser.getUserExternalId())
                .setOrgExtId(migrationUser.getOrgExternalId())
                .setEmail(migrationUser.getEmail())
                .setPhone(migrationUser.getPhone())
                .setUserId(null)
                .setChannel(migrationUser.getChannel())
                .setName(migrationUser.getName())
                .setProcessId(processId)
                .setCreatedOn(new Timestamp(System.currentTimeMillis()))
                .setClaimedOn(null)
                .setClaimStatus(ClaimStatus.UNCLAIMED.getValue())
                .setId(id)
                .setRoles(userRoles)
                .setUserStatus(getInputStatus(migrationUser.getInputStatus()))
                .setUpdatedOn(null)
                .build();
        Map<String, Object> dbMap = mapper.convertValue(shadowUser, Map.class);
        Response response = cassandraOperation.insertRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, dbMap);
        ProjectLogger.log("ShadowUserMigrationScheduler:insertShadowUser: record status in cassandra ".concat(response.getResult() + ""), LoggerEnum.INFO.name());
    }

    private int getInputStatus(String inputStatus) {
        if (inputStatus.equalsIgnoreCase(JsonKey.ACTIVE)) {
            return ProjectUtil.Status.ACTIVE.getValue();
        }
        return ProjectUtil.Status.INACTIVE.getValue();
    }

    private void updateUserInShadowDb(MigrationUser migrationUser, ShadowUser shadowUser) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.EMAIL, migrationUser.getEmail());
        propertiesMap.put(JsonKey.PHONE, migrationUser.getPhone());
        propertiesMap.put(JsonKey.NAME, migrationUser.getName());
        propertiesMap.put(JsonKey.ORG_EXTERNAL_ID, migrationUser.getOrgExternalId());
        propertiesMap.put(JsonKey.UPDATED_ON, new Timestamp(System.currentTimeMillis()));
        propertiesMap.put(JsonKey.CHANNEL, migrationUser.getChannel());
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), propertiesMap);
        updateUserInUserDb(migrationUser.getInputStatus(), shadowUser);
    }

    private void updateUserInUserDb(String migrationUserStatus, ShadowUser shadowUser) {
        if (shadowUser.getClaimStatus() == ClaimStatus.CLAIMED.getValue()) {
            if (migrationUserStatus.equalsIgnoreCase(JsonKey.ACTIVE)) {
                updateUser(true, shadowUser.getUserId());
            } else {
                updateUser(false, shadowUser.getUserId());
            }
        }
    }

    private void updateUser(boolean isActive, String userId) {
        Map<String, Object> propertiesMap = new HashMap<>();
        try {
            propertiesMap.put(JsonKey.ID, userId);
            if (isActive) {
                propertiesMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
            } else {
                propertiesMap.put(JsonKey.STATUS, ProjectUtil.Status.INACTIVE.getValue());
            }
            propertiesMap.put(JsonKey.IS_DELETED, isActive);
            cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), propertiesMap);
        } catch (Exception e) {
            e.printStackTrace();
            ProjectLogger.log("ShadowUserMigrationScheduler:updateUserStatus: data failed to updates in cassandra  with userId:".concat(userId + ""), LoggerEnum.ERROR.name());
        }
        syncUserToES(propertiesMap);
    }

    private void syncUserToES(Map<String, Object> propertiesMap) {
        String userId = (String) propertiesMap.get(JsonKey.ID);
        try {
            Future<Boolean> future = elasticSearchService.update(JsonKey.USER, userId, propertiesMap);
            if ((boolean) ElasticSearchHelper.getResponseFromFuture(future)) {
                ProjectLogger.log("ShadowUserMigrationScheduler:updateUserStatus: data successfully updated to elastic search with userId:".concat(userId + ""), LoggerEnum.INFO.name());
            }
        } catch (Exception e) {
            e.printStackTrace();
            ProjectLogger.log("ShadowUserMigrationScheduler:syncUserToES: data failed to updates in elastic search with userId:".concat(userId + ""), LoggerEnum.ERROR.name());
        }
    }

    private void updateStatusInUserBulkTable(String processId, int statusVal) {
        try {
            ProjectLogger.log("ShadowUserMigrationScheduler:updateStatusInUserBulkTable: got status to change in bulk upload table".concat(statusVal + ""), LoggerEnum.INFO.name());
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(JsonKey.ID, processId);
            propertiesMap.put(JsonKey.STATUS, statusVal);
            Response response = cassandraOperation.updateRecord(bulkUploadDbInfo.getKeySpace(), bulkUploadDbInfo.getTableName(), propertiesMap);
            ProjectLogger.log("ShadowUserMigrationScheduler:updateStatusInUserBulkTable: status update result".concat(response.getResult() + ""), LoggerEnum.INFO.name());
        } catch (Exception e) {
            ProjectLogger.log("ShadowUserMigrationScheduler:updateStatusInUserBulkTable: status update failed".concat(e.getMessage() + ""), LoggerEnum.ERROR.name());
        }
    }
}
