package org.sunbird.learner.actors.bulkupload;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.BackgroundRequestRouter;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.BulkProcessStatus;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.Slug;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.AuditOperation;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.SocialMediaType;
import org.sunbird.learner.util.TelemetryUtil;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.notification.sms.provider.ISmsProvider;
import org.sunbird.notification.utils.SMSFactory;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;

/**
 * This actor will handle bulk upload operation .
 *
 * @author Amit Kumar
 */
public class BulkUploadBackGroundJobActor extends BaseActor {

    private String processId = "";
    private final Util.DbInfo bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    private final EncryptionService encryptionService =
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                    .getEncryptionServiceInstance(null);
    private final PropertiesCache propertiesCache = PropertiesCache.getInstance();
    private final List<String> locnIdList = new ArrayList<>();
    private final CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private final SSOManager ssoManager = SSOServiceFactory.getInstance();
    private static final String SUNBIRD_WEB_URL = "sunbird_web_url";
    private static final String SUNBIRD_APP_URL = "sunbird_app_url";

    public static void init() {
        BackgroundRequestRouter.registerActor(BulkUploadBackGroundJobActor.class,
                Arrays.asList(ActorOperations.PROCESS_BULK_UPLOAD.getValue()));
    }

    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, JsonKey.USER);
        ExecutionContext.setRequestId(request.getRequestId());
        if (request.getOperation()
                .equalsIgnoreCase(ActorOperations.PROCESS_BULK_UPLOAD.getValue())) {
            process(request);
        } else {
            onReceiveUnsupportedOperation(request.getOperation());
        }
    }

    private void process(Request actorMessage) {
        ObjectMapper mapper = new ObjectMapper();
        processId = (String) actorMessage.get(JsonKey.PROCESS_ID);
        Map<String, Object> dataMap = getBulkData(processId);
        int status = (int) dataMap.get(JsonKey.STATUS);
        if (!(status == (ProjectUtil.BulkProcessStatus.COMPLETED.getValue())
                || status == (ProjectUtil.BulkProcessStatus.INTERRUPT.getValue()))) {
            TypeReference<List<Map<String, Object>>> mapType =
                    new TypeReference<List<Map<String, Object>>>() {};
            List<Map<String, Object>> jsonList = null;
            try {
                jsonList = mapper.readValue((String) dataMap.get(JsonKey.DATA), mapType);
            } catch (IOException e) {
                ProjectLogger.log(
                        "Exception occurred while converting json String to List in BulkUploadBackGroundJobActor : ",
                        e);
            }
            if (((String) dataMap.get(JsonKey.OBJECT_TYPE)).equalsIgnoreCase(JsonKey.USER)) {
                processUserInfo(jsonList, processId, (String) dataMap.get(JsonKey.UPLOADED_BY));
            } else if (((String) dataMap.get(JsonKey.OBJECT_TYPE))
                    .equalsIgnoreCase(JsonKey.ORGANISATION)) {
                CopyOnWriteArrayList<Map<String, Object>> orgList =
                        new CopyOnWriteArrayList<>(jsonList);
                processOrgInfo(orgList, dataMap);
            } else if (((String) dataMap.get(JsonKey.OBJECT_TYPE))
                    .equalsIgnoreCase(JsonKey.BATCH)) {
                processBatchEnrollment(jsonList, processId);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void processBatchEnrollment(List<Map<String, Object>> jsonList, String processId) {
        // update status from NEW to INProgress
        updateStatusForProcessing(processId);
        Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
        List<Map<String, Object>> successResultList = new ArrayList<>();
        List<Map<String, Object>> failureResultList = new ArrayList<>();

        Map<String, Object> successListMap = null;
        Map<String, Object> failureListMap = null;
        for (Map<String, Object> batchMap : jsonList) {
            successListMap = new HashMap<>();
            failureListMap = new HashMap<>();
            Map<String, Object> tempFailList = new HashMap<>();
            Map<String, Object> tempSuccessList = new HashMap<>();

            String batchId = (String) batchMap.get(JsonKey.BATCH_ID);
            Response courseBatchResult = cassandraOperation.getRecordById(dbInfo.getKeySpace(),
                    dbInfo.getTableName(), batchId);
            String msg = validateBatchInfo(courseBatchResult);
            if (msg.equals(JsonKey.SUCCESS)) {
                List<Map<String, Object>> courseList =
                        (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
                List<String> userList = new ArrayList<>(
                        Arrays.asList((((String) batchMap.get(JsonKey.USER_IDs)).split(","))));
                validateBatchUserListAndAdd(courseList.get(0), batchId, userList, tempFailList,
                        tempSuccessList);
                failureListMap.put(batchId, tempFailList.get(JsonKey.FAILURE_RESULT));
                successListMap.put(batchId, tempSuccessList.get(JsonKey.SUCCESS_RESULT));
            } else {
                batchMap.put(JsonKey.ERROR_MSG, msg);
                failureResultList.add(batchMap);
            }
            if (!successListMap.isEmpty()) {
                successResultList.add(successListMap);
            }
            if (!failureListMap.isEmpty()) {
                failureResultList.add(failureListMap);
            }
        }

        // Insert record to BulkDb table
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.ID, processId);
        map.put(JsonKey.SUCCESS_RESULT, convertMapToJsonString(successResultList));
        map.put(JsonKey.FAILURE_RESULT, convertMapToJsonString(failureResultList));
        map.put(JsonKey.PROCESS_END_TIME, ProjectUtil.getFormattedDate());
        map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
        try {
            cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
        } catch (Exception e) {
            ProjectLogger.log(
                    "Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ",
                    e);
        }
    }

    @SuppressWarnings("unchecked")
    private void validateBatchUserListAndAdd(Map<String, Object> courseBatchObject, String batchId,
            List<String> userIds, Map<String, Object> failList, Map<String, Object> successList) {
        Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
        Util.DbInfo userOrgdbInfo = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        List<Map<String, Object>> failedUserList = new ArrayList<>();
        List<Map<String, Object>> passedUserList = new ArrayList<>();

        Map<String, Object> map = null;
        List<String> createdFor = (List<String>) courseBatchObject.get(JsonKey.COURSE_CREATED_FOR);
        Map<String, Boolean> participants =
                (Map<String, Boolean>) courseBatchObject.get(JsonKey.PARTICIPANT);
        if (participants == null) {
            participants = new HashMap<>();
        }
        // check whether can update user or not
        for (String userId : userIds) {
            if (!(participants.containsKey(userId))) {
                Response dbResponse =
                        cassandraOperation.getRecordsByProperty(userOrgdbInfo.getKeySpace(),
                                userOrgdbInfo.getTableName(), JsonKey.USER_ID, userId);
                List<Map<String, Object>> userOrgResult =
                        (List<Map<String, Object>>) dbResponse.get(JsonKey.RESPONSE);

                if (userOrgResult.isEmpty()) {
                    map = new HashMap<>();
                    map.put(userId, ResponseCode.userNotAssociatedToOrg.getErrorMessage());
                    failedUserList.add(map);
                    continue;
                }
                boolean flag = false;
                for (int i = 0; i < userOrgResult.size() && !flag; i++) {
                    Map<String, Object> usrOrgDetail = userOrgResult.get(i);
                    if (createdFor.contains(usrOrgDetail.get(JsonKey.ORGANISATION_ID))) {
                        participants.put(userId,
                                addUserCourses(batchId,
                                        (String) courseBatchObject.get(JsonKey.COURSE_ID), userId,
                                        (Map<String, String>) (courseBatchObject
                                                .get(JsonKey.COURSE_ADDITIONAL_INFO))));
                        flag = true;
                    }
                }
                if (flag) {
                    map = new HashMap<>();
                    map.put(userId, JsonKey.SUCCESS);
                    passedUserList.add(map);
                } else {
                    map = new HashMap<>();
                    map.put(userId, ResponseCode.userNotAssociatedToOrg.getErrorMessage());
                    failedUserList.add(map);
                }

            } else {
                map = new HashMap<>();
                map.put(userId, JsonKey.SUCCESS);
                passedUserList.add(map);
            }
        }
        courseBatchObject.put(JsonKey.PARTICIPANT, participants);
        cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(),
                courseBatchObject);
        successList.put(JsonKey.SUCCESS_RESULT, passedUserList);
        failList.put(JsonKey.FAILURE_RESULT, failedUserList);
        // process Audit Log
        processAuditLog(courseBatchObject, ActorOperations.UPDATE_BATCH.getValue(), "",
                JsonKey.BATCH);
        ProjectLogger.log("method call going to satrt for ES--.....");
        Request request = new Request();
        request.setOperation(ActorOperations.UPDATE_COURSE_BATCH_ES.getValue());
        request.getRequest().put(JsonKey.BATCH, courseBatchObject);
        ProjectLogger.log("making a call to save Course Batch data to ES");
        try {
            tellToAnother(request);
        } catch (Exception ex) {
            ProjectLogger.log(
                    "Exception Occured during saving Course Batch to Es while updating Course Batch : ",
                    ex);
        }
    }

    private Boolean addUserCourses(String batchId, String courseId, String userId,
            Map<String, String> additionalCourseInfo) {

        Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
        Util.DbInfo coursePublishdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
        Response response = cassandraOperation.getRecordById(coursePublishdbInfo.getKeySpace(),
                coursePublishdbInfo.getTableName(), courseId);
        List<Map<String, Object>> resultList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if (!ProjectUtil.CourseMgmtStatus.LIVE.getValue()
                .equalsIgnoreCase(additionalCourseInfo.get(JsonKey.STATUS))) {
            if (resultList.isEmpty()) {
                return false;
            }
            Map<String, Object> publishStatus = resultList.get(0);

            if (Status.ACTIVE.getValue() != (Integer) publishStatus.get(JsonKey.STATUS)) {
                return false;
            }
        }
        Boolean flag = false;
        Timestamp ts = new Timestamp(new Date().getTime());
        Map<String, Object> userCourses = new HashMap<>();
        userCourses.put(JsonKey.USER_ID, userId);
        userCourses.put(JsonKey.BATCH_ID, batchId);
        userCourses.put(JsonKey.COURSE_ID, courseId);
        userCourses.put(JsonKey.ID, generatePrimaryKey(userCourses));
        userCourses.put(JsonKey.CONTENT_ID, courseId);
        userCourses.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
        userCourses.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
        userCourses.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
        userCourses.put(JsonKey.DATE_TIME, ts);
        userCourses.put(JsonKey.COURSE_PROGRESS, 0);
        userCourses.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.COURSE_LOGO_URL));
        userCourses.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.COURSE_NAME));
        userCourses.put(JsonKey.DESCRIPTION, additionalCourseInfo.get(JsonKey.DESCRIPTION));
        if (!ProjectUtil.isStringNullOREmpty(additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT))) {
            userCourses.put(JsonKey.LEAF_NODE_COUNT,
                    Integer.parseInt("" + additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT)));
        }
        userCourses.put(JsonKey.TOC_URL, additionalCourseInfo.get(JsonKey.TOC_URL));
        try {
            cassandraOperation.insertRecord(courseEnrollmentdbInfo.getKeySpace(),
                    courseEnrollmentdbInfo.getTableName(), userCourses);
            // TODO: for some reason, ES indexing is failing with Timestamp value. need to
            // check and
            // correct it.
            userCourses.put(JsonKey.DATE_TIME, ProjectUtil.formatDate(ts));
            insertUserCoursesToES(userCourses);
            flag = true;
        } catch (Exception ex) {
            ProjectLogger.log("INSERT RECORD TO USER COURSES EXCEPTION ", ex);
            flag = false;
        }
        return flag;
    }

    private void insertUserCoursesToES(Map<String, Object> courseMap) {
        Request request = new Request();
        request.setOperation(ActorOperations.INSERT_USR_COURSES_INFO_ELASTIC.getValue());
        request.getRequest().put(JsonKey.USER_COURSES, courseMap);
        try {
            tellToAnother(request);
        } catch (Exception ex) {
            ProjectLogger.log("Exception Occured during saving user count to Es : ", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private String validateBatchInfo(Response courseBatchResult) {
        // check batch exist in db or not
        List<Map<String, Object>> courseList =
                (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
        if ((courseList.isEmpty())) {
            return ResponseCode.invalidCourseBatchId.getErrorMessage();
        }
        Map<String, Object> courseBatchObject = courseList.get(0);
        // check whether coursebbatch type is invite only or not ...
        if (ProjectUtil.isNull(courseBatchObject.get(JsonKey.ENROLLMENT_TYPE))
                || !((String) courseBatchObject.get(JsonKey.ENROLLMENT_TYPE))
                        .equalsIgnoreCase(JsonKey.INVITE_ONLY)) {
            return ResponseCode.enrollmentTypeValidation.getErrorMessage();
        }
        if (ProjectUtil.isNull(courseBatchObject.get(JsonKey.COURSE_CREATED_FOR))
                || ((List) courseBatchObject.get(JsonKey.COURSE_CREATED_FOR)).isEmpty()) {
            return ResponseCode.courseCreatedForIsNull.getErrorMessage();
        }
        return JsonKey.SUCCESS;

    }

    private void processOrgInfo(CopyOnWriteArrayList<Map<String, Object>> jsonList,
            Map<String, Object> dataMap) {

        Map<String, String> channelToRootOrgCache = new HashMap<>();
        List<Map<String, Object>> successList = new ArrayList<>();
        List<Map<String, Object>> failureList = new ArrayList<>();
        // Iteration for rootorg
        for (Map<String, Object> map : jsonList) {
            try {
                if (map.containsKey(JsonKey.IS_ROOT_ORG)
                        && isNotNull(map.get(JsonKey.IS_ROOT_ORG))) {
                    Boolean isRootOrg = Boolean.valueOf((String) map.get(JsonKey.IS_ROOT_ORG));
                    if (isRootOrg) {
                        processOrg(map, dataMap, successList, failureList, channelToRootOrgCache);
                        jsonList.remove(map);
                    }
                }
            } catch (Exception ex) {
                ProjectLogger.log("Exception occurs  ", ex);
                map.put(JsonKey.ERROR_MSG, ex.getMessage());
                failureList.add(map);
            }
        }

        // Iteration for non root org
        for (Map<String, Object> map : jsonList) {
            try {
                processOrg(map, dataMap, successList, failureList, channelToRootOrgCache);
            } catch (Exception ex) {
                ProjectLogger.log("Exception occurs  ", ex);
                map.put(JsonKey.ERROR_MSG, ex.getMessage());
                failureList.add(map);
            }
        }

        dataMap.put(JsonKey.SUCCESS_RESULT, convertMapToJsonString(successList));
        dataMap.put(JsonKey.FAILURE_RESULT, convertMapToJsonString(failureList));
        dataMap.put(JsonKey.STATUS, BulkProcessStatus.COMPLETED.getValue());

        cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), dataMap);

    }

    @SuppressWarnings("unchecked")
    private void processOrg(Map<String, Object> map, Map<String, Object> dataMap,
            List<Map<String, Object>> successList, List<Map<String, Object>> failureList,
            Map<String, String> channelToRootOrgCache) {

        Map<String, Object> concurrentHashMap = map;
        Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
        Object[] orgContactList = null;
        String contactDetails = null;

        // object of telemetry event...
        Map<String, Object> targetObject = new HashMap<>();
        List<Map<String, Object>> correlatedObject = new ArrayList<>();

        if (concurrentHashMap.containsKey(JsonKey.ORG_TYPE) && !ProjectUtil
                .isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.ORG_TYPE))) {
            String orgTypeId = validateOrgType((String) concurrentHashMap.get(JsonKey.ORG_TYPE));
            if (null == orgTypeId) {
                concurrentHashMap.put(JsonKey.ERROR_MSG, "Invalid OrgType.");
                failureList.add(concurrentHashMap);
                return;
            } else {
                concurrentHashMap.put(JsonKey.ORG_TYPE_ID, orgTypeId);
            }
        }

        if (concurrentHashMap.containsKey(JsonKey.LOC_ID) && !ProjectUtil
                .isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.LOC_ID))) {
            String locId = validateLocationId((String) concurrentHashMap.get(JsonKey.LOC_ID));
            if (null == locId) {
                concurrentHashMap.put(JsonKey.ERROR_MSG, "Invalid Location Id.");
                failureList.add(concurrentHashMap);
                return;
            } else {
                concurrentHashMap.put(JsonKey.LOC_ID, locId);
            }
        }

        if (isNull(concurrentHashMap.get(JsonKey.ORGANISATION_NAME)) || ProjectUtil
                .isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.ORGANISATION_NAME))) {
            ProjectLogger.log("orgName is mandatory for org creation.");
            concurrentHashMap.put(JsonKey.ERROR_MSG, "orgName is mandatory for org creation.");
            failureList.add(concurrentHashMap);
            return;
        }

        Boolean isRootOrg;
        if (isNotNull(concurrentHashMap.get(JsonKey.IS_ROOT_ORG))) {
            isRootOrg = Boolean.valueOf((String) concurrentHashMap.get(JsonKey.IS_ROOT_ORG));
        } else {
            isRootOrg = false;
        }
        concurrentHashMap.put(JsonKey.IS_ROOT_ORG, isRootOrg);

        if (concurrentHashMap.containsKey(JsonKey.CONTACT_DETAILS) && !ProjectUtil
                .isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.CONTACT_DETAILS))) {

            contactDetails = (String) concurrentHashMap.get(JsonKey.CONTACT_DETAILS);
            contactDetails = contactDetails.replaceAll("'", "\"");
            try {
                ObjectMapper mapper = new ObjectMapper();
                orgContactList = mapper.readValue(contactDetails, Object[].class);

            } catch (IOException ex) {
                ProjectLogger.log("Unable to parse Org contact Details - OrgBulkUpload.", ex);
                concurrentHashMap.put(JsonKey.ERROR_MSG,
                        "Unable to parse Org contact Details - OrgBulkUpload.");
                failureList.add(concurrentHashMap);
                return;
            }
        }

        if (isNotNull(concurrentHashMap.get(JsonKey.PROVIDER))
                || isNotNull(concurrentHashMap.get(JsonKey.EXTERNAL_ID))) {
            if (isNull(concurrentHashMap.get(JsonKey.PROVIDER))
                    || isNull(concurrentHashMap.get(JsonKey.EXTERNAL_ID))) {
                ProjectLogger.log("Provider and external ids both should exist.");
                concurrentHashMap.put(JsonKey.ERROR_MSG,
                        "Provider and external ids both should exist.");
                failureList.add(concurrentHashMap);
                return;
            }

            Map<String, Object> dbMap = new HashMap<>();
            dbMap.put(JsonKey.PROVIDER, concurrentHashMap.get(JsonKey.PROVIDER));
            dbMap.put(JsonKey.EXTERNAL_ID, concurrentHashMap.get(JsonKey.EXTERNAL_ID));
            Response result = cassandraOperation.getRecordsByProperties(orgDbInfo.getKeySpace(),
                    orgDbInfo.getTableName(), dbMap);
            List<Map<String, Object>> list =
                    (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
            if (!(list.isEmpty())) {

                Map<String, Object> orgResult = list.get(0);

                boolean dbRootOrg = (boolean) orgResult.get(JsonKey.IS_ROOT_ORG);
                if (isRootOrg != dbRootOrg) {
                    ProjectLogger.log("Can not update isRootorg value ");
                    concurrentHashMap.put(JsonKey.ERROR_MSG, "Can not update isRootorg value ");
                    failureList.add(concurrentHashMap);
                    return;
                }

                if (!compareStrings((String) concurrentHashMap.get(JsonKey.CHANNEL),
                        (String) orgResult.get(JsonKey.CHANNEL))) {
                    ProjectLogger.log("Can not update is Channel value ");
                    concurrentHashMap.put(JsonKey.ERROR_MSG, "Can not update channel value ");
                    failureList.add(concurrentHashMap);
                    return;
                }

                // check logic here to check the hashtag id ... if in db null and requested one
                // not exist in
                // db only then we are going to update the new one...

                if (!ProjectUtil
                        .isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.HASHTAGID))) {
                    String requestedHashTagId = (String) concurrentHashMap.get(JsonKey.HASHTAGID);
                    // if both are not equal ...
                    if (!requestedHashTagId.equals(orgResult.get(JsonKey.HASHTAGID))) {
                        Map<String, Object> dbMap1 = new HashMap<>();
                        dbMap1.put(JsonKey.HASHTAGID, concurrentHashMap.get(JsonKey.HASHTAGID));
                        Response result1 = cassandraOperation.getRecordsByProperties(
                                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), dbMap1);
                        List<Map<String, Object>> list1 =
                                (List<Map<String, Object>>) result1.get(JsonKey.RESPONSE);
                        if (!list1.isEmpty()) {
                            ProjectLogger.log(
                                    "Can not update hashtag value , since it is already exist ");
                            concurrentHashMap.put(JsonKey.ERROR_MSG,
                                    "Hash Tag ID already exist for another org ");
                            failureList.add(concurrentHashMap);
                            return;
                        }
                    }
                }
                concurrentHashMap.put(JsonKey.ID, orgResult.get(JsonKey.ID));

                try {
                    cassandraOperation.upsertRecord(orgDbInfo.getKeySpace(),
                            orgDbInfo.getTableName(), concurrentHashMap);
                    Response orgResponse = new Response();

                    // sending the org contact as List if it is null simply remove from map
                    if (isNotNull(orgContactList)) {
                        concurrentHashMap.put(JsonKey.CONTACT_DETAILS,
                                Arrays.asList(orgContactList));
                    }

                    orgResponse.put(JsonKey.ORGANISATION, concurrentHashMap);
                    orgResponse.put(JsonKey.OPERATION,
                            ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
                    ProjectLogger.log("Calling background job to save org data into ES"
                            + orgResult.get(JsonKey.ID));
                    Request request = new Request();
                    request.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
                    request.getRequest().put(JsonKey.ORGANISATION, concurrentHashMap);
                    tellToAnother(request);
                    successList.add(concurrentHashMap);
                    // process Audit Log
                    processAuditLog(concurrentHashMap, ActorOperations.UPDATE_ORG.getValue(), "",
                            JsonKey.ORGANISATION);
                    return;
                } catch (Exception ex) {

                    ProjectLogger.log("Exception occurs  ", ex);
                    concurrentHashMap.put(JsonKey.ERROR_MSG, ex.getMessage());
                    failureList.add(concurrentHashMap);
                    return;
                }
            }
        }

        if (isRootOrg) {
            if (isNull(concurrentHashMap.get(JsonKey.CHANNEL))) {
                concurrentHashMap.put(JsonKey.ERROR_MSG, "Channel is mandatory for root org ");
                failureList.add(concurrentHashMap);
                return;
            }

            // check for unique root org for channel -----
            Map<String, Object> filters = new HashMap<>();
            filters.put(JsonKey.CHANNEL, concurrentHashMap.get(JsonKey.CHANNEL));
            filters.put(JsonKey.IS_ROOT_ORG, true);

            Map<String, Object> esResult = elasticSearchComplexSearch(filters,
                    EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());

            // if for root org true for this channel means simply update the existing record
            // ...
            if (isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT)
                    && isNotNull(esResult.get(JsonKey.CONTENT))
                    && ((List) esResult.get(JsonKey.CONTENT)).size() > 0) {

                List<Map<String, Object>> contentList =
                        (List<Map<String, Object>>) esResult.get(JsonKey.CONTENT);
                Map<String, Object> rootOrgInfo = contentList.get(0);
                concurrentHashMap.put(JsonKey.ID, contentList.get(0).get(JsonKey.ID));

                if (!compareStrings((String) concurrentHashMap.get(JsonKey.EXTERNAL_ID),
                        (String) rootOrgInfo.get(JsonKey.EXTERNAL_ID))) {
                    ProjectLogger.log("Can not update is External Id ");
                    concurrentHashMap.put(JsonKey.ERROR_MSG, "Can not update External Id ");
                    failureList.add(concurrentHashMap);
                    return;
                }

                if (!compareStrings((String) concurrentHashMap.get(JsonKey.PROVIDER),
                        (String) rootOrgInfo.get(JsonKey.PROVIDER))) {
                    ProjectLogger.log("Can not update is Provider ");
                    concurrentHashMap.put(JsonKey.ERROR_MSG, "Can not update Provider ");
                    failureList.add(concurrentHashMap);
                    return;
                }

                // check for duplicate hashtag id ...
                if (!ProjectUtil
                        .isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.HASHTAGID))) {
                    String requestedHashTagId = (String) concurrentHashMap.get(JsonKey.HASHTAGID);
                    // if both are not equal ...
                    if (!requestedHashTagId
                            .equalsIgnoreCase((String) rootOrgInfo.get(JsonKey.HASHTAGID))) {
                        Map<String, Object> dbMap1 = new HashMap<>();
                        dbMap1.put(JsonKey.HASHTAGID, concurrentHashMap.get(JsonKey.HASHTAGID));
                        Response result1 = cassandraOperation.getRecordsByProperties(
                                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), dbMap1);
                        List<Map<String, Object>> list1 =
                                (List<Map<String, Object>>) result1.get(JsonKey.RESPONSE);
                        if (!list1.isEmpty()) {
                            ProjectLogger.log(
                                    "Can not update hashtag value , since it is already exist ");
                            concurrentHashMap.put(JsonKey.ERROR_MSG,
                                    "Hash Tag ID already exist for another org ");
                            failureList.add(concurrentHashMap);
                            return;
                        }
                    }
                }

                try {
                    cassandraOperation.upsertRecord(orgDbInfo.getKeySpace(),
                            orgDbInfo.getTableName(), concurrentHashMap);
                    Response orgResponse = new Response();

                    // sending the org contact as List if it is null simply remove from map
                    if (isNotNull(orgContactList)) {
                        concurrentHashMap.put(JsonKey.CONTACT_DETAILS,
                                Arrays.asList(orgContactList));
                    }

                    orgResponse.put(JsonKey.ORGANISATION, concurrentHashMap);
                    orgResponse.put(JsonKey.OPERATION,
                            ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
                    ProjectLogger.log("Calling background job to save org data into ES"
                            + contentList.get(0).get(JsonKey.ID));
                    Request request = new Request();
                    request.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
                    request.getRequest().put(JsonKey.ORGANISATION, concurrentHashMap);
                    tellToAnother(request);
                    successList.add(concurrentHashMap);
                    // process Audit Log
                    processAuditLog(concurrentHashMap, ActorOperations.UPDATE_ORG.getValue(), "",
                            JsonKey.ORGANISATION);
                    return;
                } catch (Exception ex) {

                    ProjectLogger.log("Exception occurs  ", ex);
                    concurrentHashMap.put(JsonKey.ERROR_MSG, ex.getMessage());
                    failureList.add(concurrentHashMap);
                    return;
                }

            }
            concurrentHashMap.put(JsonKey.ROOT_ORG_ID, JsonKey.DEFAULT_ROOT_ORG_ID);
            channelToRootOrgCache.put((String) concurrentHashMap.get(JsonKey.CHANNEL),
                    (String) concurrentHashMap.get(JsonKey.ORGANISATION_NAME));

        } else {

            if (concurrentHashMap.containsKey(JsonKey.CHANNEL) && !(ProjectUtil
                    .isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.CHANNEL)))) {
                String channel = (String) concurrentHashMap.get(JsonKey.CHANNEL);
                if (channelToRootOrgCache.containsKey(channel)) {
                    concurrentHashMap.put(JsonKey.ROOT_ORG_ID, channelToRootOrgCache.get(channel));
                } else {
                    Map<String, Object> filters = new HashMap<>();
                    filters.put(JsonKey.CHANNEL, concurrentHashMap.get(JsonKey.CHANNEL));
                    filters.put(JsonKey.IS_ROOT_ORG, true);

                    Map<String, Object> esResult = elasticSearchComplexSearch(filters,
                            EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
                    if (isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT)
                            && isNotNull(esResult.get(JsonKey.CONTENT))
                            && ((List) esResult.get(JsonKey.CONTENT)).size() > 0) {

                        Map<String, Object> esContent =
                                ((List<Map<String, Object>>) esResult.get(JsonKey.CONTENT)).get(0);
                        concurrentHashMap.put(JsonKey.ROOT_ORG_ID, esContent.get(JsonKey.ID));
                        channelToRootOrgCache.put((String) concurrentHashMap.get(JsonKey.CHANNEL),
                                (String) esContent.get(JsonKey.ID));

                    } else {
                        concurrentHashMap.put(JsonKey.ERROR_MSG,
                                "This is not root org and No Root Org id exist for channel  "
                                        + concurrentHashMap.get(JsonKey.CHANNEL));
                        failureList.add(concurrentHashMap);
                        return;
                    }
                }
            } else if (concurrentHashMap.containsKey(JsonKey.PROVIDER)
                    && !(ProjectUtil.isStringNullOREmpty(JsonKey.PROVIDER))) {
                String rootOrgId = Util
                        .getRootOrgIdFromChannel((String) concurrentHashMap.get(JsonKey.PROVIDER));

                if (!(ProjectUtil.isStringNullOREmpty(rootOrgId))) {
                    concurrentHashMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
                } else {
                    concurrentHashMap.put(JsonKey.ROOT_ORG_ID, JsonKey.DEFAULT_ROOT_ORG_ID);
                }

            } else {
                concurrentHashMap.put(JsonKey.ROOT_ORG_ID, JsonKey.DEFAULT_ROOT_ORG_ID);
            }

        }

        // we can put logic here to check uniqueness of hash tag id in order to create
        // new organisation
        // ...
        if (!ProjectUtil.isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.HASHTAGID))) {

            Map<String, Object> dbMap1 = new HashMap<>();
            dbMap1.put(JsonKey.HASHTAGID, concurrentHashMap.get(JsonKey.HASHTAGID));
            Response result1 = cassandraOperation.getRecordsByProperties(orgDbInfo.getKeySpace(),
                    orgDbInfo.getTableName(), dbMap1);
            List<Map<String, Object>> list1 =
                    (List<Map<String, Object>>) result1.get(JsonKey.RESPONSE);
            if (!list1.isEmpty()) {
                ProjectLogger.log("Can not update hashtag value , since it is already exist ");
                concurrentHashMap.put(JsonKey.ERROR_MSG,
                        "Hash Tag ID already exist for another org ");
                failureList.add(concurrentHashMap);
                return;
            }
        }

        concurrentHashMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        concurrentHashMap.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
        // allow lower case values for provider and externalId to the database
        if (concurrentHashMap.get(JsonKey.PROVIDER) != null) {
            concurrentHashMap.put(JsonKey.PROVIDER,
                    ((String) concurrentHashMap.get(JsonKey.PROVIDER)).toLowerCase());
        }
        if (concurrentHashMap.get(JsonKey.EXTERNAL_ID) != null) {
            concurrentHashMap.put(JsonKey.EXTERNAL_ID,
                    ((String) concurrentHashMap.get(JsonKey.EXTERNAL_ID)).toLowerCase());
        }
        concurrentHashMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        concurrentHashMap.put(JsonKey.CREATED_BY, dataMap.get(JsonKey.UPLOADED_BY));
        String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(1);
        if (ProjectUtil.isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.ID))) {
            concurrentHashMap.put(JsonKey.ID, uniqueId);
            // if user does not provide hash tag id in the file set it to equivalent to org
            // id
            if (ProjectUtil
                    .isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.HASHTAGID))) {
                concurrentHashMap.put(JsonKey.HASHTAGID, uniqueId);
            }
            if (ProjectUtil.isNull(concurrentHashMap.get(JsonKey.IS_ROOT_ORG))) {
                concurrentHashMap.put(JsonKey.IS_ROOT_ORG, false);
            }
            Boolean isRootOrgFlag = (Boolean) concurrentHashMap.get(JsonKey.IS_ROOT_ORG);

            // Remove the slug key if coming form user input.
            concurrentHashMap.remove(JsonKey.SLUG);
            if (concurrentHashMap.containsKey(JsonKey.CHANNEL)) {
                String slug = Slug.makeSlug(
                        (String) concurrentHashMap.getOrDefault(JsonKey.CHANNEL, ""), true);
                if (null != isRootOrgFlag && isRootOrgFlag) {
                    boolean bool = isSlugUnique(slug);
                    if (bool) {
                        concurrentHashMap.put(JsonKey.SLUG, slug);
                    } else {
                        ProjectLogger.log(ResponseCode.slugIsNotUnique.getErrorMessage());
                        concurrentHashMap.put(JsonKey.ERROR_MSG,
                                ResponseCode.slugIsNotUnique.getErrorMessage());
                        failureList.add(concurrentHashMap);
                        return;
                    }
                } else {
                    concurrentHashMap.put(JsonKey.SLUG, slug);
                }
            }

            if (null != isRootOrgFlag && isRootOrgFlag) {
                boolean bool = Util.registerChannel(concurrentHashMap);
                if (!bool) {
                    ProjectLogger.log("channel registration failed.");
                    concurrentHashMap.put(JsonKey.ERROR_MSG, "channel registration failed.");
                    failureList.add(concurrentHashMap);
                    return;
                }
            }
        }

        concurrentHashMap.put(JsonKey.CONTACT_DETAILS, contactDetails);

        try {
            cassandraOperation.upsertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(),
                    concurrentHashMap);
            Response orgResponse = new Response();

            // sending the org contact as List if it is null simply remove from map
            if (isNotNull(orgContactList)) {
                concurrentHashMap.put(JsonKey.CONTACT_DETAILS, Arrays.asList(orgContactList));
            }
            orgResponse.put(JsonKey.ORGANISATION, concurrentHashMap);
            orgResponse.put(JsonKey.OPERATION, ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
            ProjectLogger.log("Calling background job to save org data into ES" + uniqueId);
            Request request = new Request();
            request.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
            request.getRequest().put(JsonKey.ORGANISATION, concurrentHashMap);
            tellToAnother(request);
            successList.add(concurrentHashMap);
            // process Audit Log
            processAuditLog(concurrentHashMap, ActorOperations.CREATE_ORG.getValue(), "",
                    JsonKey.ORGANISATION);
        } catch (Exception ex) {

            ProjectLogger.log("Exception occurs  ", ex);
            concurrentHashMap.put(JsonKey.ERROR_MSG, ex.getMessage());
            failureList.add(concurrentHashMap);
            return;
        }

        targetObject = TelemetryUtil.generateTargetObject(uniqueId, JsonKey.ORGANISATION,
                JsonKey.CREATE, null);
        TelemetryUtil.generateCorrelatedObject(uniqueId, JsonKey.ORGANISATION, null,
                correlatedObject);
        TelemetryUtil.telemetryProcessingCall(map, targetObject, correlatedObject);
    }

    private String validateLocationId(String locId) {
        String locnId = null;
        try {
            if (locnIdList.isEmpty()) {
                Util.DbInfo geoLocDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
                Response response = cassandraOperation.getAllRecords(geoLocDbInfo.getKeySpace(),
                        geoLocDbInfo.getTableName());
                List<Map<String, Object>> list =
                        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
                if (!list.isEmpty()) {
                    for (Map<String, Object> map : list) {
                        locnIdList.add(((String) map.get(JsonKey.ID)));
                    }
                }
            }
            if (locnIdList.contains(locId)) {
                return locId;
            } else {
                return null;
            }
        } catch (Exception ex) {
            ProjectLogger.log("Exception occurred while validating location id ", ex);
        }
        return locnId;
    }

    private String validateOrgType(String orgType) {
        String orgTypeId = null;
        try {
            if (!ProjectUtil.isStringNullOREmpty(
                    DataCacheHandler.getOrgTypeMap().get(orgType.toLowerCase()))) {
                orgTypeId = DataCacheHandler.getOrgTypeMap().get(orgType.toLowerCase());
            } else {
                Util.DbInfo orgTypeDbInfo = Util.dbInfoMap.get(JsonKey.ORG_TYPE_DB);
                Response response = cassandraOperation.getAllRecords(orgTypeDbInfo.getKeySpace(),
                        orgTypeDbInfo.getTableName());
                List<Map<String, Object>> list =
                        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
                if (!list.isEmpty()) {
                    for (Map<String, Object> map : list) {
                        if ((((String) map.get(JsonKey.NAME)).toLowerCase())
                                .equalsIgnoreCase(orgType.toLowerCase())) {
                            orgTypeId = (String) map.get(JsonKey.ID);
                            DataCacheHandler.getOrgTypeMap().put(
                                    ((String) map.get(JsonKey.NAME)).toLowerCase(),
                                    (String) map.get(JsonKey.ID));
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ProjectLogger.log("Exception occurred while getting orgTypeId from OrgType", ex);
        }
        return orgTypeId;
    }

    private Map<String, Object> elasticSearchComplexSearch(Map<String, Object> filters,
            String index, String type) {

        SearchDTO searchDTO = new SearchDTO();
        searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);

        return ElasticSearchUtil.complexSearch(searchDTO, index, type);

    }

    @SuppressWarnings("unchecked")
    private void processUserInfo(List<Map<String, Object>> dataMapList, String processId,
            String updatedBy) {
        // update status from NEW to INProgress
        updateStatusForProcessing(processId);
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        List<Map<String, Object>> failureUserReq = new ArrayList<>();
        List<Map<String, Object>> successUserReq = new ArrayList<>();
        Map<String, Object> userMap = null;
        for (int i = 0; i < dataMapList.size(); i++) {
            userMap = dataMapList.get(i);
            Map<String, Object> welcomeMailTemplateMap = new HashMap<>();
            if (ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.PASSWORD))) {
                String randomPassword = ProjectUtil.generateRandomPassword();
                userMap.put(JsonKey.PASSWORD, randomPassword);
                welcomeMailTemplateMap.put(JsonKey.TEMPORARY_PASSWORD, randomPassword);
            } else {
                welcomeMailTemplateMap.put(JsonKey.TEMPORARY_PASSWORD,
                        userMap.get(JsonKey.PASSWORD));
            }
            String errMsg = validateUser(userMap);
            if (errMsg.equalsIgnoreCase(JsonKey.SUCCESS)) {
                try {
                    // this role is part of organization
                    if (null != userMap.get(JsonKey.ROLES)) {
                        String[] userRole = ((String) userMap.get(JsonKey.ROLES)).split(",");
                        List<String> list = new ArrayList<>(Arrays.asList(userRole));
                        // validating roles
                        if (null != list && !list.isEmpty()) {
                            String msg = Util.validateRoles(list);
                            if (!msg.equalsIgnoreCase(JsonKey.SUCCESS)) {
                                userMap.put(JsonKey.ERROR_MSG, msg);
                                failureUserReq.add(userMap);
                                continue;
                            }
                        }
                        userMap.put(JsonKey.ROLES, list);
                    }

                    if (null != userMap.get(JsonKey.GRADE)) {
                        String[] userGrade = ((String) userMap.get(JsonKey.GRADE)).split(",");
                        List<String> list = new ArrayList<>(Arrays.asList(userGrade));
                        userMap.put(JsonKey.GRADE, list);
                    }

                    if (null != userMap.get(JsonKey.SUBJECT)) {
                        String[] subjects = ((String) userMap.get(JsonKey.SUBJECT)).split(",");
                        List<String> list = new ArrayList<>(Arrays.asList(subjects));
                        userMap.put(JsonKey.SUBJECT, list);
                    }

                    if (null != userMap.get(JsonKey.LANGUAGE)) {
                        String[] languages = ((String) userMap.get(JsonKey.LANGUAGE)).split(",");
                        List<String> list = new ArrayList<>(Arrays.asList(languages));
                        userMap.put(JsonKey.LANGUAGE, list);
                    }

                    if (null != userMap.get(JsonKey.WEB_PAGES)) {
                        String webPageString = (String) userMap.get(JsonKey.WEB_PAGES);
                        webPageString = webPageString.replaceAll("'", "\"");
                        List<Map<String, String>> webPages = new ArrayList<>();
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            webPages = mapper.readValue(webPageString, List.class);
                        } catch (Exception ex) {
                            ProjectLogger.log("Unable to parse Web Page Details ", ex);
                            userMap.put(JsonKey.ERROR_MSG, "Unable to parse Web Page Details ");
                            failureUserReq.add(userMap);
                            continue;
                        }
                        SocialMediaType.validateSocialMedia(webPages);
                        userMap.put(JsonKey.WEB_PAGES, webPages);
                    }
                    // convert userName,provide,loginId,externalId.. value to lowercase
                    updateMapSomeValueTOLowerCase(userMap);
                    userMap = insertRecordToKeyCloak(userMap);
                    Map<String, Object> tempMap = new HashMap<>();
                    tempMap.putAll(userMap);
                    tempMap.remove(JsonKey.EMAIL_VERIFIED);
                    tempMap.remove(JsonKey.PHONE_VERIFIED);
                    tempMap.remove(JsonKey.POSITION);
                    tempMap.put(JsonKey.EMAIL_VERIFIED, false);
                    Response response = null;
                    if (null == tempMap.get(JsonKey.OPERATION)) {
                        // will allowing only PUBLIC role at user level.
                        tempMap.remove(JsonKey.ROLES);
                        // insert user record
                        // Add only PUBLIC role to user
                        List<String> list = new ArrayList<>();
                        list.add(JsonKey.PUBLIC);
                        tempMap.put(JsonKey.ROLES, list);
                        try {
                            UserUtility.encryptUserData(tempMap);
                        } catch (Exception ex) {
                            ProjectLogger.log(
                                    "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during data encryption :",
                                    ex);
                            throw new ProjectCommonException(
                                    ResponseCode.userDataEncryptionError.getErrorCode(),
                                    ResponseCode.userDataEncryptionError.getErrorMessage(),
                                    ResponseCode.SERVER_ERROR.getResponseCode());
                        }
                        tempMap.put(JsonKey.CREATED_BY, updatedBy);
                        tempMap.put(JsonKey.IS_DELETED, false);
                        try {
                            response = cassandraOperation.insertRecord(usrDbInfo.getKeySpace(),
                                    usrDbInfo.getTableName(), tempMap);
                        } catch (Exception ex) {
                            ProjectLogger.log(
                                    "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor:",
                                    ex);
                            userMap.remove(JsonKey.ID);
                            userMap.remove(JsonKey.PASSWORD);
                            userMap.put(JsonKey.ERROR_MSG,
                                    ex.getMessage() + " ,user insertion failed.");
                            failureUserReq.add(userMap);
                            continue;
                        } finally {
                            if (null == response) {
                                ssoManager.removeUser(userMap);
                            }
                        }
                        // insert details to user_org table
                        insertRecordToUserOrgTable(userMap);
                        // send the welcome mail to user
                        welcomeMailTemplateMap.putAll(userMap);
                        // the loginid will become user id for logon purpose .
                        welcomeMailTemplateMap.put(JsonKey.USERNAME, userMap.get(JsonKey.LOGIN_ID));
                        sendOnboardingMail(welcomeMailTemplateMap);
                        sendSMS(userMap);
                        // process Audit Log
                        processAuditLog(userMap, ActorOperations.CREATE_USER.getValue(), updatedBy,
                                JsonKey.USER);
                        // generate telemetry for new user creation
                        // object of telemetry event...
                        Map<String, Object> targetObject = null;
                        List<Map<String, Object>> correlatedObject = new ArrayList<>();

                        targetObject =
                                TelemetryUtil.generateTargetObject((String) userMap.get(JsonKey.ID),
                                        JsonKey.USER, JsonKey.CREATE, null);
                        TelemetryUtil.telemetryProcessingCall(userMap, targetObject,
                                correlatedObject);
                    } else {
                        // update user record
                        tempMap.remove(JsonKey.OPERATION);
                        tempMap.remove(JsonKey.REGISTERED_ORG_ID);
                        tempMap.remove(JsonKey.ROOT_ORG_ID);
                        // will not allowing to update roles at user level
                        tempMap.remove(JsonKey.ROLES);
                        tempMap.put(JsonKey.UPDATED_BY, updatedBy);
                        tempMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
                        try {
                            UserUtility.encryptUserData(tempMap);
                        } catch (Exception ex) {
                            ProjectLogger.log(
                                    "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during data encryption :",
                                    ex);
                            throw new ProjectCommonException(
                                    ResponseCode.userDataEncryptionError.getErrorCode(),
                                    ResponseCode.userDataEncryptionError.getErrorMessage(),
                                    ResponseCode.SERVER_ERROR.getResponseCode());
                        }
                        try {
                            response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),
                                    usrDbInfo.getTableName(), tempMap);
                        } catch (Exception ex) {
                            ProjectLogger.log(
                                    "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor:",
                                    ex);
                            userMap.remove(JsonKey.ID);
                            userMap.remove(JsonKey.PASSWORD);
                            userMap.put(JsonKey.ERROR_MSG,
                                    ex.getMessage() + " ,user updation failed.");
                            failureUserReq.add(userMap);
                            continue;
                        }
                        // update user_org data
                        updateUserOrgData(userMap, updatedBy);
                        // Process Audit Log
                        processAuditLog(userMap, ActorOperations.UPDATE_USER.getValue(), updatedBy,
                                JsonKey.USER);
                    }
                    // save successfully created user data
                    tempMap.putAll(userMap);
                    tempMap.remove(JsonKey.STATUS);
                    tempMap.remove(JsonKey.CREATED_DATE);
                    tempMap.remove(JsonKey.CREATED_BY);
                    tempMap.remove(JsonKey.ID);
                    tempMap.put(JsonKey.PASSWORD, "*****");
                    successUserReq.add(tempMap);

                    // insert details to user Ext Identity table
                    insertRecordToUserExtTable(userMap);

                    // update elastic search
                    ProjectLogger.log(
                            "making a call to save user data to ES in BulkUploadBackGroundJobActor");
                    Request request = new Request();
                    request.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
                    request.getRequest().put(JsonKey.ID, userMap.get(JsonKey.ID));
                    tellToAnother(request);
                    // generate telemetry for update user
                    // object of telemetry event...
                    Map<String, Object> targetObject = null;
                    List<Map<String, Object>> correlatedObject = new ArrayList<>();
                    targetObject = TelemetryUtil.generateTargetObject(
                            (String) userMap.get(JsonKey.ID), JsonKey.USER, JsonKey.UPDATE, null);
                    TelemetryUtil.telemetryProcessingCall(userMap, targetObject, correlatedObject);
                } catch (Exception ex) {
                    ProjectLogger.log(
                            "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor:",
                            ex);
                    userMap.remove(JsonKey.ID);
                    userMap.remove(JsonKey.PASSWORD);
                    userMap.put(JsonKey.ERROR_MSG, ex.getMessage());
                    failureUserReq.add(userMap);
                }
            } else {
                userMap.put(JsonKey.ERROR_MSG, errMsg);
                failureUserReq.add(userMap);
            }
        }
        // Insert record to BulkDb table
        // After Successful completion of bulk upload process , encrypt the success and
        // failure result
        // and delete the user data(csv file data)
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.ID, processId);
        try {
            map.put(JsonKey.SUCCESS_RESULT,
                    UserUtility.encryptData(convertMapToJsonString(successUserReq)));
            map.put(JsonKey.FAILURE_RESULT,
                    UserUtility.encryptData(convertMapToJsonString(failureUserReq)));
        } catch (Exception e1) {
            ProjectLogger.log(
                    "Exception occurred while encrypting success and failure result in bulk upload process : ",
                    e1);
        }
        map.put(JsonKey.PROCESS_END_TIME, ProjectUtil.getFormattedDate());
        map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
        map.put(JsonKey.DATA, "");
        try {
            cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
        } catch (Exception e) {
            ProjectLogger.log(
                    "Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ",
                    e);
        }
    }

    private void updateUserOrgData(Map<String, Object> userMap, String updatedBy) {
        Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> reqMap = new HashMap<>();
        map.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
        map.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.REGISTERED_ORG_ID));
        Response response = cassandraOperation.getRecordsByProperties(usrOrgDb.getKeySpace(),
                usrOrgDb.getTableName(), map);
        List<Map<String, Object>> resList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if (!resList.isEmpty()) {
            Map<String, Object> res = resList.get(0);
            reqMap.put(JsonKey.ID, res.get(JsonKey.ID));
            reqMap.put(JsonKey.ROLES, userMap.get(JsonKey.ROLES));
            reqMap.put(JsonKey.UPDATED_BY, updatedBy);
            reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            try {
                cassandraOperation.updateRecord(usrOrgDb.getKeySpace(), usrOrgDb.getTableName(),
                        res);
            } catch (Exception e) {
                ProjectLogger.log(e.getMessage(), e);
            }
        }
    }

    private void processAuditLog(Map<String, Object> dataMap, String actorOperationType,
            String updatedBy, String objectType) {
        Request req = new Request();
        Response res = new Response();
        req.setRequestId(processId);
        req.setOperation(actorOperationType);
        dataMap.remove("header");
        req.getRequest().put(JsonKey.REQUESTED_BY, updatedBy);
        if (objectType.equalsIgnoreCase(JsonKey.USER)) {
            req.getRequest().put(JsonKey.USER, dataMap);
            res.getResult().put(JsonKey.USER_ID, dataMap.get(JsonKey.USER_ID));
        } else if (objectType.equalsIgnoreCase(JsonKey.ORGANISATION)) {
            req.getRequest().put(JsonKey.ORGANISATION, dataMap);
            res.getResult().put(JsonKey.ORGANISATION_ID, dataMap.get(JsonKey.ID));
        } else if (objectType.equalsIgnoreCase(JsonKey.BATCH)) {
            req.getRequest().put(JsonKey.BATCH, dataMap);
            res.getResult().put(JsonKey.BATCH_ID, dataMap.get(JsonKey.ID));
        }
        saveAuditLog(res, actorOperationType, req);
    }

    private void updateStatusForProcessing(String processId) {
        // Update status to BulkDb table
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.ID, processId);
        map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
        try {
            cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
        } catch (Exception e) {
            ProjectLogger.log(
                    "Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ",
                    e);
        }
    }

    private String convertMapToJsonString(List<Map<String, Object>> mapList) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(mapList);
        } catch (IOException e) {
            ProjectLogger.log(e.getMessage(), e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getBulkData(String processId) {
        try {
            Map<String, Object> map = new HashMap<>();
            map.put(JsonKey.ID, processId);
            map.put(JsonKey.PROCESS_START_TIME, ProjectUtil.getFormattedDate());
            map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
            cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
        } catch (Exception ex) {
            ProjectLogger.log("Exception occurred while updating status to bulk_upload_process "
                    + "table in BulkUploadBackGroundJobActor.", ex);
        }
        Response res = cassandraOperation.getRecordById(bulkDb.getKeySpace(), bulkDb.getTableName(),
                processId);
        return (((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0));
    }

    private void insertRecordToUserExtTable(Map<String, Object> requestMap) {
        Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> reqMap = new HashMap<>();
        reqMap.put(JsonKey.USER_ID, requestMap.get(JsonKey.USER_ID));
        /*
         * update table for userName,phone,email for each of these parameter insert a record into db
         * for username update isVerified as true and for others param this will be false once
         * verified will update this flag to true
         */

        map.put(JsonKey.USER_ID, requestMap.get(JsonKey.ID));
        map.put(JsonKey.IS_VERIFIED, false);
        if (requestMap.containsKey(JsonKey.USERNAME)
                && !(ProjectUtil.isStringNullOREmpty((String) requestMap.get(JsonKey.USERNAME)))) {
            map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
            map.put(JsonKey.EXTERNAL_ID, JsonKey.USERNAME);
            map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.USERNAME));
            map.put(JsonKey.IS_VERIFIED, true);

            reqMap.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.USERNAME));
            List<Map<String, Object>> mapList = checkDataUserExtTable(map);
            if (mapList.isEmpty()) {
                updateUserExtIdentity(map, usrExtIdDb, JsonKey.INSERT);
            }
        }
        if (requestMap.containsKey(JsonKey.PHONE)
                && !(ProjectUtil.isStringNullOREmpty((String) requestMap.get(JsonKey.PHONE)))) {
            map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
            map.put(JsonKey.EXTERNAL_ID, JsonKey.PHONE);
            map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.PHONE));

            if (null != (requestMap.get(JsonKey.PHONE_VERIFIED))
                    && (boolean) requestMap.get(JsonKey.PHONE_VERIFIED)) {
                map.put(JsonKey.IS_VERIFIED, true);
            }
            reqMap.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.PHONE));
            List<Map<String, Object>> mapList = checkDataUserExtTable(map);
            if (mapList.isEmpty()) {
                updateUserExtIdentity(map, usrExtIdDb, JsonKey.INSERT);
            } else {
                map.put(JsonKey.ID, mapList.get(0).get(JsonKey.ID));
                updateUserExtIdentity(map, usrExtIdDb, JsonKey.UPDATE);
            }
        }
        if (requestMap.containsKey(JsonKey.EMAIL)
                && !(ProjectUtil.isStringNullOREmpty((String) requestMap.get(JsonKey.EMAIL)))) {
            map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
            map.put(JsonKey.EXTERNAL_ID, JsonKey.EMAIL);
            map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.EMAIL));

            map.put(JsonKey.IS_VERIFIED, false);
            reqMap.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.EMAIL));
            List<Map<String, Object>> mapList = checkDataUserExtTable(map);
            if (mapList.isEmpty()) {
                updateUserExtIdentity(map, usrExtIdDb, JsonKey.INSERT);
            } else {
                map.put(JsonKey.ID, mapList.get(0).get(JsonKey.ID));
                updateUserExtIdentity(map, usrExtIdDb, JsonKey.UPDATE);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> checkDataUserExtTable(Map<String, Object> map) {
        Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
        Map<String, Object> reqMap = new HashMap<>();
        reqMap.put(JsonKey.USER_ID, map.get(JsonKey.USER_ID));
        reqMap.put(JsonKey.EXTERNAL_ID_VALUE, map.get(JsonKey.EXTERNAL_ID_VALUE));
        Response response = null;
        List<Map<String, Object>> responseList = new ArrayList<>();
        try {
            response = cassandraOperation.getRecordsByProperties(usrExtIdDb.getKeySpace(),
                    usrExtIdDb.getTableName(), reqMap);
        } catch (Exception ex) {
            ProjectLogger.log(
                    "Exception Occured while fetching data from user Ext Table in bulk upload", ex);
        }
        if (null != response) {
            responseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        }
        return responseList;
    }

    private void updateUserExtIdentity(Map<String, Object> map, DbInfo usrExtIdDb, String opType) {
        try {
            if (JsonKey.INSERT.equalsIgnoreCase(opType)) {
                cassandraOperation.insertRecord(usrExtIdDb.getKeySpace(), usrExtIdDb.getTableName(),
                        map);
            } else {
                cassandraOperation.updateRecord(usrExtIdDb.getKeySpace(), usrExtIdDb.getTableName(),
                        map);
            }
        } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
        }

    }

    @SuppressWarnings("unchecked")
    private void insertRecordToUserOrgTable(Map<String, Object> userMap) {
        Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        Map<String, Object> reqMap = new HashMap<>();
        reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
        reqMap.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.REGISTERED_ORG_ID));
        reqMap.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
        reqMap.put(JsonKey.POSITION, userMap.get(JsonKey.POSITION));
        reqMap.put(JsonKey.IS_DELETED, false);
        List<String> roleList = (List<String>) userMap.get(JsonKey.ROLES);
        reqMap.put(JsonKey.ROLES, roleList);

        try {
            cassandraOperation.insertRecord(usrOrgDb.getKeySpace(), usrOrgDb.getTableName(),
                    reqMap);
        } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> insertRecordToKeyCloak(Map<String, Object> userMap) {
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        if (userMap.containsKey(JsonKey.PROVIDER)
                && !ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.PROVIDER))) {
            userMap.put(JsonKey.LOGIN_ID, (String) userMap.get(JsonKey.USERNAME) + "@"
                    + (String) userMap.get(JsonKey.PROVIDER));
        } else {
            userMap.put(JsonKey.LOGIN_ID, userMap.get(JsonKey.USERNAME));
        }

        if (null != userMap.get(JsonKey.LOGIN_ID)) {
            String loginId = "";
            try {
                loginId = encryptionService.encryptData((String) userMap.get(JsonKey.LOGIN_ID));
            } catch (Exception ex) {
                ProjectLogger.log(
                        "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during encryption of loginId:",
                        ex);
                throw new ProjectCommonException(
                        ResponseCode.userDataEncryptionError.getErrorCode(),
                        ResponseCode.userDataEncryptionError.getErrorMessage(),
                        ResponseCode.SERVER_ERROR.getResponseCode());
            }
            Response resultFrUserName = cassandraOperation.getRecordsByProperty(
                    usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), JsonKey.LOGIN_ID, loginId);
            if (!(((List<Map<String, Object>>) resultFrUserName.get(JsonKey.RESPONSE)).isEmpty())) {
                // user exist
                Map<String, Object> map =
                        ((List<Map<String, Object>>) resultFrUserName.get(JsonKey.RESPONSE)).get(0);
                if (null != map.get(JsonKey.IS_DELETED) && (boolean) map.get(JsonKey.IS_DELETED)) {
                    throw new ProjectCommonException(ResponseCode.inactiveUser.getErrorCode(),
                            ResponseCode.inactiveUser.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }

                userMap.put(JsonKey.ID, map.get(JsonKey.ID));
                userMap.put(JsonKey.USER_ID, map.get(JsonKey.ID));
                userMap.put(JsonKey.OPERATION, JsonKey.UPDATE);
                if (userMap.get(JsonKey.REGISTERED_ORG_ID)
                        .equals(map.get(JsonKey.REGISTERED_ORG_ID))) {
                    checkEmailUniqueness(userMap, JsonKey.UPDATE);
                    checkPhoneUniqueness(userMap, JsonKey.UPDATE);
                    String email = "";
                    try {
                        email = encryptionService.encryptData((String) userMap.get(JsonKey.EMAIL));
                    } catch (Exception ex) {
                        ProjectLogger.log(
                                "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during encryption of loginId:",
                                ex);
                        throw new ProjectCommonException(
                                ResponseCode.userDataEncryptionError.getErrorCode(),
                                ResponseCode.userDataEncryptionError.getErrorMessage(),
                                ResponseCode.SERVER_ERROR.getResponseCode());
                    }
                    if (null != (String) map.get(JsonKey.EMAIL)
                            && ((String) map.get(JsonKey.EMAIL)).equalsIgnoreCase(email)) {
                        // DB email value and req email value both are same , no need to update
                        email = (String) userMap.get(JsonKey.EMAIL);
                        userMap.remove(JsonKey.EMAIL);
                    }
                    // check user is active for this organization or not
                    if (isUserDeletedFromOrg(userMap)) {
                        throw new ProjectCommonException(
                                ResponseCode.userInactiveForThisOrg.getErrorCode(),
                                ResponseCode.userInactiveForThisOrg.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                    }
                    updateKeyCloakUserBase(userMap);
                    userMap.put(JsonKey.EMAIL, email);
                } else {
                    throw new ProjectCommonException(ResponseCode.userRegOrgError.getErrorCode(),
                            ResponseCode.userRegOrgError.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }
            } else {
                // user doesn't exist
                try {
                    String userId = "";
                    userMap.put(JsonKey.BULK_USER_UPLOAD, true);
                    checkEmailUniqueness(userMap, JsonKey.CREATE);
                    checkPhoneUniqueness(userMap, JsonKey.CREATE);
                    Map<String, String> userKeyClaokResp = ssoManager.createUser(userMap);
                    userMap.remove(JsonKey.BULK_USER_UPLOAD);
                    userId = userKeyClaokResp.get(JsonKey.USER_ID);
                    if (!ProjectUtil.isStringNullOREmpty(userId)) {
                        userMap.put(JsonKey.USER_ID, userId);
                        userMap.put(JsonKey.ID, userId);
                    } else {
                        throw new ProjectCommonException(
                                ResponseCode.userRegUnSuccessfull.getErrorCode(),
                                ResponseCode.userRegUnSuccessfull.getErrorMessage(),
                                ResponseCode.SERVER_ERROR.getResponseCode());
                    }
                } catch (Exception exception) {
                    ProjectLogger.log("Exception occured while creating user in keycloak ",
                            exception);
                    throw exception;
                }
                userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
                userMap.put(JsonKey.IS_DELETED, false);
                if (!ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.COUNTRY_CODE))) {
                    userMap.put(JsonKey.COUNTRY_CODE,
                            propertiesCache.getProperty("sunbird_default_country_code"));
                }
                /**
                 * set role as PUBLIC by default if role is empty in request body. And if roles are
                 * coming in request body, then check for PUBLIC role , if not present then add
                 * PUBLIC role to the list
                 *
                 */

                if (null != userMap.get(JsonKey.ROLES)) {
                    List<String> roles = (List<String>) userMap.get(JsonKey.ROLES);
                    if (!roles.contains(ProjectUtil.UserRole.PUBLIC.getValue())) {
                        roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
                        userMap.put(JsonKey.ROLES, roles);
                    }
                } else {
                    List<String> roles = new ArrayList<>();
                    roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
                    userMap.put(JsonKey.ROLES, roles);
                }
            }
        }
        if (!ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.PASSWORD))) {
            userMap.put(JsonKey.PASSWORD,
                    OneWayHashing.encryptVal((String) userMap.get(JsonKey.PASSWORD)));
        }
        return userMap;
    }

    private boolean isUserDeletedFromOrg(Map<String, Object> userMap) {
        Util.DbInfo usrOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
        map.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.REGISTERED_ORG_ID));
        Response response = cassandraOperation.getRecordsByProperties(usrOrgDbInfo.getKeySpace(),
                usrOrgDbInfo.getTableName(), map);
        List<Map<String, Object>> resList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if (!resList.isEmpty()) {
            Map<String, Object> res = resList.get(0);
            if (null != res.get(JsonKey.IS_DELETED)) {
                return (boolean) (res.get(JsonKey.IS_DELETED));
            } else {
                return false;
            }
        }
        return false;

    }

    private void checkEmailUniqueness(Map<String, Object> userMap, String opType) {
        // Get Email configuration if not found , by default Email can be duplicate
        // across the
        // application
        String emailSetting = DataCacheHandler.getConfigSettings().get(JsonKey.EMAIL);
        if (null != emailSetting && JsonKey.UNIQUE.equalsIgnoreCase(emailSetting)) {
            String email = (String) userMap.get(JsonKey.EMAIL);
            if (!ProjectUtil.isStringNullOREmpty(email)) {
                try {
                    email = encryptionService.encryptData(email);
                } catch (Exception e) {
                    ProjectLogger.log("Exception occured while encrypting Email ", e);
                }
                Map<String, Object> filters = new HashMap<>();
                filters.put(JsonKey.ENC_EMAIL, email);
                Map<String, Object> map = new HashMap<>();
                map.put(JsonKey.FILTERS, filters);
                SearchDTO searchDto = Util.createSearchDto(map);
                Map<String, Object> result = ElasticSearchUtil.complexSearch(searchDto,
                        ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.user.getTypeName());
                List<Map<String, Object>> userMapList =
                        (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
                if (!userMapList.isEmpty()) {
                    if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
                        throw new ProjectCommonException(ResponseCode.emailInUse.getErrorCode(),
                                ResponseCode.emailInUse.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                    } else {
                        Map<String, Object> user = userMapList.get(0);
                        if (!(((String) user.get(JsonKey.ID))
                                .equalsIgnoreCase((String) userMap.get(JsonKey.ID)))) {
                            throw new ProjectCommonException(ResponseCode.emailInUse.getErrorCode(),
                                    ResponseCode.emailInUse.getErrorMessage(),
                                    ResponseCode.CLIENT_ERROR.getResponseCode());
                        }
                    }
                }
            }
        }
    }

    private void checkPhoneUniqueness(Map<String, Object> userMap, String opType) {
        // Get Phone configuration if not found , by default phone will be unique across
        // the application
        String phoneSetting = DataCacheHandler.getConfigSettings().get(JsonKey.PHONE);
        if (null != phoneSetting && JsonKey.UNIQUE.equalsIgnoreCase(phoneSetting)) {
            String phone = (String) userMap.get(JsonKey.PHONE);
            if (!ProjectUtil.isStringNullOREmpty(phone)) {
                try {
                    phone = encryptionService.encryptData(phone);
                } catch (Exception e) {
                    ProjectLogger.log("Exception occured while encrypting phone number ", e);
                }
                Map<String, Object> filters = new HashMap<>();
                filters.put(JsonKey.ENC_PHONE, phone);
                Map<String, Object> map = new HashMap<>();
                map.put(JsonKey.FILTERS, filters);
                SearchDTO searchDto = Util.createSearchDto(map);
                Map<String, Object> result = ElasticSearchUtil.complexSearch(searchDto,
                        ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.user.getTypeName());
                List<Map<String, Object>> userMapList =
                        (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
                if (!userMapList.isEmpty()) {
                    if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
                        throw new ProjectCommonException(
                                ResponseCode.PhoneNumberInUse.getErrorCode(),
                                ResponseCode.PhoneNumberInUse.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                    } else {
                        Map<String, Object> user = userMapList.get(0);
                        if (!(((String) user.get(JsonKey.ID))
                                .equalsIgnoreCase((String) userMap.get(JsonKey.ID)))) {
                            throw new ProjectCommonException(
                                    ResponseCode.PhoneNumberInUse.getErrorCode(),
                                    ResponseCode.PhoneNumberInUse.getErrorMessage(),
                                    ResponseCode.CLIENT_ERROR.getResponseCode());
                        }
                    }
                }
            }
        }
    }

    private String validateUser(Map<String, Object> map) {
        if (null != map.get(JsonKey.DOB)) {
            boolean bool = ProjectUtil.isDateValidFormat(ProjectUtil.YEAR_MONTH_DATE_FORMAT,
                    (String) map.get(JsonKey.DOB));
            if (!bool) {
                return "Incorrect DOB date format.";
            }
        }
        if (!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PHONE))) {
            if (((String) map.get(JsonKey.PHONE)).contains("+")) {
                return ResponseCode.invalidPhoneNumber.getErrorMessage();
            }
            boolean bool = ProjectUtil.validatePhone((String) map.get(JsonKey.PHONE),
                    (String) map.get(JsonKey.COUNTRY_CODE));
            if (!bool) {
                return ResponseCode.phoneNoFormatError.getErrorMessage();
            }
        }
        if (!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.COUNTRY_CODE))) {
            boolean bool = ProjectUtil.validateCountryCode((String) map.get(JsonKey.COUNTRY_CODE));
            if (!bool) {
                return ResponseCode.invalidCountryCode.getErrorMessage();
            }
        }
        if (ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.EMAIL))
                && ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PHONE))) {
            return ResponseCode.emailorPhoneRequired.getErrorMessage();
        }
        if (map.get(JsonKey.USERNAME) == null
                || (ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.USERNAME)))) {
            return ResponseCode.userNameRequired.getErrorMessage();
        }
        if (map.get(JsonKey.FIRST_NAME) == null
                || (ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.FIRST_NAME)))) {
            return ResponseCode.firstNameRequired.getErrorMessage();
        }
        if (!(ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.EMAIL)))
                && !ProjectUtil.isEmailvalid((String) map.get(JsonKey.EMAIL))) {
            return ResponseCode.emailFormatError.getErrorMessage();
        }
        if (!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PHONE_VERIFIED))) {
            try {
                map.put(JsonKey.PHONE_VERIFIED,
                        Boolean.parseBoolean((String) map.get(JsonKey.PHONE_VERIFIED)));
            } catch (Exception ex) {
                return "property phoneVerified should be instanceOf type Boolean.";
            }
        }
        if (!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.EMAIL_VERIFIED))) {
            try {
                map.put(JsonKey.EMAIL_VERIFIED,
                        Boolean.parseBoolean((String) map.get(JsonKey.EMAIL_VERIFIED)));
            } catch (Exception ex) {
                return "property emailVerified should be instanceOf type Boolean.";
            }
        }
        if (!ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PROVIDER))
                && !ProjectUtil.isStringNullOREmpty((String) map.get(JsonKey.PHONE))) {
            if (null != map.get(JsonKey.PHONE_VERIFIED)) {
                if (map.get(JsonKey.PHONE_VERIFIED) instanceof Boolean) {
                    if (!((boolean) map.get(JsonKey.PHONE_VERIFIED))) {
                        return ResponseCode.phoneVerifiedError.getErrorMessage();
                    }
                } else {
                    return "property phoneVerified should be instanceOf type Boolean.";
                }
            } else {
                return ResponseCode.phoneVerifiedError.getErrorMessage();
            }
        }

        return JsonKey.SUCCESS;
    }

    private String generatePrimaryKey(Map<String, Object> req) {
        String userId = (String) req.get(JsonKey.USER_ID);
        String courseId = (String) req.get(JsonKey.COURSE_ID);
        String batchId = (String) req.get(JsonKey.BATCH_ID);
        return OneWayHashing.encryptVal(userId + JsonKey.PRIMARY_KEY_DELIMETER + courseId
                + JsonKey.PRIMARY_KEY_DELIMETER + batchId);

    }

    /**
     * This method will make some requested key value as lower case.
     * 
     * @param map Request
     */
    public static void updateMapSomeValueTOLowerCase(Map<String, Object> map) {
        if (map.get(JsonKey.SOURCE) != null) {
            map.put(JsonKey.SOURCE, ((String) map.get(JsonKey.SOURCE)).toLowerCase());
        }
        if (map.get(JsonKey.EXTERNAL_ID) != null) {
            map.put(JsonKey.EXTERNAL_ID, ((String) map.get(JsonKey.EXTERNAL_ID)).toLowerCase());
        }
        if (map.get(JsonKey.USERNAME) != null) {
            map.put(JsonKey.USERNAME, ((String) map.get(JsonKey.USERNAME)).toLowerCase());
        }
        if (map.get(JsonKey.USER_NAME) != null) {
            map.put(JsonKey.USER_NAME, ((String) map.get(JsonKey.USER_NAME)).toLowerCase());
        }
        if (map.get(JsonKey.PROVIDER) != null) {
            map.put(JsonKey.PROVIDER, ((String) map.get(JsonKey.PROVIDER)).toLowerCase());
        }
        if (map.get(JsonKey.LOGIN_ID) != null) {
            map.put(JsonKey.LOGIN_ID, ((String) map.get(JsonKey.LOGIN_ID)).toLowerCase());
        }

    }

    private void updateKeyCloakUserBase(Map<String, Object> userMap) {
        try {
            String userId = ssoManager.updateUser(userMap);
            if (!(!ProjectUtil.isStringNullOREmpty(userId)
                    && userId.equalsIgnoreCase(JsonKey.SUCCESS))) {
                throw new ProjectCommonException(
                        ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
                        ResponseCode.userUpdationUnSuccessfull.getErrorMessage(),
                        ResponseCode.SERVER_ERROR.getResponseCode());
            } else if (!ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.EMAIL))) {
                // if Email is Null or Empty , it means we are not updating email
                Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
                Map<String, Object> map = new HashMap<>();
                map.put(JsonKey.ID, userId);
                map.put(JsonKey.EMAIL_VERIFIED, false);
                cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(),
                        map);
            }
        } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
            throw new ProjectCommonException(ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
                    ResponseCode.userUpdationUnSuccessfull.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }
    }

    // method will compare two strings and return true id both are same otherwise
    // false ...
    private boolean compareStrings(String first, String second) {

        if (isNull(first) && isNull(second)) {
            return true;
        }
        if ((isNull(first) && isNotNull(second)) || (isNull(second) && isNotNull(first))) {
            return false;
        }
        return first.equalsIgnoreCase(second);
    }

    private void saveAuditLog(Response result, String operation, Request message) {
        AuditOperation auditOperation = (AuditOperation) Util.auditLogUrlMap.get(operation);
        if (auditOperation.getObjectType().equalsIgnoreCase(JsonKey.USER)) {
            try {
                Map<String, Object> map = new HashMap<>();
                map.putAll((Map<String, Object>) message.getRequest().get(JsonKey.USER));
                UserUtility.encryptUserData(map);
                message.getRequest().put(JsonKey.USER, map);
            } catch (Exception ex) {
                ProjectLogger.log(
                        "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during data encryption :",
                        ex);
            }
        }
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.OPERATION, auditOperation);
        map.put(JsonKey.REQUEST, message);
        map.put(JsonKey.RESPONSE, result);
        Request request = new Request();
        request.setOperation(ActorOperations.PROCESS_AUDIT_LOG.getValue());
        request.setRequest(map);
        tellToAnother(request);
    }

    private void sendOnboardingMail(Map<String, Object> emailTemplateMap) {

        if (!(ProjectUtil.isStringNullOREmpty((String) emailTemplateMap.get(JsonKey.EMAIL)))) {

            String envName = System.getenv(JsonKey.SUNBIRD_INSTALLATION);
            if (ProjectUtil.isStringNullOREmpty(envName)) {
                envName = propertiesCache.getProperty(JsonKey.SUNBIRD_INSTALLATION);
            }

            String welcomeSubject = propertiesCache.getProperty("onboarding_mail_subject");
            emailTemplateMap.put(JsonKey.SUBJECT,
                    ProjectUtil.formatMessage(welcomeSubject, envName));
            List<String> reciptientsMail = new ArrayList<>();
            reciptientsMail.add((String) emailTemplateMap.get(JsonKey.EMAIL));
            emailTemplateMap.put(JsonKey.RECIPIENT_EMAILS, reciptientsMail);

            String webUrl = Util.getSunbirdWebUrlPerTenent(emailTemplateMap);
            if ((!ProjectUtil.isStringNullOREmpty(webUrl))
                    && (!SUNBIRD_WEB_URL.equalsIgnoreCase(webUrl))) {
                emailTemplateMap.put(JsonKey.WEB_URL, webUrl);
            }

            String appUrl = System.getenv(SUNBIRD_APP_URL);
            if (ProjectUtil.isStringNullOREmpty(appUrl)) {
                appUrl = propertiesCache.getProperty(SUNBIRD_APP_URL);
            }

            if ((!ProjectUtil.isStringNullOREmpty(appUrl))
                    && (!SUNBIRD_APP_URL.equalsIgnoreCase(appUrl))) {
                emailTemplateMap.put(JsonKey.APP_URL, appUrl);
            }

            emailTemplateMap.put(JsonKey.BODY,
                    propertiesCache.getProperty(JsonKey.ONBOARDING_WELCOME_MAIL_BODY));
            emailTemplateMap.put(JsonKey.NOTE, propertiesCache.getProperty(JsonKey.MAIL_NOTE));
            emailTemplateMap.put(JsonKey.ORG_NAME, envName);
            String welcomeMessage = propertiesCache.getProperty("onboarding_welcome_message");
            emailTemplateMap.put(JsonKey.WELCOME_MESSAGE,
                    ProjectUtil.formatMessage(welcomeMessage, envName));

            emailTemplateMap.put(JsonKey.EMAIL_TEMPLATE_TYPE, "welcome");

            Request request = new Request();
            request.setOperation(BackgroundOperations.emailService.name());
            request.put(JsonKey.EMAIL_REQUEST, emailTemplateMap);
            tellToAnother(request);
        }
    }

    private boolean isSlugUnique(String slug) {
        if (!ProjectUtil.isStringNullOREmpty(slug)) {
            Map<String, Object> filters = new HashMap<>();
            filters.put(JsonKey.SLUG, slug);
            filters.put(JsonKey.IS_ROOT_ORG, true);
            Map<String, Object> esResult = elasticSearchComplexSearch(filters,
                    EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
            if (isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT)
                    && isNotNull(esResult.get(JsonKey.CONTENT))) {
                return (((List) esResult.get(JsonKey.CONTENT)).isEmpty());
            }
        }
        return false;
    }

    private void sendSMS(Map<String, Object> userMap) {
        if (ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.EMAIL))
                && !ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.PHONE))) {
            UserUtility.decryptUserData(userMap);
            String name = (String) userMap.get(JsonKey.FIRST_NAME) + " "
                    + (String) userMap.get(JsonKey.LAST_NAME);
            String envName = System.getenv(JsonKey.SUNBIRD_INSTALLATION);
            if (ProjectUtil.isStringNullOREmpty(envName)) {
                envName = propertiesCache.getProperty(JsonKey.SUNBIRD_INSTALLATION);
            }

            String webUrl = Util.getSunbirdWebUrlPerTenent(userMap);

            ProjectLogger.log("shortened url :: " + webUrl);
            String sms = ProjectUtil.getSMSBody(name, webUrl, envName);
            if (ProjectUtil.isStringNullOREmpty(sms)) {
                sms = PropertiesCache.getInstance().getProperty("sunbird_default_welcome_sms");
            }
            ProjectLogger.log("SMS text : " + sms);

            String countryCode = "";
            if (ProjectUtil.isStringNullOREmpty((String) userMap.get(JsonKey.COUNTRY_CODE))) {
                countryCode =
                        PropertiesCache.getInstance().getProperty("sunbird_default_country_code");
            } else {
                countryCode = (String) userMap.get(JsonKey.COUNTRY_CODE);
            }
            ISmsProvider smsProvider = SMSFactory.getInstance("91SMS");
            boolean response =
                    smsProvider.send((String) userMap.get(JsonKey.PHONE), countryCode, sms);
            if (response) {
                ProjectLogger.log("Welcome Message sent successfully to ."
                        + (String) userMap.get(JsonKey.PHONE));
            } else {
                ProjectLogger
                        .log("Welcome Message failed for ." + (String) userMap.get(JsonKey.PHONE));
            }
        }
    }
}
