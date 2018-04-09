package org.sunbird.user.actors;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.VelocityContext;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Constants;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.models.util.mail.SendMail;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.SocialMediaType;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.notification.sms.provider.ISmsProvider;
import org.sunbird.notification.utils.SMSFactory;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;

/**
 * This actor will handle course enrollment operation .
 *
 * @author Manzarul
 * @author Amit Kumar
 */
@ActorConfig(tasks = {"createUser", "updateUser", "login", "logout", "changePassword",
        "getUserProfile", "getRoles", "getUserDetailsByLoginId", "downloadUsersData",
        "forgotpassword", "profileVisibility", "unblockUser", "blockUser", "assignRoles",
        "userCurrentLogin", "getMediaTypes"}, asyncTasks = {})
public class UserManagementActor extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private SSOManager ssoManager = SSOServiceFactory.getInstance();
    private EncryptionService encryptionService =
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                    .getEncryptionServiceInstance(null);
    private DecryptionService decryptionService =
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                    .getDecryptionServiceInstance(null);
    private PropertiesCache propertiesCache = PropertiesCache.getInstance();
    private boolean isSSOEnabled =
            Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
    private Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    private Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
    private static final String SUNBIRD_WEB_URL = "sunbird_web_url";
    private static final String SUNBIRD_APP_URL = "sunbird_app_url";

    /**
     * Receives the actor message and perform the course enrollment operation .
     */
    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, JsonKey.USER);
        // set request id fto thread loacl...
        ExecutionContext.setRequestId(request.getRequestId());
        String operation = request.getOperation();
        if (operation.equalsIgnoreCase(ActorOperations.CREATE_USER.getValue())) {
            createUser(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.UPDATE_USER.getValue())) {
            updateUser(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.LOGIN.getValue())) {
            login(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.LOGOUT.getValue())) {
            logout(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.CHANGE_PASSWORD.getValue())) {
            changePassword(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.GET_PROFILE.getValue())) {
            getUserProfile(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.GET_ROLES.getValue())) {
            getRoles();
        } else if (operation.equalsIgnoreCase(ActorOperations.JOIN_USER_ORGANISATION.getValue())) {
            joinUserOrganisation(request);
        } else if (operation
                .equalsIgnoreCase(ActorOperations.APPROVE_USER_ORGANISATION.getValue())) {
            approveUserOrg(request);
        } else if (operation
                .equalsIgnoreCase(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue())) {
            getUserDetailsByLoginId(request);
        } else if (operation
                .equalsIgnoreCase(ActorOperations.REJECT_USER_ORGANISATION.getValue())) {
            rejectUserOrg(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.DOWNLOAD_USERS.getValue())) {
            getUserDetails(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.BLOCK_USER.getValue())) {
            blockUser(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.ASSIGN_ROLES.getValue())) {
            assignRoles(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.UNBLOCK_USER.getValue())) {
            unBlockUser(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.USER_CURRENT_LOGIN.getValue())) {
            updateUserLoginTime(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.GET_MEDIA_TYPES.getValue())) {
            getMediaTypes();
        } else if (operation.equalsIgnoreCase(ActorOperations.FORGOT_PASSWORD.getValue())) {
            forgotPassword(request);
        } else if (operation.equalsIgnoreCase(ActorOperations.PROFILE_VISIBILITY.getValue())) {
            profileVisibility(request);
        } else {
            ProjectLogger.log("UNSUPPORTED OPERATION");
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                            ResponseCode.invalidOperationName.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
        }
    }

    /**
     * This method will first check user exist with us or not. after that it will create private
     * filed Map, for creating private field map it will take store value from ES and then a
     * separate map for private field and remove those field from original map. if will user is
     * sending some public field list as well then it will take private field values from another ES
     * index and update values under original data.
     * 
     * @param actorMessage
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void profileVisibility(Request actorMessage) {
        // object of telemetry event...
        Map<String, Object> targetObject = null;
        Map<String, Object> map = (Map) actorMessage.getRequest().get(JsonKey.USER);
        String userId = (String) map.get(JsonKey.USER_ID);
        List<String> privateList = (List) map.get(JsonKey.PRIVATE);
        List<String> publicList = (List) map.get(JsonKey.PUBLIC);
        Map<String, Object> esResult =
                ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.user.getTypeName(), userId);
        if (esResult == null || esResult.size() == 0) {
            throw new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
                    ResponseCode.userNotFound.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        Map<String, Object> esPrivateResult =
                ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.userprofilevisibility.getTypeName(), userId);
        Map<String, Object> responseMap = new HashMap<>();
        if (privateList != null && !privateList.isEmpty()) {
            responseMap = handlePrivateVisibility(privateList, esResult, esPrivateResult);
        }
        if (responseMap != null && !responseMap.isEmpty()) {
            Map<String, Object> privateDataMap =
                    (Map<String, Object>) responseMap.get(JsonKey.DATA);
            if (privateDataMap != null && privateDataMap.size() >= esPrivateResult.size()) {
                // this will indicate some extra private data is added
                esPrivateResult = privateDataMap;
                UserUtility.updateProfileVisibilityFields(privateDataMap, esResult);
            }
        }
        // now have a check for public field.
        if (publicList != null && !publicList.isEmpty()) {
            // this estype will hold all private data of user.
            // now collecting values from private filed and it will update
            // under original index with public field.
            for (String field : publicList) {
                if (esPrivateResult.containsKey(field)) {
                    esResult.put(field, esPrivateResult.get(field));
                    esPrivateResult.remove(field);
                } else {
                    ProjectLogger.log("field value not found inside private index ==" + field);
                }
            }
        }
        Map<String, String> privateFieldMap =
                (Map<String, String>) esResult.get(JsonKey.PROFILE_VISIBILITY);
        if (null == privateFieldMap) {
            privateFieldMap = new HashMap<>();
        }
        if (privateList != null) {
            for (String key : privateList) {
                privateFieldMap.put(key, JsonKey.PRIVATE);
            }
        }
        if (publicList != null) {
            for (String key : publicList) {
                privateFieldMap.remove(key);
            }
            updateCassandraWithPrivateFiled(userId, privateFieldMap);
            esResult.put(JsonKey.PROFILE_VISIBILITY, privateFieldMap);
        }
        if (privateFieldMap.size() > 0) {
            updateCassandraWithPrivateFiled(userId, privateFieldMap);
            esResult.put(JsonKey.PROFILE_VISIBILITY, privateFieldMap);
        }
        boolean updateResponse = true;
        updateResponse = updateDataInES(esResult, esPrivateResult, userId);
        Response response = new Response();
        if (updateResponse) {
            response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
        } else {
            response.put(JsonKey.RESPONSE, JsonKey.FAILURE);
        }
        sender().tell(response, self());

        targetObject =
                TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        Map<String, Object> telemetryAction = new HashMap<>();
        telemetryAction.put("profileVisibility", "profileVisibility");
        TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, new ArrayList<>());
    }

    private Map<String, Object> handlePrivateVisibility(List<String> privateFieldList,
            Map<String, Object> data, Map<String, Object> oldPrivateData) {
        Map<String, Object> privateFiledMap = createPrivateFiledMap(data, privateFieldList);
        privateFiledMap.putAll(oldPrivateData);
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.DATA, privateFiledMap);
        return map;
    }

    /**
     * This method will create a private field map and remove those filed from original map.
     * 
     * @param map Map<String, Object> complete save data Map
     * @param fields List<String> list of private fields
     * @return Map<String, Object> map of private field with their original values.
     */
    private Map<String, Object> createPrivateFiledMap(Map<String, Object> map,
            List<String> fields) {
        Map<String, Object> privateMap = new HashMap<>();
        if (fields != null && !fields.isEmpty()) {
            for (String field : fields) {
                /*
                 * now if field contains
                 * {address.someField,education.someField,jobprofile.someField} then we need to
                 * remove those filed
                 */
                if (field.contains(JsonKey.ADDRESS + ".")) {
                    privateMap.put(JsonKey.ADDRESS, map.get(JsonKey.ADDRESS));
                } else if (field.contains(JsonKey.EDUCATION + ".")) {
                    privateMap.put(JsonKey.EDUCATION, map.get(JsonKey.EDUCATION));
                } else if (field.contains(JsonKey.JOB_PROFILE + ".")) {
                    privateMap.put(JsonKey.JOB_PROFILE, map.get(JsonKey.JOB_PROFILE));
                } else if (field.contains(JsonKey.SKILLS + ".")) {
                    privateMap.put(JsonKey.SKILLS, map.get(JsonKey.SKILLS));
                } else {
                    if (!map.containsKey(field)) {
                        throw new ProjectCommonException(
                                ResponseCode.InvalidColumnError.getErrorCode(),
                                ResponseCode.InvalidColumnError.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                    }
                    privateMap.put(field, map.get(field));
                }
            }
        }
        return privateMap;
    }

    /**
     * THis methods will update user private field under cassandra.
     * 
     * @param userId Stirng
     * @param privateFieldMap Map<String,String>
     */
    private void updateCassandraWithPrivateFiled(String userId,
            Map<String, String> privateFieldMap) {
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String, Object> reqMap = new HashMap<>();
        reqMap.put(JsonKey.ID, userId);
        reqMap.put(JsonKey.PROFILE_VISIBILITY, privateFieldMap);
        Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), reqMap);
        String val = (String) response.get(JsonKey.RESPONSE);
        ProjectLogger.log("Private field updated under cassandra==" + val);
    }

    /**
     * This method will first removed the remove the saved private data for the user and then it
     * will create new private data for that user.
     * 
     * @param dataMap Map<String, Object> allData
     * @param privateDataMap Map<String, Object> only private data.
     * @param userId String
     * @return boolean
     */
    private boolean updateDataInES(Map<String, Object> dataMap, Map<String, Object> privateDataMap,
            String userId) {
        ElasticSearchUtil.createData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.userprofilevisibility.getTypeName(), userId, privateDataMap);
        ElasticSearchUtil.createData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName(), userId, dataMap);
        return true;
    }

    /**
     * This method will verify the loginId or email key against cassandra db. if user is found then
     * it will send temporary password to user register email.
     * 
     * @param actorMessage Request
     */
    private void forgotPassword(Request actorMessage) {
        Map<String, Object> map = (Map) actorMessage.getRequest().get(JsonKey.USER);
        String userName = (String) map.get(JsonKey.USERNAME);
        boolean isEmailvalid = ProjectUtil.isEmailvalid(userName);
        String searchedKey = "";
        if (isEmailvalid) {
            searchedKey = JsonKey.EMAIL;
        } else {
            searchedKey = JsonKey.USERNAME;
        }
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Response response = null;
        try {
            response = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),
                    usrDbInfo.getTableName(), searchedKey, encryptionService.encryptData(userName));
            if (response != null) {
                List<Map<String, Object>> list =
                        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
                if (list != null && list.size() == 1) {
                    Map<String, Object> userMap = list.get(0);
                    String email =
                            decryptionService.decryptData((String) userMap.get(JsonKey.EMAIL));
                    String name = (String) userMap.get(JsonKey.FIRST_NAME);
                    String userId = (String) userMap.get(JsonKey.USER_ID);
                    if (!StringUtils.isBlank(email)) {
                        response = new Response();
                        response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
                        sender().tell(response, self());
                        sendForgotPasswordEmail(name, email, userId);
                        return;
                    }
                }
            }
        } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
        }
        ProjectCommonException exception =
                new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
                        ResponseCode.userNotFound.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
    }

    /**
     * This method will update user current login time in keycloak
     * 
     * @param actorMessage Request
     */
    private void updateUserLoginTime(Request actorMessage) {
        String userId = (String) actorMessage.getRequest().get(JsonKey.USER_ID);
        Response response = new Response();
        response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
        sender().tell(response, self());
        if (Boolean
                .parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED))) {
            boolean addedResponse = ssoManager.addUserLoginTime(userId);
            ProjectLogger.log("user login time added response is ==" + addedResponse);

            // read value for emailVerified
            boolean emailVerified = ssoManager.isEmailVerified(userId);
            ProjectLogger.log("user emailVerified response ==" + emailVerified);
            String emailVerifiedUpdatedFlag = ssoManager.getEmailVerifiedUpdatedFlag(userId);
            ProjectLogger
                    .log("user emailVerifiedUpdatedFlag response ==" + emailVerifiedUpdatedFlag);
            if (emailVerified && "false".equalsIgnoreCase(emailVerifiedUpdatedFlag)) {
                Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
                Map<String, Object> map = new HashMap<>();
                map.put(JsonKey.ID, userId);
                map.put(JsonKey.EMAIL_VERIFIED, true);
                cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(),
                        map);
                // update user Ext Db
                updateUserAndExtIdTable(userId);
                ssoManager.setEmailVerifiedUpdatedFlag(userId, "true");
                boolean flag =
                        ElasticSearchUtil.updateData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                                ProjectUtil.EsType.user.getTypeName(), userId, map);
                if (flag) {
                    ProjectLogger.log(
                            "User data updated to ES for EMAIL_VERIFIED for userId :: " + userId);
                } else {
                    ProjectLogger
                            .log("User data update failed to ES for EMAIL_VERIFIED for userId :: "
                                    + userId);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void updateUserAndExtIdTable(String userId) {
        String email = "";
        String phone = "";
        Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Response usrResponse = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), userId);
        if (!(((List<Map<String, Object>>) usrResponse.get(JsonKey.RESPONSE)).isEmpty())) {
            Map<String, Object> dbusrMap =
                    ((List<Map<String, Object>>) usrResponse.get(JsonKey.RESPONSE)).get(0);
            email = (String) dbusrMap.get(JsonKey.EMAIL);
            phone = (String) dbusrMap.get(JsonKey.PHONE);
        }
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.USER_ID, userId);
        Response usrExtDbResponse = cassandraOperation
                .getRecordsByProperties(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), map);
        if (!(((List<Map<String, Object>>) usrExtDbResponse.get(JsonKey.RESPONSE)).isEmpty())) {
            List<Map<String, Object>> extDbResList =
                    ((List<Map<String, Object>>) usrExtDbResponse.get(JsonKey.RESPONSE));
            for (Map<String, Object> extDbRes : extDbResList) {
                if ((JsonKey.PHONE).equalsIgnoreCase((String) extDbRes.get(JsonKey.EXTERNAL_ID))
                        && !phone.equalsIgnoreCase(
                                (String) extDbRes.get(JsonKey.EXTERNAL_ID_VALUE))) {
                    cassandraOperation.deleteRecord(usrExtIdDb.getKeySpace(),
                            usrExtIdDb.getTableName(), (String) extDbRes.get(JsonKey.ID));
                }
                if ((JsonKey.EMAIL).equalsIgnoreCase((String) extDbRes.get(JsonKey.EXTERNAL_ID))) {
                    if (email.equalsIgnoreCase((String) extDbRes.get(JsonKey.EXTERNAL_ID_VALUE))) {
                        Map<String, Object> extDbmap = new HashMap<>();
                        extDbmap.put(JsonKey.ID, extDbRes.get(JsonKey.ID));
                        extDbmap.put(JsonKey.IS_VERIFIED, true);
                        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),
                                usrDbInfo.getTableName(), map);
                    } else if (!(email
                            .equalsIgnoreCase((String) extDbRes.get(JsonKey.EXTERNAL_ID_VALUE)))) {
                        cassandraOperation.deleteRecord(usrExtIdDb.getKeySpace(),
                                usrExtIdDb.getTableName(), (String) extDbRes.get(JsonKey.ID));
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void getUserDetailsByLoginId(Request actorMessage) {
        Map<String, Object> userMap =
                (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        if (null != userMap.get(JsonKey.LOGIN_ID)) {
            String loginId = (String) userMap.get(JsonKey.LOGIN_ID);
            try {
                loginId = encryptionService.encryptData((String) userMap.get(JsonKey.LOGIN_ID));
            } catch (Exception e) {
                ProjectCommonException exception = new ProjectCommonException(
                        ResponseCode.userDataEncryptionError.getErrorCode(),
                        ResponseCode.userDataEncryptionError.getErrorMessage(),
                        ResponseCode.SERVER_ERROR.getResponseCode());
                sender().tell(exception, self());
                return;
            }

            SearchDTO searchDto = new SearchDTO();
            Map<String, Object> filter = new HashMap<>();
            filter.put(JsonKey.LOGIN_ID, loginId);
            searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filter);
            Map<String, Object> esResponse = ElasticSearchUtil.complexSearch(searchDto,
                    ProjectUtil.EsIndex.sunbird.getIndexName(),
                    ProjectUtil.EsType.user.getTypeName());
            List<Map<String, Object>> userList =
                    (List<Map<String, Object>>) esResponse.get(JsonKey.CONTENT);
            Map<String, Object> result = null;
            if (null != userList && !userList.isEmpty()) {
                result = userList.get(0);
            } else {
                throw new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
                        ResponseCode.userNotFound.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
            if (result == null || result.size() == 0) {
                throw new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
                        ResponseCode.userNotFound.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }

            // check whether is_deletd true or false
            if (ProjectUtil.isNotNull(result) && result.containsKey(JsonKey.IS_DELETED)
                    && ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))
                    && (Boolean) result.get(JsonKey.IS_DELETED)) {
                throw new ProjectCommonException(ResponseCode.userAccountlocked.getErrorCode(),
                        ResponseCode.userAccountlocked.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
            fetchRootAndRegisterOrganisation(result);
            // having check for removing private filed from user , if call user and response
            // user data id is not same.
            String requestedById =
                    (String) actorMessage.getRequest().getOrDefault(JsonKey.REQUESTED_BY, "");
            ProjectLogger.log("requested By and requested user id == " + requestedById + "  "
                    + (String) result.get(JsonKey.USER_ID));
            // Decrypt user data
            UserUtility.decryptUserDataFrmES(result);
            try {
                if (!(((String) result.get(JsonKey.USER_ID)).equalsIgnoreCase(requestedById))) {
                    result = removeUserPrivateField(result);
                } else {
                    // If the user requests his data then we are fetching the private data from
                    // userprofilevisibility index
                    // and merge it with user index data
                    Map<String, Object> privateResult = ElasticSearchUtil.getDataByIdentifier(
                            ProjectUtil.EsIndex.sunbird.getIndexName(),
                            ProjectUtil.EsType.userprofilevisibility.getTypeName(),
                            (String) userMap.get(JsonKey.USER_ID));
                    UserUtility.decryptUserDataFrmES(privateResult);
                    result.putAll(privateResult);
                }
            } catch (Exception e) {
                ProjectCommonException exception = new ProjectCommonException(
                        ResponseCode.userDataEncryptionError.getErrorCode(),
                        ResponseCode.userDataEncryptionError.getErrorMessage(),
                        ResponseCode.SERVER_ERROR.getResponseCode());
                sender().tell(exception, self());
                return;
            }

            Response response = new Response();
            if (null != result) {
                // remove email and phone no from response
                result.remove(JsonKey.ENC_EMAIL);
                result.remove(JsonKey.ENC_PHONE);
                if (null != actorMessage.getRequest().get(JsonKey.FIELDS)) {
                    List<String> requestFields =
                            (List<String>) actorMessage.getRequest().get(JsonKey.FIELDS);
                    if (requestFields != null) {
                        if (!requestFields.contains(JsonKey.COMPLETENESS)) {
                            result.remove(JsonKey.COMPLETENESS);
                        }
                        if (!requestFields.contains(JsonKey.MISSING_FIELDS)) {
                            result.remove(JsonKey.MISSING_FIELDS);
                        }
                        if (requestFields.contains(JsonKey.LAST_LOGIN_TIME)) {
                            result.put(JsonKey.LAST_LOGIN_TIME,
                                    Long.parseLong(
                                            getLastLoginTime((String) userMap.get(JsonKey.USER_ID),
                                                    (String) result.get(JsonKey.LAST_LOGIN_TIME))));
                        }
                        if (!requestFields.contains(JsonKey.LAST_LOGIN_TIME)) {
                            result.remove(JsonKey.LAST_LOGIN_TIME);
                        }
                        if (requestFields.contains(JsonKey.TOPIC)) {
                            // fetch the topic details of all user associated orgs and append in the
                            // result
                            fetchTopicOfAssociatedOrgs(result);
                        }
                    } else {
                        result.remove(JsonKey.MISSING_FIELDS);
                        result.remove(JsonKey.COMPLETENESS);
                    }
                } else {
                    result.remove(JsonKey.MISSING_FIELDS);
                    result.remove(JsonKey.COMPLETENESS);
                }
                response.put(JsonKey.RESPONSE, result);
            } else {
                result = new HashMap<>();
                response.put(JsonKey.RESPONSE, result);
            }
            sender().tell(response, self());
            return;
        } else {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
                            ResponseCode.userNotFound.getErrorMessage(),
                            ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
            sender().tell(exception, self());
            return;
        }
    }

    private void fetchRootAndRegisterOrganisation(Map<String, Object> result) {
        try {
            if (isNotNull(result.get(JsonKey.ROOT_ORG_ID))) {

                String rootOrgId = (String) result.get(JsonKey.ROOT_ORG_ID);
                Map<String, Object> esResult = ElasticSearchUtil.getDataByIdentifier(
                        ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.organisation.getTypeName(), rootOrgId);
                result.put(JsonKey.ROOT_ORG, esResult);

            }
            if (isNotNull(result.get(JsonKey.REGISTERED_ORG_ID))) {

                String regOrgId = (String) result.get(JsonKey.REGISTERED_ORG_ID);
                Map<String, Object> esResult = ElasticSearchUtil.getDataByIdentifier(
                        ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.organisation.getTypeName(), regOrgId);
                result.put(JsonKey.REGISTERED_ORG, esResult != null ? esResult : new HashMap<>());
            }
        } catch (Exception ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        }
    }

    /**
     * Method to get the user profile .
     *
     * @param actorMessage Request
     */
    @SuppressWarnings("unchecked")
    private void getUserProfile(Request actorMessage) {
        Map<String, Object> userMap =
                (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        Map<String, Object> result = ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(),
                (String) userMap.get(JsonKey.USER_ID));
        // check user found or not
        if (result == null || result.size() == 0) {
            throw new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
                    ResponseCode.userNotFound.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        // check whether is_deletd true or false
        if (ProjectUtil.isNotNull(result) && result.containsKey(JsonKey.IS_DELETED)
                && ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))
                && (Boolean) result.get(JsonKey.IS_DELETED)) {
            throw new ProjectCommonException(ResponseCode.userAccountlocked.getErrorCode(),
                    ResponseCode.userAccountlocked.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        fetchRootAndRegisterOrganisation(result);
        // Decrypt user data
        UserUtility.decryptUserDataFrmES(result);
        // having check for removing private filed from user , if call user and response
        // user data id is not same.
        String requestedById =
                (String) actorMessage.getRequest().getOrDefault(JsonKey.REQUESTED_BY, "");
        ProjectLogger.log("requested By and requested user id == " + requestedById + "  "
                + (String) userMap.get(JsonKey.USER_ID));
        try {
            if (!((String) userMap.get(JsonKey.USER_ID)).equalsIgnoreCase(requestedById)) {
                result = removeUserPrivateField(result);
            } else {
                // If the user requests his data then we are fetching the private data from
                // userprofilevisibility index
                // and merge it with user index data
                Map<String, Object> privateResult = ElasticSearchUtil.getDataByIdentifier(
                        ProjectUtil.EsIndex.sunbird.getIndexName(),
                        ProjectUtil.EsType.userprofilevisibility.getTypeName(),
                        (String) userMap.get(JsonKey.USER_ID));
                UserUtility.decryptUserDataFrmES(privateResult);
                result.putAll(privateResult);
            }
        } catch (Exception e) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userDataEncryptionError.getErrorCode(),
                            ResponseCode.userDataEncryptionError.getErrorMessage(),
                            ResponseCode.SERVER_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        if (null != actorMessage.getRequest().get(JsonKey.FIELDS)) {
            String requestFields = (String) actorMessage.getRequest().get(JsonKey.FIELDS);
            if (!StringUtils.isBlank(requestFields)) {
                if (!requestFields.contains(JsonKey.COMPLETENESS)) {
                    result.remove(JsonKey.COMPLETENESS);
                }
                if (!requestFields.contains(JsonKey.MISSING_FIELDS)) {
                    result.remove(JsonKey.MISSING_FIELDS);
                }
                if (requestFields.contains(JsonKey.LAST_LOGIN_TIME)) {
                    result.put(JsonKey.LAST_LOGIN_TIME,
                            Long.parseLong(getLastLoginTime((String) userMap.get(JsonKey.USER_ID),
                                    (String) result.get(JsonKey.LAST_LOGIN_TIME))));
                }
                if (!requestFields.contains(JsonKey.LAST_LOGIN_TIME)) {
                    result.remove(JsonKey.LAST_LOGIN_TIME);
                }
                if (requestFields.contains(JsonKey.TOPIC)) {
                    // fetch the topic details of all user associated orgs and append in the result
                    fetchTopicOfAssociatedOrgs(result);
                }
            }
        } else {
            result.remove(JsonKey.MISSING_FIELDS);
            result.remove(JsonKey.COMPLETENESS);
        }
        Response response = new Response();
        if (null != result) {
            result.remove(JsonKey.ENC_EMAIL);
            result.remove(JsonKey.ENC_PHONE);
            response.put(JsonKey.RESPONSE, result);
        } else {
            result = new HashMap<>();
            response.put(JsonKey.RESPONSE, result);
        }
        sender().tell(response, self());
    }

    private void fetchTopicOfAssociatedOrgs(Map<String, Object> result) {

        String userId = (String) result.get(JsonKey.ID);
        Map<String, Object> locationCache = new HashMap<>();
        Set<String> topicSet = new HashSet<>();

        // fetch all associated user orgs
        Response response1 = cassandraOperation.getRecordsByProperty(userOrgDbInfo.getKeySpace(),
                userOrgDbInfo.getTableName(), JsonKey.USER_ID, userId);

        List<Map<String, Object>> list =
                (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);

        List<String> orgIdsList = new ArrayList<>();
        if (!list.isEmpty()) {

            for (Map<String, Object> m : list) {
                String orgId = (String) m.get(JsonKey.ORGANISATION_ID);
                orgIdsList.add(orgId);
            }

            // fetch all org details from elasticsearch ...
            if (!orgIdsList.isEmpty()) {

                Map<String, Object> filters = new HashMap<>();
                filters.put(JsonKey.ID, orgIdsList);

                List<String> orgfields = new ArrayList<>();
                orgfields.add(JsonKey.ID);
                orgfields.add(JsonKey.LOCATION_ID);

                SearchDTO searchDTO = new SearchDTO();
                searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
                searchDTO.setFields(orgfields);

                Map<String, Object> esresult = ElasticSearchUtil.complexSearch(searchDTO,
                        ProjectUtil.EsIndex.sunbird.getIndexName(),
                        EsType.organisation.getTypeName());
                List<Map<String, Object>> esContent =
                        (List<Map<String, Object>>) esresult.get(JsonKey.CONTENT);

                if (!esContent.isEmpty()) {
                    for (Map<String, Object> m : esContent) {
                        if (!StringUtils.isBlank((String) m.get(JsonKey.LOCATION_ID))) {
                            String locationId = (String) m.get(JsonKey.LOCATION_ID);
                            if (locationCache.containsKey(locationId)) {
                                topicSet.add((String) locationCache.get(locationId));
                            } else {
                                // get the location id info from db and set to the cacche and
                                // topicSet
                                Response response3 = cassandraOperation.getRecordById(
                                        geoLocationDbInfo.getKeySpace(),
                                        geoLocationDbInfo.getTableName(), locationId);
                                List<Map<String, Object>> list3 =
                                        (List<Map<String, Object>>) response3.get(JsonKey.RESPONSE);
                                if (!list3.isEmpty()) {
                                    Map<String, Object> locationInfoMap = list3.get(0);
                                    String topic = (String) locationInfoMap.get(JsonKey.TOPIC);
                                    topicSet.add(topic);
                                    locationCache.put(locationId, topic);
                                }

                            }
                        }
                    }
                }
            }
        }
        result.put(JsonKey.TOPICS, topicSet);
    }

    /**
     * Method to change the user password .
     *
     * @param actorMessage Request
     */
    @SuppressWarnings("unchecked")
    private void changePassword(Request actorMessage) {
        Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String, Object> userMap =
                (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        String currentPassword = (String) userMap.get(JsonKey.PASSWORD);
        String newPassword = (String) userMap.get(JsonKey.NEW_PASSWORD);
        Response result = cassandraOperation.getRecordById(userDbInfo.getKeySpace(),
                userDbInfo.getTableName(), (String) userMap.get(JsonKey.USER_ID));
        List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (!(list.isEmpty())) {
            Map<String, Object> resultMap = list.get(0);
            boolean passwordMatched = ((String) resultMap.get(JsonKey.PASSWORD))
                    .equals(OneWayHashing.encryptVal(currentPassword));
            if (passwordMatched) {
                // update the new password
                String newHashedPassword = OneWayHashing.encryptVal(newPassword);
                Map<String, Object> queryMap = new LinkedHashMap<>();
                queryMap.put(JsonKey.ID, userMap.get(JsonKey.USER_ID));
                queryMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
                queryMap.put(JsonKey.UPDATED_BY,
                        actorMessage.getRequest().get(JsonKey.REQUESTED_BY));
                queryMap.put(JsonKey.PASSWORD, newHashedPassword);
                queryMap.put(JsonKey.TEMPORARY_PASSWORD, "");
                result = cassandraOperation.updateRecord(userDbInfo.getKeySpace(),
                        userDbInfo.getTableName(), queryMap);
                sender().tell(result, self());
            } else {
                ProjectCommonException exception =
                        new ProjectCommonException(ResponseCode.invalidCredentials.getErrorCode(),
                                ResponseCode.invalidCredentials.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
            }
        }
    }

    /**
     * Method to Logout the user , once logout successfully it will delete the AuthToken from DB.
     *
     * @param actorMessage Request
     */
    private void logout(Request actorMessage) {
        Util.DbInfo userAuthDbInfo = Util.dbInfoMap.get(JsonKey.USER_AUTH_DB);
        String authToken = (String) actorMessage.getRequest().get(JsonKey.AUTH_TOKEN);

        Response result = cassandraOperation.deleteRecord(userAuthDbInfo.getKeySpace(),
                userAuthDbInfo.getTableName(), authToken);

        result.put(Constants.RESPONSE, JsonKey.SUCCESS);
        sender().tell(result, self());
    }

    /**
     * Method to Login the user by taking Username and password , once login successful it will
     * create Authtoken and return the token. user can login from multiple source at a time for
     * example web, app,etc but for a same source user cann't be login from different machine, for
     * example if user is trying to login from a source from two different machine we are
     * invalidating the auth token of previous machine and creating a new auth token for new machine
     * and sending that auth token with response
     *
     * @param actorMessage Request
     */
    @SuppressWarnings("unchecked")
    private void login(Request actorMessage) throws Exception {
        Map<String, Object> reqMap =
                (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        String data = (String) reqMap.get(JsonKey.USERNAME);
        Response result = null;
        if (ProjectUtil.isEmailvalid(data)) {
            result = loginWithEmail(data);
        } else if (ProjectUtil.validatePhoneNumber(data)) {
            result = loginWithPhone(data);
        } else {
            result = loginWithLoginId(data);
        }
        List<Map<String, Object>> list = ((List<Map<String, Object>>) result.get(JsonKey.RESPONSE));
        if (null != list && list.size() == 1) {
            Map<String, Object> resultMap = list.get(0);
            boolean isChangePasswordReqquired = false;
            if (null != resultMap.get(JsonKey.STATUS) && (ProjectUtil.Status.ACTIVE
                    .getValue()) == (int) resultMap.get(JsonKey.STATUS)) {
                if (StringUtils.isBlank(((String) reqMap.get(JsonKey.LOGIN_TYPE)))) {
                    // here login type is general
                    boolean password = ((String) resultMap.get(JsonKey.PASSWORD)).equals(
                            OneWayHashing.encryptVal((String) reqMap.get(JsonKey.PASSWORD)));
                    if (((String) resultMap.get(JsonKey.PASSWORD))
                            .equals(resultMap.get(JsonKey.TEMPORARY_PASSWORD))) {
                        isChangePasswordReqquired = true;
                    }
                    if (password) {
                        Map<String, Object> userAuthMap = new HashMap<>();
                        userAuthMap.put(JsonKey.SOURCE, reqMap.get(JsonKey.SOURCE));
                        userAuthMap.put(JsonKey.USER_ID, resultMap.get(JsonKey.ID));
                        userAuthMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());

                        String userAuth =
                                ProjectUtil.createAuthToken((String) resultMap.get(JsonKey.ID),
                                        (String) reqMap.get(JsonKey.SOURCE));
                        userAuthMap.put(JsonKey.ID, userAuth);
                        checkForDuplicateUserAuthToken(userAuthMap, resultMap, reqMap);
                        reqMap.remove(JsonKey.PASSWORD);
                        reqMap.remove(JsonKey.USERNAME);
                        reqMap.remove(JsonKey.SOURCE);
                        reqMap.put(JsonKey.FIRST_NAME, resultMap.get(JsonKey.FIRST_NAME));
                        reqMap.put(JsonKey.ACCESSTOKEN, userAuthMap.get(JsonKey.ID));
                        reqMap.put(JsonKey.USER_ID, resultMap.get(JsonKey.USER_ID));
                        if (isChangePasswordReqquired) {
                            reqMap.put(JsonKey.STATUS_CODE,
                                    ResponseCode.REDIRECTION_REQUIRED.getResponseCode());
                        }
                        Response response = new Response();
                        response.put(JsonKey.RESPONSE, reqMap);
                        sender().tell(response, self());
                        updateUserLoginTime(resultMap);
                    } else {
                        ProjectCommonException exception = new ProjectCommonException(
                                ResponseCode.invalidCredentials.getErrorCode(),
                                ResponseCode.invalidCredentials.getErrorMessage(),
                                ResponseCode.UNAUTHORIZED.getResponseCode());
                        sender().tell(exception, self());
                    }
                } else {
                    // for other login type operation
                    ProjectCommonException exception =
                            new ProjectCommonException(ResponseCode.loginTypeError.getErrorCode(),
                                    ResponseCode.loginTypeError.getErrorMessage(),
                                    ResponseCode.CLIENT_ERROR.getResponseCode());
                    sender().tell(exception, self());
                }
            } else {
                ProjectCommonException exception =
                        new ProjectCommonException(ResponseCode.invalidCredentials.getErrorCode(),
                                ResponseCode.invalidCredentials.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
            }
        } else {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidCredentials.getErrorCode(),
                            ResponseCode.invalidCredentials.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
        }
    }

    /**
     * 
     * @param email
     * @return Response
     * @throws Exception
     */
    private Response loginWithEmail(String email) throws Exception {
        Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        return cassandraOperation.getRecordsByProperty(userDbInfo.getKeySpace(),
                userDbInfo.getTableName(), JsonKey.EMAIL, encryptionService.encryptData(email));
    }

    /**
     * 
     * @param phone
     * @return Response
     * @throws Exception
     */
    private Response loginWithPhone(String phone) throws Exception {
        Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Response resposne = null;
        try {
            resposne = cassandraOperation.getRecordsByProperty(userDbInfo.getKeySpace(),
                    userDbInfo.getTableName(), JsonKey.PHONE, encryptionService.encryptData(phone));
        } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
        }
        return resposne;
    }

    /**
     * 
     * @param loginId
     * @return Response
     * @throws Exception
     */
    private Response loginWithLoginId(String loginId) throws Exception {
        Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        return cassandraOperation.getRecordsByProperty(userDbInfo.getKeySpace(),
                userDbInfo.getTableName(), JsonKey.LOGIN_ID,
                encryptionService.encryptData(loginId));
    }

    /**
     * Method to update the user profile.
     */
    @SuppressWarnings("unchecked")
    private void updateUser(Request actorMessage) {
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String, Object> req = actorMessage.getRequest();
        Map<String, Object> requestMap = null;
        Map<String, Object> userMap = (Map<String, Object>) req.get(JsonKey.USER);
        if (null != userMap.get(JsonKey.USER_ID)) {
            userMap.put(JsonKey.ID, userMap.get(JsonKey.USER_ID));
        } else {
            userMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
        }
        if (isUserDeleted(userMap)) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.inactiveUser.getErrorCode(),
                            ResponseCode.inactiveUser.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        // object of telemetry event...
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();

        if (userMap.containsKey(JsonKey.WEB_PAGES)) {
            SocialMediaType.validateSocialMedia(
                    (List<Map<String, String>>) userMap.get(JsonKey.WEB_PAGES));
        }


        checkPhoneUniqueness(userMap, JsonKey.UPDATE);
        checkEmailUniqueness(userMap, JsonKey.UPDATE);

        /**
         * Ignore all roles coming from req. With update user api, we are not allowing to update
         * user roles
         */
        userMap.remove(JsonKey.ROLES);

        // not allowing user to update the status,provider,userName
        removeFieldsFrmReq(userMap);

        if (!StringUtils.isBlank((String) userMap.get(JsonKey.EMAIL))) {
            boolean flag = checkEmailSameOrDiff(userMap);
            if (flag) {
                userMap.remove(JsonKey.EMAIL);
            }
        }

        if (isSSOEnabled) {
            updateKeyCloakUserBase(userMap);
        }
        userMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        userMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
        try {
            UserUtility.encryptUserData(userMap);
        } catch (Exception e1) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userDataEncryptionError.getErrorCode(),
                            ResponseCode.userDataEncryptionError.getErrorMessage(),
                            ResponseCode.SERVER_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        requestMap = new HashMap<>();
        requestMap.putAll(userMap);
        removeUnwanted(requestMap);
        Response result = null;
        try {
            result = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),
                    usrDbInfo.getTableName(), requestMap);
        } catch (Exception ex) {
            sender().tell(ex, self());
            return;
        }
        // update user address
        if (userMap.containsKey(JsonKey.ADDRESS)) {
            updateUserAddress(req, userMap);
        }
        if (userMap.containsKey(JsonKey.EDUCATION)) {
            updateUserEducation(req, userMap);
        }
        if (userMap.containsKey(JsonKey.JOB_PROFILE)) {
            updateUserJobProfile(req, userMap);
        }

        updateUserExtId(requestMap);
        sender().tell(result, self());

        targetObject = TelemetryUtil.generateTargetObject((String) userMap.get(JsonKey.USER_ID),
                JsonKey.USER, JsonKey.UPDATE, null);
        TelemetryUtil.telemetryProcessingCall(actorMessage.getRequest(), targetObject,
                correlatedObject);

        if (((String) result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
            Request userRequest = new Request();
            userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
            userRequest.getRequest().put(JsonKey.ID, userMap.get(JsonKey.ID));
            try {
                tellToAnother(userRequest);
            } catch (Exception ex) {
                ProjectLogger.log(
                        "Exception Occured during saving user to Es while updating user : ", ex);
            }
        }
    }

    private boolean isUserDeleted(Map<String, Object> userMap) {
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Response response = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), (String) userMap.get(JsonKey.ID));
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

    private void removeFieldsFrmReq(Map<String, Object> userMap) {
        userMap.remove(JsonKey.ENC_EMAIL);
        userMap.remove(JsonKey.ENC_PHONE);
        userMap.remove(JsonKey.EMAIL_VERIFIED);
        userMap.remove(JsonKey.STATUS);
        userMap.remove(JsonKey.PROVIDER);
        userMap.remove(JsonKey.USERNAME);
        userMap.remove(JsonKey.REGISTERED_ORG_ID);
        userMap.remove(JsonKey.ROOT_ORG_ID);
        userMap.remove(JsonKey.LOGIN_ID);
    }

    private void updateUserJobProfile(Map<String, Object> req, Map<String, Object> userMap) {
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
        List<Map<String, Object>> reqList =
                (List<Map<String, Object>>) userMap.get(JsonKey.JOB_PROFILE);
        for (Map<String, Object> reqMap : reqList) {
            if (reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED)
                    && ((boolean) reqMap.get(JsonKey.IS_DELETED))
                    && !StringUtils.isBlank((String) reqMap.get(JsonKey.ID))) {
                String addrsId = null;
                if (reqMap.containsKey(JsonKey.ADDRESS) && null != reqMap.get(JsonKey.ADDRESS)) {
                    addrsId = (String) ((Map<String, Object>) reqMap.get(JsonKey.ADDRESS))
                            .get(JsonKey.ID);
                    deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
                } else {
                    addrsId = getAddressId((String) reqMap.get(JsonKey.ID), jobProDbInfo);
                    if (null != addrsId) {
                        deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
                    }
                }
                deleteRecord(jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(),
                        (String) reqMap.get(JsonKey.ID));
                continue;
            }
            processJobProfileInfo(reqMap, userMap, req, addrDbInfo, jobProDbInfo);
        }
    }

    private void updateUserEducation(Map<String, Object> req, Map<String, Object> userMap) {
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
        List<Map<String, Object>> reqList =
                (List<Map<String, Object>>) userMap.get(JsonKey.EDUCATION);
        for (int i = 0; i < reqList.size(); i++) {
            Map<String, Object> reqMap = reqList.get(i);
            if (reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED)
                    && ((boolean) reqMap.get(JsonKey.IS_DELETED))
                    && !StringUtils.isBlank((String) reqMap.get(JsonKey.ID))) {
                String addrsId = null;
                if (reqMap.containsKey(JsonKey.ADDRESS) && null != reqMap.get(JsonKey.ADDRESS)) {
                    addrsId = (String) ((Map<String, Object>) reqMap.get(JsonKey.ADDRESS))
                            .get(JsonKey.ID);
                    deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
                } else {
                    addrsId = getAddressId((String) reqMap.get(JsonKey.ID), eduDbInfo);
                    if (null != addrsId) {
                        deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
                    }
                }
                deleteRecord(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(),
                        (String) reqMap.get(JsonKey.ID));
                continue;
            }
            processEducationInfo(reqMap, userMap, req, addrDbInfo, eduDbInfo);
        }
    }

    private void updateUserAddress(Map<String, Object> req, Map<String, Object> userMap) {
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        List<Map<String, Object>> reqList =
                (List<Map<String, Object>>) userMap.get(JsonKey.ADDRESS);
        for (int i = 0; i < reqList.size(); i++) {
            Map<String, Object> reqMap = reqList.get(i);
            if (reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED)
                    && ((boolean) reqMap.get(JsonKey.IS_DELETED))
                    && !StringUtils.isBlank((String) reqMap.get(JsonKey.ID))) {
                deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(),
                        (String) reqMap.get(JsonKey.ID));
                continue;
            }
            processUserAddress(reqMap, req, userMap, addrDbInfo);

        }
    }

    private boolean checkEmailSameOrDiff(Map<String, Object> userMap) {
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Response response = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), (String) userMap.get(JsonKey.ID));
        List<Map<String, Object>> resList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if (!resList.isEmpty()) {
            Map<String, Object> res = resList.get(0);
            String email = (String) res.get(JsonKey.EMAIL);
            String encEmail = (String) userMap.get(JsonKey.EMAIL);
            try {
                encEmail = encryptionService.encryptData((String) userMap.get(JsonKey.EMAIL));
            } catch (Exception ex) {
                ProjectLogger.log("Exception occurred while encrypting user email.");
            }
            return ((encEmail).equalsIgnoreCase(email));
        }
        return false;
    }

    private void processUserAddress(Map<String, Object> reqMap, Map<String, Object> req,
            Map<String, Object> userMap, DbInfo addrDbInfo) {
        String encUserId = "";
        String encreqById = "";
        try {
            encUserId = encryptionService.encryptData((String) userMap.get(JsonKey.ID));
            encreqById = encryptionService.encryptData((String) req.get(JsonKey.REQUESTED_BY));
        } catch (Exception e1) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userDataEncryptionError.getErrorCode(),
                            ResponseCode.userDataEncryptionError.getErrorMessage(),
                            ResponseCode.SERVER_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        if (!reqMap.containsKey(JsonKey.ID)) {
            reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
            reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            reqMap.put(JsonKey.CREATED_BY, encreqById);
            reqMap.put(JsonKey.USER_ID, encUserId);
        } else {
            reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            reqMap.put(JsonKey.UPDATED_BY, encreqById);
            reqMap.remove(JsonKey.USER_ID);
        }
        try {
            cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(),
                    reqMap);
        } catch (Exception ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    private String getAddressId(String id, DbInfo dbInfo) {
        String addressId = null;
        try {
            Response res = cassandraOperation.getPropertiesValueById(dbInfo.getKeySpace(),
                    dbInfo.getTableName(), id, JsonKey.ADDRESS_ID);
            if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
                addressId =
                        (String) (((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0))
                                .get(JsonKey.ADDRESS_ID);
            }
        } catch (Exception ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        }
        return addressId;
    }

    private void deleteRecord(String keyspaceName, String tableName, String id) {
        try {
            cassandraOperation.deleteRecord(keyspaceName, tableName, id);
        } catch (Exception ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        }
    }

    private void processJobProfileInfo(Map<String, Object> reqMap, Map<String, Object> userMap,
            Map<String, Object> req, DbInfo addrDbInfo, DbInfo jobProDbInfo) {
        String addrId = null;
        Response addrResponse = null;
        if (reqMap.containsKey(JsonKey.ADDRESS)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
            if (!address.containsKey(JsonKey.ID)) {
                addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
                address.put(JsonKey.ID, addrId);
                address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
            } else {
                addrId = (String) address.get(JsonKey.ID);
                address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
                address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
                address.remove(JsonKey.USER_ID);
            }
            try {
                addrResponse = cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),
                        addrDbInfo.getTableName(), address);
            } catch (Exception ex) {
                ProjectLogger.log(ex.getMessage(), ex);
            }
        }
        if (null != addrResponse && ((String) addrResponse.get(JsonKey.RESPONSE))
                .equalsIgnoreCase(JsonKey.SUCCESS)) {
            reqMap.put(JsonKey.ADDRESS_ID, addrId);
            reqMap.remove(JsonKey.ADDRESS);
        }

        if (reqMap.containsKey(JsonKey.ID)) {
            reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
            reqMap.remove(JsonKey.USER_ID);
        } else {
            reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
            reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
            reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
        }
        try {
            cassandraOperation.upsertRecord(jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(),
                    reqMap);
        } catch (Exception ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        }
    }

    private void processEducationInfo(Map<String, Object> reqMap, Map<String, Object> userMap,
            Map<String, Object> req, DbInfo addrDbInfo, DbInfo eduDbInfo) {

        String addrId = null;
        Response addrResponse = null;
        if (reqMap.containsKey(JsonKey.ADDRESS)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
            if (!address.containsKey(JsonKey.ID)) {
                addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
                address.put(JsonKey.ID, addrId);
                address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
            } else {
                addrId = (String) address.get(JsonKey.ID);
                address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
                address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
                address.remove(JsonKey.USER_ID);
            }
            try {
                addrResponse = cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),
                        addrDbInfo.getTableName(), address);
            } catch (Exception ex) {
                ProjectLogger.log(ex.getMessage(), ex);
            }
        }
        if (null != addrResponse && ((String) addrResponse.get(JsonKey.RESPONSE))
                .equalsIgnoreCase(JsonKey.SUCCESS)) {
            reqMap.put(JsonKey.ADDRESS_ID, addrId);
            reqMap.remove(JsonKey.ADDRESS);
        }
        try {
            if (null != reqMap.get(JsonKey.YEAR_OF_PASSING)) {
                reqMap.put(JsonKey.YEAR_OF_PASSING,
                        ((BigInteger) reqMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
            } else {
                reqMap.put(JsonKey.YEAR_OF_PASSING, 0);
            }
        } catch (Exception ex) {
            reqMap.put(JsonKey.YEAR_OF_PASSING, 0);
            ProjectLogger.log(ex.getMessage(), ex);
        }
        try {
            if (null != reqMap.get(JsonKey.PERCENTAGE)) {
                reqMap.put(JsonKey.PERCENTAGE,
                        Double.parseDouble(String.valueOf(reqMap.get(JsonKey.PERCENTAGE))));
            } else {
                reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf("0")));
            }
        } catch (Exception ex) {
            reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf("0")));
            ProjectLogger.log(ex.getMessage(), ex);
        }

        if (reqMap.containsKey(JsonKey.ID)) {
            reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
            reqMap.remove(JsonKey.USER_ID);
        } else {
            reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
            reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
            reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
        }
        try {
            cassandraOperation.upsertRecord(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(),
                    reqMap);
        } catch (Exception ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        }

    }

    private void updateKeyCloakUserBase(Map<String, Object> userMap) {
        try {
            String userId = ssoManager.updateUser(userMap);
            if (!(!StringUtils.isBlank(userId) && userId.equalsIgnoreCase(JsonKey.SUCCESS))) {
                throw new ProjectCommonException(
                        ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
                        ResponseCode.userUpdationUnSuccessfull.getErrorMessage(),
                        ResponseCode.SERVER_ERROR.getResponseCode());
            } else if (!StringUtils.isBlank((String) userMap.get(JsonKey.EMAIL))) {
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

    /**
     * Method to create the new user , Username should be unique .
     *
     * @param actorMessage Request
     */
    @SuppressWarnings("unchecked")
    private void createUser(Request actorMessage) {
        ProjectLogger.log("create user method started..");
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo orgDb = Util.dbInfoMap.get(JsonKey.ORG_DB);
        ProjectLogger.log("collected all the DB setup..");
        Map<String, Object> req = actorMessage.getRequest();
        Map<String, Object> requestMap = null;
        Map<String, Object> userMap = (Map<String, Object>) req.get(JsonKey.USER);
        checkPhoneUniqueness(userMap, JsonKey.CREATE);
        checkEmailUniqueness(userMap, JsonKey.CREATE);
        Map<String, Object> emailTemplateMap = new HashMap<>(userMap);
        if (userMap.containsKey(JsonKey.WEB_PAGES)) {
            SocialMediaType.validateSocialMedia(
                    (List<Map<String, String>>) userMap.get(JsonKey.WEB_PAGES));
        }
        userMap.put(JsonKey.CREATED_BY, req.get(JsonKey.REQUESTED_BY));
        // remove these fields from req
        userMap.remove(JsonKey.ENC_EMAIL);
        userMap.remove(JsonKey.ENC_PHONE);
        userMap.remove(JsonKey.EMAIL_VERIFIED);

        if (userMap.containsKey(JsonKey.PROVIDER)
                && !StringUtils.isBlank((String) userMap.get(JsonKey.PROVIDER))) {
            userMap.put(JsonKey.LOGIN_ID, (String) userMap.get(JsonKey.USERNAME) + "@"
                    + (String) userMap.get(JsonKey.PROVIDER));
        } else {
            userMap.put(JsonKey.LOGIN_ID, userMap.get(JsonKey.USERNAME));
        }
        emailTemplateMap.put(JsonKey.USERNAME, userMap.get(JsonKey.LOGIN_ID));

        if (null != userMap.get(JsonKey.LOGIN_ID)) {
            String loginId = "";
            try {
                loginId = encryptionService.encryptData((String) userMap.get(JsonKey.LOGIN_ID));
            } catch (Exception e) {
                ProjectCommonException exception = new ProjectCommonException(
                        ResponseCode.userDataEncryptionError.getErrorCode(),
                        ResponseCode.userDataEncryptionError.getErrorMessage(),
                        ResponseCode.SERVER_ERROR.getResponseCode());
                sender().tell(exception, self());
                return;
            }
            Response resultFrUserName = cassandraOperation.getRecordsByProperty(
                    usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), JsonKey.LOGIN_ID, loginId);
            if (!(((List<Map<String, Object>>) resultFrUserName.get(JsonKey.RESPONSE)).isEmpty())) {
                ProjectCommonException exception =
                        new ProjectCommonException(ResponseCode.userAlreadyExist.getErrorCode(),
                                ResponseCode.userAlreadyExist.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
                return;
            }
        }
        // validate root org and reg org
        userMap.put(JsonKey.ROOT_ORG_ID, JsonKey.DEFAULT_ROOT_ORG_ID);
        if (!StringUtils.isBlank((String) userMap.get(JsonKey.REGISTERED_ORG_ID))) {
            validateRegAndRootOrg(userMap);
        } else {
            String provider = (String) userMap.get(JsonKey.PROVIDER);
            String rootOrgId = Util.getRootOrgIdFromChannel(provider);
            if (!StringUtils.isBlank(rootOrgId)) {
                userMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
            }
        }
        /**
         * will ignore roles coming from req, Only public role is applicable for user by default
         */
        userMap.remove(JsonKey.ROLES);
        List<String> roles = new ArrayList<>();
        roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
        userMap.put(JsonKey.ROLES, roles);

        String accessToken = "";
        if (isSSOEnabled) {
            try {
                String userId = "";
                Map<String, String> responseMap = ssoManager.createUser(userMap);
                userId = responseMap.get(JsonKey.USER_ID);
                accessToken = responseMap.get(JsonKey.ACCESSTOKEN);
                if (!StringUtils.isBlank(userId)) {
                    userMap.put(JsonKey.USER_ID, userId);
                    userMap.put(JsonKey.ID, userId);
                } else {
                    ProjectCommonException exception = new ProjectCommonException(
                            ResponseCode.userRegUnSuccessfull.getErrorCode(),
                            ResponseCode.userRegUnSuccessfull.getErrorMessage(),
                            ResponseCode.SERVER_ERROR.getResponseCode());
                    sender().tell(exception, self());
                    return;
                }
            } catch (Exception exception) {
                ProjectLogger.log(exception.getMessage(), exception);
                sender().tell(exception, self());
                return;
            }
        } else {
            userMap.put(JsonKey.USER_ID,
                    OneWayHashing.encryptVal((String) userMap.get(JsonKey.USERNAME)));
            userMap.put(JsonKey.ID,
                    OneWayHashing.encryptVal((String) userMap.get(JsonKey.USERNAME)));
        }

        userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());

        if (!StringUtils.isBlank((String) userMap.get(JsonKey.PASSWORD))) {
            emailTemplateMap.put(JsonKey.TEMPORARY_PASSWORD, userMap.get(JsonKey.PASSWORD));
            userMap.put(JsonKey.PASSWORD,
                    OneWayHashing.encryptVal((String) userMap.get(JsonKey.PASSWORD)));
        } else {
            // create tempPassword
            String tempPassword = ProjectUtil.generateRandomPassword();
            userMap.put(JsonKey.PASSWORD, OneWayHashing.encryptVal(tempPassword));
            emailTemplateMap.put(JsonKey.TEMPORARY_PASSWORD, tempPassword);
        }
        try {
            UserUtility.encryptUserData(userMap);
        } catch (Exception e1) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userDataEncryptionError.getErrorCode(),
                            ResponseCode.userDataEncryptionError.getErrorMessage(),
                            ResponseCode.SERVER_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        requestMap = new HashMap<>();
        requestMap.putAll(userMap);
        removeUnwanted(requestMap);
        // update db with emailVerified as false (default)
        requestMap.put(JsonKey.EMAIL_VERIFIED, false);

        Map<String, String> profileVisbility = new HashMap<>();
        for (String field : ProjectUtil.defaultPrivateFields) {
            profileVisbility.put(field, JsonKey.PRIVATE);
        }
        requestMap.put(JsonKey.PROFILE_VISIBILITY, profileVisbility);
        if (!StringUtils.isBlank((String) requestMap.get(JsonKey.COUNTRY_CODE))) {
            requestMap.put(JsonKey.COUNTRY_CODE,
                    propertiesCache.getProperty("sunbird_default_country_code"));
        }
        requestMap.put(JsonKey.IS_DELETED, false);
        Response response = null;
        try {
            response = cassandraOperation.insertRecord(usrDbInfo.getKeySpace(),
                    usrDbInfo.getTableName(), requestMap);
        } catch (ProjectCommonException exception) {
            sender().tell(exception, self());
            return;
        } finally {
            if (null == response && isSSOEnabled) {
                ssoManager.removeUser(userMap);
            }
        }
        response.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
        if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
            if (userMap.containsKey(JsonKey.ADDRESS)) {
                List<Map<String, Object>> reqList =
                        (List<Map<String, Object>>) userMap.get(JsonKey.ADDRESS);
                for (int i = 0; i < reqList.size(); i++) {
                    Map<String, Object> reqMap = reqList.get(i);
                    reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i + 1));
                    reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                    String encUserId = "";
                    String encCreatedById = "";
                    try {
                        encUserId = encryptionService.encryptData((String) userMap.get(JsonKey.ID));
                        encCreatedById = encryptionService
                                .encryptData((String) userMap.get(JsonKey.CREATED_BY));
                    } catch (Exception e) {
                        ProjectCommonException exception = new ProjectCommonException(
                                ResponseCode.userDataEncryptionError.getErrorCode(),
                                ResponseCode.userDataEncryptionError.getErrorMessage(),
                                ResponseCode.SERVER_ERROR.getResponseCode());
                        sender().tell(exception, self());
                        return;
                    }
                    reqMap.put(JsonKey.CREATED_BY, encCreatedById);
                    reqMap.put(JsonKey.USER_ID, encUserId);
                    try {
                        cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),
                                addrDbInfo.getTableName(), reqMap);
                    } catch (Exception e) {
                        ProjectLogger.log(e.getMessage(), e);
                    }
                }
            }
            ProjectLogger.log("User insertation on DB started--.....");
            if (userMap.containsKey(JsonKey.EDUCATION)) {
                insertEducationDetails(userMap);
                ProjectLogger.log("User insertation for Education done--.....");
            }
            if (userMap.containsKey(JsonKey.JOB_PROFILE)) {
                insertJobProfileDetails(userMap);
                ProjectLogger.log("User insertation for Job profile done--.....");
            }
            if (!StringUtils.isBlank((String) userMap.get(JsonKey.REGISTERED_ORG_ID))) {
                Response orgResponse = null;
                try {
                    orgResponse = cassandraOperation.getRecordById(orgDb.getKeySpace(),
                            orgDb.getTableName(), (String) userMap.get(JsonKey.REGISTERED_ORG_ID));
                } catch (Exception e) {
                    ProjectLogger.log(
                            "Exception occured while verifying regOrgId during create user : ", e);
                }
                if (null != orgResponse
                        && (!((List<Map<String, Object>>) orgResponse.get(JsonKey.RESPONSE))
                                .isEmpty())) {
                    insertOrganisationDetails(userMap);
                } else {
                    ProjectLogger
                            .log("Reg Org Id :" + (String) userMap.get(JsonKey.REGISTERED_ORG_ID)
                                    + " for user id " + userMap.get(JsonKey.ID) + " is not valid.");
                }
            }
            // update the user external identity data
            ProjectLogger.log("User insertation for extrenal identity started--.....");
            requestMap.put(JsonKey.EMAIL_VERIFIED, false);
            updateUserExtId(requestMap);
            ProjectLogger.log("User insertation for extrenal identity completed--.....");
        }

        ProjectLogger.log("User created successfully.....");
        response.put(JsonKey.ACCESSTOKEN, accessToken);
        sender().tell(response, self());

        // object of telemetry event...
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();

        targetObject = TelemetryUtil.generateTargetObject((String) userMap.get(JsonKey.ID),
                JsonKey.USER, JsonKey.CREATE, null);
        TelemetryUtil.telemetryProcessingCall(actorMessage.getRequest(), targetObject,
                correlatedObject);

        // user created successfully send the onboarding mail
        // putting rootOrgId to get web url per tenant while sending mail
        emailTemplateMap.put(JsonKey.ROOT_ORG_ID, userMap.get(JsonKey.ROOT_ORG_ID));
        sendOnboardingMail(emailTemplateMap);
        ProjectLogger.log("calling Send SMS method:", LoggerEnum.INFO);
        sendSMS(userMap);

        if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
            ProjectLogger.log("method call going to start for ES--.....");
            Request userRequest = new Request();
            userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
            userRequest.getRequest().put(JsonKey.ID, userMap.get(JsonKey.ID));
            ProjectLogger.log("making a call to save user data to ES");
            try {
                tellToAnother(userRequest);
                // ActorUtil.tell(userRequest);
            } catch (Exception ex) {
                ProjectLogger.log(
                        "Exception Occured during saving user to Es while creating user : ", ex);
            }
        } else {
            ProjectLogger.log("no call for ES to save user");
        }

    }

    private void validateRegAndRootOrg(Map<String, Object> userMap) {
        Util.DbInfo orgDb = Util.dbInfoMap.get(JsonKey.ORG_DB);
        Response orgResponse = null;
        try {
            orgResponse = cassandraOperation.getRecordById(orgDb.getKeySpace(),
                    orgDb.getTableName(), (String) userMap.get(JsonKey.REGISTERED_ORG_ID));
        } catch (Exception e) {
            ProjectLogger.log("Exception occured while verifying regOrgId during create user : ",
                    e);
            throw new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
                    ResponseCode.invalidOrgId.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        List<Map<String, Object>> responseList =
                (List<Map<String, Object>>) orgResponse.get(JsonKey.RESPONSE);
        String rootOrgId = "";
        if (null != responseList && !(responseList.isEmpty())) {
            String orgId = (String) responseList.get(0).get(JsonKey.ID);
            Map<String, Object> orgMap = responseList.get(0);
            boolean isRootOrg = false;
            if (null != orgMap.get(JsonKey.IS_ROOT_ORG)) {
                isRootOrg = (boolean) orgMap.get(JsonKey.IS_ROOT_ORG);
            } else {
                isRootOrg = false;
            }
            if (isRootOrg) {
                rootOrgId = orgId;
            } else {
                String channel = (String) orgMap.get(JsonKey.CHANNEL);
                if (!StringUtils.isBlank(channel)) {
                    Map<String, Object> filters = new HashMap<>();
                    filters.put(JsonKey.CHANNEL, channel);
                    filters.put(JsonKey.IS_ROOT_ORG, true);
                    Map<String, Object> esResult = elasticSearchComplexSearch(filters,
                            EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
                    if (isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT)
                            && isNotNull(esResult.get(JsonKey.CONTENT))
                            && !(((List<String>) esResult.get(JsonKey.CONTENT)).isEmpty())) {
                        Map<String, Object> esContent =
                                ((List<Map<String, Object>>) esResult.get(JsonKey.CONTENT)).get(0);
                        rootOrgId = (String) esContent.get(JsonKey.ID);
                    } else {
                        throw new ProjectCommonException(
                                ResponseCode.invalidRootOrgData.getErrorCode(),
                                ProjectUtil.formatMessage(
                                        ResponseCode.invalidRootOrgData.getErrorMessage(), channel),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                    }
                } else {
                    rootOrgId = JsonKey.DEFAULT_ROOT_ORG_ID;
                }
            }
            userMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
        } else {
            throw new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
                    ResponseCode.invalidOrgId.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    private void sendSMS(Map<String, Object> userMap) {
        ProjectLogger.log("Inside Send SMS method:", LoggerEnum.INFO);
        if (StringUtils.isBlank((String) userMap.get(JsonKey.EMAIL))
                && !StringUtils.isBlank((String) userMap.get(JsonKey.PHONE))) {

            UserUtility.decryptUserData(userMap);
            String name = (String) userMap.get(JsonKey.FIRST_NAME) + " "
                    + (String) userMap.get(JsonKey.LAST_NAME);

            String envName = System.getenv(JsonKey.SUNBIRD_INSTALLATION);
            if (StringUtils.isBlank(envName)) {
                envName = propertiesCache.getProperty(JsonKey.SUNBIRD_INSTALLATION);
            }
            String webUrl = Util.getSunbirdWebUrlPerTenent(userMap);

            ProjectLogger.log("shortened url :: " + webUrl, LoggerEnum.INFO);
            String sms = ProjectUtil.getSMSBody(name, webUrl, envName);
            if (StringUtils.isBlank(sms)) {
                sms = PropertiesCache.getInstance().getProperty("sunbird_default_welcome_sms");
            }
            ProjectLogger.log("SMS text : " + sms, LoggerEnum.INFO);
            String countryCode = "";
            if (StringUtils.isBlank((String) userMap.get(JsonKey.COUNTRY_CODE))) {
                countryCode =
                        PropertiesCache.getInstance().getProperty("sunbird_default_country_code");
            } else {
                countryCode = (String) userMap.get(JsonKey.COUNTRY_CODE);
            }
            ISmsProvider smsProvider = SMSFactory.getInstance("91SMS");
            ProjectLogger.log(
                    "SMS text : " + sms + " with phone " + (String) userMap.get(JsonKey.PHONE),
                    LoggerEnum.INFO);
            boolean response =
                    smsProvider.send((String) userMap.get(JsonKey.PHONE), countryCode, sms);
            ProjectLogger.log("Response from smsProvider : " + response, LoggerEnum.INFO);
            if (response) {
                ProjectLogger.log("Welcome Message sent successfully to ."
                        + (String) userMap.get(JsonKey.PHONE), LoggerEnum.INFO);
            } else {
                ProjectLogger.log(
                        "Welcome Message failed for ." + (String) userMap.get(JsonKey.PHONE),
                        LoggerEnum.INFO);
            }
        }
    }

    private void checkEmailUniqueness(Map<String, Object> userMap, String opType) {
        // Get Email configuration if not found , by default Email can be duplicate
        // across the
        // application
        String emailSetting = DataCacheHandler.getConfigSettings().get(JsonKey.EMAIL_UNIQUE);
        if (null != emailSetting && Boolean.parseBoolean(emailSetting)) {
            String email = (String) userMap.get(JsonKey.EMAIL);
            if (!StringUtils.isBlank(email)) {
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
        String phoneSetting = DataCacheHandler.getConfigSettings().get(JsonKey.PHONE_UNIQUE);
        if (null != phoneSetting && Boolean.parseBoolean(phoneSetting)) {
            String phone = (String) userMap.get(JsonKey.PHONE);
            if (!StringUtils.isBlank(phone)) {
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

    private void insertOrganisationDetails(Map<String, Object> userMap) {

        Map<String, Object> reqMap = new HashMap<>();
        reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
        reqMap.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.REGISTERED_ORG_ID));
        reqMap.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
        reqMap.put(JsonKey.IS_DELETED, false);
        Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        try {
            cassandraOperation.insertRecord(usrOrgDb.getKeySpace(), usrOrgDb.getTableName(),
                    reqMap);
        } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private void insertJobProfileDetails(Map<String, Object> userMap) {
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
        List<Map<String, Object>> reqList =
                (List<Map<String, Object>>) userMap.get(JsonKey.JOB_PROFILE);
        for (int i = 0; i < reqList.size(); i++) {
            Map<String, Object> reqMap = reqList.get(i);
            reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i + 1));
            String addrId = null;
            Response addrResponse = null;
            if (reqMap.containsKey(JsonKey.ADDRESS)) {
                Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
                addrId = ProjectUtil.getUniqueIdFromTimestamp(i + 1);
                address.put(JsonKey.ID, addrId);
                address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
                try {
                    addrResponse = cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),
                            addrDbInfo.getTableName(), address);
                } catch (Exception e) {
                    ProjectLogger.log(e.getMessage(), e);
                }
            }
            if (null != addrResponse && ((String) addrResponse.get(JsonKey.RESPONSE))
                    .equalsIgnoreCase(JsonKey.SUCCESS)) {
                reqMap.put(JsonKey.ADDRESS_ID, addrId);
                reqMap.remove(JsonKey.ADDRESS);
            }
            reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
            reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
            try {
                cassandraOperation.insertRecord(jobProDbInfo.getKeySpace(),
                        jobProDbInfo.getTableName(), reqMap);
            } catch (Exception e) {
                ProjectLogger.log(e.getMessage(), e);
            }
        }

    }

    @SuppressWarnings("unchecked")
    private void insertEducationDetails(Map<String, Object> userMap) {
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
        List<Map<String, Object>> reqList =
                (List<Map<String, Object>>) userMap.get(JsonKey.EDUCATION);
        for (int i = 0; i < reqList.size(); i++) {
            Map<String, Object> reqMap = reqList.get(i);
            reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i + 1));
            String addrId = null;
            Response addrResponse = null;
            if (reqMap.containsKey(JsonKey.ADDRESS)) {
                Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
                addrId = ProjectUtil.getUniqueIdFromTimestamp(i + 1);
                address.put(JsonKey.ID, addrId);
                address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
                try {
                    addrResponse = cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),
                            addrDbInfo.getTableName(), address);
                } catch (Exception e) {
                    ProjectLogger.log(e.getMessage(), e);
                }
            }
            if (null != addrResponse && ((String) addrResponse.get(JsonKey.RESPONSE))
                    .equalsIgnoreCase(JsonKey.SUCCESS)) {
                reqMap.put(JsonKey.ADDRESS_ID, addrId);
                reqMap.remove(JsonKey.ADDRESS);
            }
            try {
                if (null != reqMap.get(JsonKey.YEAR_OF_PASSING)) {
                    reqMap.put(JsonKey.YEAR_OF_PASSING,
                            ((BigInteger) reqMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
                } else {
                    reqMap.put(JsonKey.YEAR_OF_PASSING, 0);
                }
            } catch (Exception ex) {
                ProjectLogger.log(ex.getMessage(), ex);
                reqMap.put(JsonKey.YEAR_OF_PASSING, 0);
            }
            try {
                if (null != reqMap.get(JsonKey.PERCENTAGE)) {
                    reqMap.put(JsonKey.PERCENTAGE,
                            Double.parseDouble(String.valueOf(reqMap.get(JsonKey.PERCENTAGE))));
                } else {
                    reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf("0")));
                }
            } catch (Exception ex) {
                reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf("0")));
                ProjectLogger.log(ex.getMessage(), ex);
            }
            reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
            reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
            try {
                cassandraOperation.insertRecord(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(),
                        reqMap);
            } catch (Exception e) {
                ProjectLogger.log(e.getMessage(), e);
            }
        }

    }

    private void updateUserExtId(Map<String, Object> requestMap) {
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
                && !(StringUtils.isBlank((String) requestMap.get(JsonKey.USERNAME)))) {
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
                && !(StringUtils.isBlank((String) requestMap.get(JsonKey.PHONE)))) {
            map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
            map.put(JsonKey.EXTERNAL_ID, JsonKey.PHONE);
            map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.PHONE));

            if (!StringUtils.isBlank((String) requestMap.get(JsonKey.PHONE_VERIFIED))
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
                && !(StringUtils.isBlank((String) requestMap.get(JsonKey.EMAIL)))) {
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

    private void removeUnwanted(Map<String, Object> reqMap) {
        reqMap.remove(JsonKey.ADDRESS);
        reqMap.remove(JsonKey.EDUCATION);
        reqMap.remove(JsonKey.JOB_PROFILE);
        reqMap.remove(JsonKey.ORGANISATION);
        reqMap.remove(JsonKey.EMAIL_VERIFIED);
        reqMap.remove(JsonKey.PHONE_NUMBER_VERIFIED);
        reqMap.remove(JsonKey.REGISTERED_ORG);
        reqMap.remove(JsonKey.ROOT_ORG);
        reqMap.remove(JsonKey.IDENTIFIER);
        reqMap.remove(JsonKey.ORGANISATIONS);
        reqMap.remove(JsonKey.IS_DELETED);
        reqMap.remove(JsonKey.PHONE_VERIFIED);
    }

    /**
     * Utility method to provide the unique authtoken .
     */
    @SuppressWarnings("unchecked")
    private void checkForDuplicateUserAuthToken(Map<String, Object> userAuthMap,
            Map<String, Object> resultMap, Map<String, Object> reqMap) {
        Util.DbInfo userAuthDbInfo = Util.dbInfoMap.get(JsonKey.USER_AUTH_DB);
        String userAuth = null;
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.SOURCE, reqMap.get(JsonKey.SOURCE));
        map.put(JsonKey.USER_ID, resultMap.get(JsonKey.USER_ID));
        Response authResponse = cassandraOperation.getRecordsByProperties(
                userAuthDbInfo.getKeySpace(), userAuthDbInfo.getTableName(), map);
        List<Map<String, Object>> userAuthList =
                ((List<Map<String, Object>>) authResponse.get(JsonKey.RESPONSE));
        if (null != userAuthList && userAuthList.isEmpty()) {
            cassandraOperation.insertRecord(userAuthDbInfo.getKeySpace(),
                    userAuthDbInfo.getTableName(), userAuthMap);
        } else {
            cassandraOperation.deleteRecord(userAuthDbInfo.getKeySpace(),
                    userAuthDbInfo.getTableName(), (String) (userAuthList.get(0)).get(JsonKey.ID));
            userAuth = ProjectUtil.createAuthToken((String) resultMap.get(JsonKey.ID),
                    (String) reqMap.get(JsonKey.SOURCE));
            userAuthMap.put(JsonKey.ID, userAuth);
            userAuthMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            cassandraOperation.insertRecord(userAuthDbInfo.getKeySpace(),
                    userAuthDbInfo.getTableName(), userAuthMap);
        }
    }

    /**
     * This method will provide the complete role structure..
     *
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void getRoles() {
        Util.DbInfo roleDbInfo = Util.dbInfoMap.get(JsonKey.ROLE);
        Util.DbInfo roleGroupDbInfo = Util.dbInfoMap.get(JsonKey.ROLE_GROUP);
        Util.DbInfo urlActionDbInfo = Util.dbInfoMap.get(JsonKey.URL_ACTION);
        Response mergeResponse = new Response();
        List<Map<String, Object>> resposnemap = new ArrayList<>();
        List<Map<String, Object>> list = null;
        Response response = cassandraOperation.getAllRecords(roleDbInfo.getKeySpace(),
                roleDbInfo.getTableName());
        Response rolegroup = cassandraOperation.getAllRecords(roleGroupDbInfo.getKeySpace(),
                roleGroupDbInfo.getTableName());
        Response urlAction = cassandraOperation.getAllRecords(urlActionDbInfo.getKeySpace(),
                urlActionDbInfo.getTableName());
        List<Map<String, Object>> urlActionListMap =
                (List<Map<String, Object>>) urlAction.getResult().get(JsonKey.RESPONSE);
        List<Map<String, Object>> roleGroupMap =
                (List<Map<String, Object>>) rolegroup.getResult().get(JsonKey.RESPONSE);
        list = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
        if (list != null && !(list.isEmpty())) {
            // This map will have all the master roles
            for (Map<String, Object> map : list) {
                Map<String, Object> roleResponseMap = new HashMap<>();
                roleResponseMap.put(JsonKey.ID, map.get(JsonKey.ID));
                roleResponseMap.put(JsonKey.NAME, map.get(JsonKey.NAME));
                List<String> roleGroup = (List) map.get(JsonKey.ROLE_GROUP_ID);
                List<Map<String, Object>> actionGroupListMap = new ArrayList<>();
                roleResponseMap.put(JsonKey.ACTION_GROUPS, actionGroupListMap);
                Map<String, Object> subRoleResponseMap = null;
                for (String val : roleGroup) {
                    subRoleResponseMap = new HashMap<>();
                    Map<String, Object> subRoleMap = getSubRoleListMap(roleGroupMap, val);
                    List<String> subRole = (List) subRoleMap.get(JsonKey.URL_ACTION_ID);
                    List<Map<String, Object>> roleUrlResponList = new ArrayList<>();
                    subRoleResponseMap.put(JsonKey.ID, subRoleMap.get(JsonKey.ID));
                    subRoleResponseMap.put(JsonKey.NAME, subRoleMap.get(JsonKey.NAME));
                    for (String rolemap : subRole) {
                        roleUrlResponList.add(getRoleAction(urlActionListMap, rolemap));
                    }
                    if (subRoleResponseMap.containsKey(JsonKey.ACTIONS)) {
                        List<Map<String, Object>> listOfMap =
                                (List<Map<String, Object>>) subRoleResponseMap.get(JsonKey.ACTIONS);
                        listOfMap.addAll(roleUrlResponList);
                    } else {
                        subRoleResponseMap.put(JsonKey.ACTIONS, roleUrlResponList);
                    }
                    actionGroupListMap.add(subRoleResponseMap);
                }

                resposnemap.add(roleResponseMap);
            }
        }
        mergeResponse.getResult().put(JsonKey.ROLES, resposnemap);
        sender().tell(mergeResponse, self());
    }

    /**
     * This method will find the action from role action mapping it will return action id, action
     * name and list of urls.
     *
     * @param urlActionListMap List<Map<String,Object>>
     * @param actionName String
     * @return Map<String,Object>
     */
    @SuppressWarnings("rawtypes")
    private Map<String, Object> getRoleAction(List<Map<String, Object>> urlActionListMap,
            String actionName) {
        Map<String, Object> response = new HashMap<>();
        if (urlActionListMap != null && !(urlActionListMap.isEmpty())) {
            for (Map<String, Object> map : urlActionListMap) {
                if (map.get(JsonKey.ID).equals(actionName)) {
                    response.put(JsonKey.ID, map.get(JsonKey.ID));
                    response.put(JsonKey.NAME, map.get(JsonKey.NAME));
                    response.put(JsonKey.URLS, map.get(JsonKey.URL) != null ? map.get(JsonKey.URL)
                            : new ArrayList<String>());
                    return response;
                }
            }
        }
        return response;
    }

    /**
     * This method will provide sub role mapping details.
     *
     * @param urlActionListMap List<Map<String, Object>>
     * @param roleName String
     * @return Map< String, Object>
     */
    @SuppressWarnings("rawtypes")
    private Map<String, Object> getSubRoleListMap(List<Map<String, Object>> urlActionListMap,
            String roleName) {
        Map<String, Object> response = new HashMap<>();
        if (urlActionListMap != null && !(urlActionListMap.isEmpty())) {
            for (Map<String, Object> map : urlActionListMap) {
                if (map.get(JsonKey.ID).equals(roleName)) {
                    response.put(JsonKey.ID, map.get(JsonKey.ID));
                    response.put(JsonKey.NAME, map.get(JsonKey.NAME));
                    response.put(JsonKey.URL_ACTION_ID, map.get(JsonKey.URL_ACTION_ID) != null
                            ? map.get(JsonKey.URL_ACTION_ID) : new ArrayList<>());
                    return response;
                }
            }
        }
        return response;
    }

    /**
     * Method to join the user with organization ...
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void joinUserOrganisation(Request actorMessage) {

        Response response = null;

        Util.DbInfo organisationDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

        Map<String, Object> req = actorMessage.getRequest();

        // object of telemetry event...
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();

        Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
        if (isNull(usrOrgData)) {
            // create exception here and sender.tell the exception and return
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        String updatedBy = null;
        String orgId = null;
        String userId = null;
        if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
            orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
        }

        if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
            userId = (String) usrOrgData.get(JsonKey.USER_ID);
        }
        if (isNotNull(req.get(JsonKey.REQUESTED_BY))) {
            updatedBy = (String) req.get(JsonKey.REQUESTED_BY);
        }

        if (isNull(orgId) || isNull(userId)) {
            // create exception here invalid request data and tell the exception , then
            // return
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        // check org exist or not
        Response orgResult = cassandraOperation.getRecordById(organisationDbInfo.getKeySpace(),
                organisationDbInfo.getTableName(), orgId);

        List orgList = (List) orgResult.get(JsonKey.RESPONSE);
        if (orgList.isEmpty()) {
            // user already enrolled for the organisation
            ProjectLogger.log("Org does not exist");
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
                            ResponseCode.invalidOrgId.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        // check user already exist for the org or not
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(JsonKey.USER_ID, userId);
        requestData.put(JsonKey.ORGANISATION_ID, orgId);

        Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
                userOrgDbInfo.getTableName(), requestData);

        List list = (List) result.get(JsonKey.RESPONSE);
        if (!list.isEmpty()) {
            // user already enrolled for the organisation
            response = new Response();
            response.getResult().put(JsonKey.RESPONSE, "User already joined the organisation");
            sender().tell(response, self());
            return;
        }

        String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        usrOrgData.put(JsonKey.ID, id);
        if (!(StringUtils.isBlank(updatedBy))) {
            String updatedByName = getUserNamebyUserId(updatedBy);
            usrOrgData.put(JsonKey.ADDED_BY_NAME, updatedByName);
            usrOrgData.put(JsonKey.ADDED_BY, updatedBy);
        }
        usrOrgData.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
        usrOrgData.put(JsonKey.IS_REJECTED, false);
        usrOrgData.put(JsonKey.IS_APPROVED, false);
        usrOrgData.remove(JsonKey.REQUESTED_BY);
        response = cassandraOperation.insertRecord(userOrgDbInfo.getKeySpace(),
                userOrgDbInfo.getTableName(), usrOrgData);
        sender().tell(response, self());

        targetObject =
                TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        TelemetryUtil.generateCorrelatedObject(userId, JsonKey.USER, null, correlatedObject);
        TelemetryUtil.generateCorrelatedObject(orgId, JsonKey.ORGANISATION, null, correlatedObject);
        TelemetryUtil.telemetryProcessingCall(actorMessage.getRequest(), targetObject,
                correlatedObject);

    }

    /**
     * Method to approve the user organisation .
     */
    @SuppressWarnings("unchecked")
    private void approveUserOrg(Request actorMessage) {

        Response response = null;
        // object of telemetry event...
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();

        Map<String, Object> updateUserOrgDBO = new HashMap<>();
        Map<String, Object> req = actorMessage.getRequest();
        String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

        Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
        if (isNull(usrOrgData)) {
            // create exception here and sender.tell the exception and return
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        String orgId = null;
        String userId = null;
        List<String> roles = null;
        if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
            orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
        }

        if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
            userId = (String) usrOrgData.get(JsonKey.USER_ID);
        }
        if (isNotNull(usrOrgData.get(JsonKey.ROLES))) {
            roles = (List<String>) usrOrgData.get(JsonKey.ROLES);
        }

        if (isNull(orgId) || isNull(userId) || isNull(roles)) {
            // create exception here invalid request data and tell the exception , then
            // return
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        // check user already exist for the org or not
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(JsonKey.USER_ID, userId);
        requestData.put(JsonKey.ORGANISATION_ID, orgId);

        Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
                userOrgDbInfo.getTableName(), requestData);

        List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (list.isEmpty()) {
            // user already enrolled for the organisation
            ProjectLogger.log("User does not belong to org");
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
                            ResponseCode.invalidOrgId.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        Map<String, Object> userOrgDBO = list.get(0);

        if (!(StringUtils.isBlank(updatedBy))) {
            String updatedByName = getUserNamebyUserId(updatedBy);
            updateUserOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
            updateUserOrgDBO.put(JsonKey.APPROVED_BY, updatedByName);
        }
        updateUserOrgDBO.put(JsonKey.ID, userOrgDBO.get(JsonKey.ID));
        updateUserOrgDBO.put(JsonKey.APPROOVE_DATE, ProjectUtil.getFormattedDate());
        updateUserOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());

        updateUserOrgDBO.put(JsonKey.IS_APPROVED, true);
        updateUserOrgDBO.put(JsonKey.IS_REJECTED, false);
        updateUserOrgDBO.put(JsonKey.ROLES, roles);

        response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace(),
                userOrgDbInfo.getTableName(), updateUserOrgDBO);
        sender().tell(response, self());

        targetObject =
                TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        TelemetryUtil.generateCorrelatedObject(userId, JsonKey.USER, null, correlatedObject);
        TelemetryUtil.generateCorrelatedObject(orgId, JsonKey.ORGANISATION, null, correlatedObject);
        TelemetryUtil.telemetryProcessingCall(actorMessage.getRequest(), targetObject,
                correlatedObject);
    }

    /**
     * Method to reject the user organisation .
     */
    private void rejectUserOrg(Request actorMessage) {

        Response response = null;

        Map<String, Object> updateUserOrgDBO = new HashMap<>();
        Map<String, Object> req = actorMessage.getRequest();
        String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

        // object of telemetry event...
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();

        @SuppressWarnings("unchecked")
        Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
        if (isNull(usrOrgData)) {
            // create exception here and sender.tell the exception and return
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        String orgId = null;
        String userId = null;

        if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
            orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
        }

        if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
            userId = (String) usrOrgData.get(JsonKey.USER_ID);
        }

        if (isNull(orgId) || isNull(userId)) {
            // creating exception here, invalid request data and tell the exception , then
            // return
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        // check user already exist for the org or not
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(JsonKey.USER_ID, userId);
        requestData.put(JsonKey.ORGANISATION_ID, orgId);

        Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
                userOrgDbInfo.getTableName(), requestData);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (list.isEmpty()) {
            // user already enrolled for the organisation
            ProjectLogger.log("User does not belong to org");
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(),
                            ResponseCode.invalidOrgId.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        Map<String, Object> userOrgDBO = list.get(0);

        if (!(StringUtils.isBlank(updatedBy))) {
            String updatedByName = getUserNamebyUserId(updatedBy);
            updateUserOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
            updateUserOrgDBO.put(JsonKey.APPROVED_BY, updatedByName);
        }
        updateUserOrgDBO.put(JsonKey.ID, userOrgDBO.get(JsonKey.ID));
        updateUserOrgDBO.put(JsonKey.APPROOVE_DATE, ProjectUtil.getFormattedDate());
        updateUserOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());

        updateUserOrgDBO.put(JsonKey.IS_APPROVED, false);
        updateUserOrgDBO.put(JsonKey.IS_REJECTED, true);

        response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace(),
                userOrgDbInfo.getTableName(), updateUserOrgDBO);
        sender().tell(response, self());

        targetObject =
                TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        TelemetryUtil.generateCorrelatedObject(userId, JsonKey.USER, null, correlatedObject);
        TelemetryUtil.generateCorrelatedObject(orgId, JsonKey.ORGANISATION, null, correlatedObject);
        TelemetryUtil.telemetryProcessingCall(actorMessage.getRequest(), targetObject,
                correlatedObject);
    }

    /**
     * This method will provide user name based on user id if user not found then it will return
     * null.
     *
     * @param userId String
     * @return String
     */
    @SuppressWarnings("unchecked")
    private String getUserNamebyUserId(String userId) {
        Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Response result = cassandraOperation.getRecordById(userdbInfo.getKeySpace(),
                userdbInfo.getTableName(), userId);
        List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (!(list.isEmpty())) {
            return (String) (list.get(0).get(JsonKey.USERNAME));
        }
        return null;
    }

    /**
     * @param actorMessage
     */
    private void getUserDetails(Request actorMessage) {
        Map<String, Object> requestMap = actorMessage.getRequest();
        SearchDTO dto = new SearchDTO();
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.REGISTERED_ORG_ID, requestMap.get(JsonKey.REGISTERED_ORG_ID));
        map.put(JsonKey.ROOT_ORG_ID, requestMap.get(JsonKey.ROOT_ORG_ID));
        Map<String, Object> additionalProperty = new HashMap<>();
        additionalProperty.put(JsonKey.FILTERS, map);
        dto.setAdditionalProperties(additionalProperty);
        Map<String, Object> responseMap = ElasticSearchUtil.complexSearch(dto,
                ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName());
        Response response = new Response();
        response.put(JsonKey.RESPONSE, responseMap);
        sender().tell(response, self());
    }

    /**
     * Method to block the user , it performs only soft delete from Cassandra , ES , Keycloak
     * 
     * @param actorMessage
     */
    @SuppressWarnings("unchecked")
    private void blockUser(Request actorMessage) {

        ProjectLogger.log("Method call  " + "deleteUser");
        // object of telemetry event...
        Map<String, Object> targetObject = null;
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String, Object> userMap =
                (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        if (ProjectUtil.isNull(userMap.get(JsonKey.USER_ID))) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;

        }
        String userId = (String) userMap.get(JsonKey.USER_ID);
        Response resultFrUserId = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), userId);
        if (((List<Map<String, Object>>) resultFrUserId.get(JsonKey.RESPONSE)).isEmpty()) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
                            ResponseCode.userNotFound.getErrorMessage(),
                            ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        Map<String, Object> dbMap = new HashMap<>();
        dbMap.put(JsonKey.IS_DELETED, true);
        dbMap.put(JsonKey.STATUS, Status.INACTIVE.getValue());
        dbMap.put(JsonKey.ID, userId);
        dbMap.put(JsonKey.USER_ID, userId);
        dbMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        dbMap.put(JsonKey.UPDATED_BY, actorMessage.getRequest().get(JsonKey.REQUESTED_BY));

        // deactivate from keycloak -- softdelete
        if (isSSOEnabled) {
            ssoManager.deactivateUser(dbMap);
        }
        // soft delete from cassandra--
        Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), dbMap);
        ProjectLogger.log("USER DELETED " + userId);
        sender().tell(response, self());

        // update record in elasticsearch ......
        Request request = new Request();
        request.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
        request.getRequest().put(JsonKey.ID, userId);
        tellToAnother(request);

        targetObject =
                TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        Map<String, Object> telemetryAction = new HashMap<>();
        telemetryAction.put("blockUser", "delete user");
        TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, new ArrayList<>());
    }

    /**
     * This method will assign roles to users or user organizations.
     * 
     * @param actorMessage
     */
    @SuppressWarnings("unchecked")
    private void assignRoles(Request actorMessage) {
        // object of telemetry event...
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();
        Map<String, Object> requestMap = actorMessage.getRequest();
        if (requestMap == null || requestMap.size() == 0) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        Map<String, Object> esUsrRes = null;
        Map<String, Object> tempMap = new HashMap<>();
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        String userId = (String) requestMap.get(JsonKey.USER_ID);
        String externalId = (String) requestMap.get(JsonKey.EXTERNAL_ID);
        String provider = (String) requestMap.get(JsonKey.PROVIDER);
        String userName = (String) requestMap.get(JsonKey.USERNAME);
        String loginId = "";
        if (StringUtils.isBlank(userId) && StringUtils.isBlank(userName)) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userNameOrUserIdRequired.getErrorCode(),
                            ResponseCode.userNameOrUserIdRequired.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        if (!StringUtils.isBlank(userId)) {
            esUsrRes = ElasticSearchUtil.getDataByIdentifier(
                    ProjectUtil.EsIndex.sunbird.getIndexName(),
                    ProjectUtil.EsType.user.getTypeName(), userId);
        } else {
            if (!StringUtils.isBlank(userName) && !StringUtils.isBlank(provider)) {
                loginId = userName + JsonKey.LOGIN_ID_DELIMETER + provider;
            } else {
                loginId = userName;
            }
            try {
                loginId = encryptionService.encryptData(loginId);
            } catch (Exception e) {
                ProjectCommonException exception = new ProjectCommonException(
                        ResponseCode.userDataEncryptionError.getErrorCode(),
                        ResponseCode.userDataEncryptionError.getErrorMessage(),
                        ResponseCode.SERVER_ERROR.getResponseCode());
                sender().tell(exception, self());
                return;
            }

            SearchDTO searchDto = new SearchDTO();
            Map<String, Object> filter = new HashMap<>();
            filter.put(JsonKey.LOGIN_ID, loginId);
            searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filter);
            Map<String, Object> esResponse = ElasticSearchUtil.complexSearch(searchDto,
                    ProjectUtil.EsIndex.sunbird.getIndexName(),
                    ProjectUtil.EsType.user.getTypeName());
            List<Map<String, Object>> esUsrList =
                    (List<Map<String, Object>>) esResponse.get(JsonKey.CONTENT);

            if (esUsrList.isEmpty()) {
                ProjectCommonException exception =
                        new ProjectCommonException(ResponseCode.invalidUsrData.getErrorCode(),
                                ResponseCode.invalidUsrData.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
                return;
            }
            esUsrRes = esUsrList.get(0);
            requestMap.put(JsonKey.USER_ID, esUsrRes.get(JsonKey.ID));
        }

        if (StringUtils.isBlank((String) requestMap.get(JsonKey.ORGANISATION_ID))
                && !StringUtils.isBlank(externalId) && !StringUtils.isBlank(provider)) {
            SearchDTO searchDto = new SearchDTO();
            Map<String, Object> filter = new HashMap<>();
            filter.put(JsonKey.EXTERNAL_ID, externalId);
            filter.put(JsonKey.PROVIDER, provider);
            searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filter);
            Map<String, Object> esResponse = ElasticSearchUtil.complexSearch(searchDto,
                    ProjectUtil.EsIndex.sunbird.getIndexName(),
                    ProjectUtil.EsType.organisation.getTypeName());
            List<Map<String, Object>> list =
                    (List<Map<String, Object>>) esResponse.get(JsonKey.CONTENT);

            if (list.isEmpty()) {
                ProjectCommonException exception =
                        new ProjectCommonException(ResponseCode.invalidOrgData.getErrorCode(),
                                ResponseCode.invalidOrgData.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
                return;
            }
            requestMap.put(JsonKey.ORGANISATION_ID, list.get(0).get(JsonKey.ID));
        }

        // now we have valid userid , roles and need to check organisation id is also
        // coming.
        // if organisationid is coming it means need to update userOrg role.
        // if organisationId is not coming then need to update only userRole.
        if (requestMap.containsKey(JsonKey.ORGANISATION_ID) && !ProjectUtil
                .isStringNullOREmpty((String) requestMap.get(JsonKey.ORGANISATION_ID))) {
            tempMap.remove(JsonKey.PROVIDER);
            tempMap.remove(JsonKey.EXTERNAL_ID);
            tempMap.remove(JsonKey.SOURCE);
            tempMap.put(JsonKey.ORGANISATION_ID, requestMap.get(JsonKey.ORGANISATION_ID));
            tempMap.put(JsonKey.USER_ID, requestMap.get(JsonKey.USER_ID));
            Util.DbInfo userOrgDb = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
            Response response = cassandraOperation.getRecordsByProperties(userOrgDb.getKeySpace(),
                    userOrgDb.getTableName(), tempMap);
            List<Map<String, Object>> list =
                    (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
            if (list.isEmpty()) {
                ProjectCommonException exception =
                        new ProjectCommonException(ResponseCode.invalidUsrOrgData.getErrorCode(),
                                ResponseCode.invalidUsrOrgData.getErrorMessage(),
                                ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
                return;
            }
            if (null != requestMap.get(JsonKey.ROLES)
                    && !((List<String>) requestMap.get(JsonKey.ROLES)).isEmpty()) {
                String msg = Util.validateRoles((List<String>) requestMap.get(JsonKey.ROLES));
                if (!msg.equalsIgnoreCase(JsonKey.SUCCESS)) {
                    throw new ProjectCommonException(ResponseCode.invalidRole.getErrorCode(),
                            ResponseCode.invalidRole.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }
            }

            List<String> roles = (List<String>) list.get(0).get(JsonKey.ROLES);
            if (null != roles && !roles.isEmpty()) {
                List<String> requestedRoles = (List<String>) requestMap.get(JsonKey.ROLES);
                for (String role : requestedRoles) {
                    if (!roles.contains(role)) {
                        roles.add(role);
                    }
                }
                tempMap.put(JsonKey.ROLES, roles);
            } else {
                tempMap.put(JsonKey.ROLES, requestMap.get(JsonKey.ROLES));
            }
            tempMap.put(JsonKey.ID, list.get(0).get(JsonKey.ID));

            response = cassandraOperation.updateRecord(userOrgDb.getKeySpace(),
                    userOrgDb.getTableName(), tempMap);
            sender().tell(response, self());
            if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
                updateRoleToEs(tempMap, JsonKey.ORGANISATION,
                        (String) requestMap.get(JsonKey.USER_ID),
                        (String) requestMap.get(JsonKey.ORGANISATION_ID));
            } else {
                ProjectLogger.log("no call for ES to save user");
            }
            targetObject =
                    TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
            TelemetryUtil.generateCorrelatedObject((String) requestMap.get(JsonKey.ORGANISATION_ID),
                    JsonKey.ORGANISATION, null, correlatedObject);
            Map<String, Object> telemetryAction = new HashMap<>();
            telemetryAction.put("assignRole", "role assigned at org level");
            TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, correlatedObject);
            return;

        } else {
            tempMap.remove(JsonKey.EXTERNAL_ID);
            tempMap.remove(JsonKey.SOURCE);
            tempMap.remove(JsonKey.PROVIDER);
            tempMap.remove(JsonKey.ORGANISATION_ID);
            if (null != requestMap.get(JsonKey.ROLES)
                    && !((List<String>) requestMap.get(JsonKey.ROLES)).isEmpty()) {
                String msg = Util.validateRoles((List<String>) requestMap.get(JsonKey.ROLES));
                if (!msg.equalsIgnoreCase(JsonKey.SUCCESS)) {
                    throw new ProjectCommonException(ResponseCode.invalidRole.getErrorCode(),
                            ResponseCode.invalidRole.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }
            }
            tempMap.put(JsonKey.ID, requestMap.get(JsonKey.USER_ID));
            List<String> roles = (List<String>) esUsrRes.get(JsonKey.ROLES);
            if (null != roles && !roles.isEmpty()) {
                List<String> requestedRoles = (List<String>) requestMap.get(JsonKey.ROLES);
                for (String role : requestedRoles) {
                    if (!roles.contains(role)) {
                        roles.add(role);
                    }
                }
                tempMap.put(JsonKey.ROLES, roles);
            } else {
                tempMap.put(JsonKey.ROLES, requestMap.get(JsonKey.ROLES));
            }
            Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),
                    usrDbInfo.getTableName(), tempMap);
            ElasticSearchUtil.updateData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                    ProjectUtil.EsType.user.getTypeName(), (String) requestMap.get(JsonKey.USER_ID),
                    tempMap);
            sender().tell(response, self());
            if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
                updateRoleToEs(tempMap, JsonKey.USER, (String) requestMap.get(JsonKey.USER_ID),
                        null);
            } else {
                ProjectLogger.log("no call for ES to save user");
            }

            targetObject =
                    TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
            Map<String, Object> telemetryAction = new HashMap<>();
            telemetryAction.put("assignRole", "role assigned at user level");
            TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, correlatedObject);
            return;
        }
    }

    private void updateRoleToEs(Map<String, Object> tempMap, String type, String userid,
            String orgId) {

        ProjectLogger.log("method call going to satrt for ES--.....");
        Request request = new Request();
        request.setOperation(ActorOperations.UPDATE_USER_ROLES_ES.getValue());
        request.getRequest().put(JsonKey.ROLES, tempMap.get(JsonKey.ROLES));
        request.getRequest().put(JsonKey.TYPE, type);
        request.getRequest().put(JsonKey.USER_ID, userid);
        request.getRequest().put(JsonKey.ORGANISATION_ID, orgId);
        ProjectLogger.log("making a call to save user data to ES");
        try {
            tellToAnother(request);
        } catch (Exception ex) {
            ProjectLogger.log(
                    "Exception Occured during saving user to Es while joinUserOrganisation : ", ex);
        }
    }

    /**
     * Method to un block the user
     * 
     * @param actorMessage
     */
    @SuppressWarnings("unchecked")
    private void unBlockUser(Request actorMessage) {

        ProjectLogger.log("Method call  " + "UnblockeUser");
        // object of telemetry event...
        Map<String, Object> targetObject = null;
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String, Object> userMap =
                (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        if (ProjectUtil.isNull(userMap.get(JsonKey.USER_ID))) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                            ResponseCode.invalidRequestData.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        String userId = (String) userMap.get(JsonKey.USER_ID);
        Response resultFrUserId = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), userId);
        List<Map<String, Object>> dbResult =
                (List<Map<String, Object>>) resultFrUserId.get(JsonKey.RESPONSE);
        if (dbResult.isEmpty()) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(),
                            ResponseCode.userNotFound.getErrorMessage(),
                            ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        Map<String, Object> dbUser = dbResult.get(0);
        if (dbUser.containsKey(JsonKey.IS_DELETED) && isNotNull(dbUser.get(JsonKey.IS_DELETED))
                && !((Boolean) dbUser.get(JsonKey.IS_DELETED))) {
            ProjectCommonException exception =
                    new ProjectCommonException(ResponseCode.userAlreadyActive.getErrorCode(),
                            ResponseCode.userAlreadyActive.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        Map<String, Object> dbMap = new HashMap<>();
        dbMap.put(JsonKey.IS_DELETED, false);
        dbMap.put(JsonKey.STATUS, Status.ACTIVE.getValue());
        dbMap.put(JsonKey.ID, userId);
        dbMap.put(JsonKey.USER_ID, userId);
        dbMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        dbMap.put(JsonKey.UPDATED_BY, actorMessage.getRequest().get(JsonKey.REQUESTED_BY));

        // Activate user from keycloak
        if (isSSOEnabled) {
            ssoManager.activateUser(dbMap);
        }
        // Activate user from cassandra-
        Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), dbMap);
        ProjectLogger.log("USER UNLOCKED " + userId);
        sender().tell(response, self());

        // make user active in elasticsearch ......
        Request request = new Request();
        request.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
        request.getRequest().put(JsonKey.ID, userId);
        tellToAnother(request);

        targetObject =
                TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
        Map<String, Object> telemetryAction = new HashMap<>();
        telemetryAction.put("unBlockUser", "unblock the user");
        TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, new ArrayList<>());
    }

    /**
     * This method will remove user private field from response map
     * 
     * @param responseMap Map<String,Object>
     */
    private Map<String, Object> removeUserPrivateField(Map<String, Object> responseMap) {
        ProjectLogger.log("Start removing User private field==");
        for (int i = 0; i < ProjectUtil.excludes.length; i++) {
            responseMap.remove(ProjectUtil.excludes[i]);
        }
        ProjectLogger.log("All private filed removed=");
        return responseMap;
    }

    private Map<String, Object> elasticSearchComplexSearch(Map<String, Object> filters,
            String index, String type) {
        SearchDTO searchDTO = new SearchDTO();
        searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
        return ElasticSearchUtil.complexSearch(searchDTO, index, type);
    }

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

    private void getMediaTypes() {
        Response response = SocialMediaType.getMediaTypeFromDB();
        sender().tell(response, self());
    }

    private void sendOnboardingMail(Map<String, Object> emailTemplateMap) {

        if (!(StringUtils.isBlank((String) emailTemplateMap.get(JsonKey.EMAIL)))) {

            String envName = System.getenv(JsonKey.SUNBIRD_INSTALLATION);
            if (StringUtils.isBlank(envName)) {
                envName = propertiesCache.getProperty(JsonKey.SUNBIRD_INSTALLATION);
            }

            String welcomeSubject = propertiesCache.getProperty("onboarding_mail_subject");
            emailTemplateMap.put(JsonKey.SUBJECT,
                    ProjectUtil.formatMessage(welcomeSubject, envName));
            List<String> reciptientsMail = new ArrayList<>();
            reciptientsMail.add((String) emailTemplateMap.get(JsonKey.EMAIL));
            emailTemplateMap.put(JsonKey.RECIPIENT_EMAILS, reciptientsMail);

            String webUrl = Util.getSunbirdWebUrlPerTenent(emailTemplateMap);
            if ((!StringUtils.isBlank(webUrl)) && (!SUNBIRD_WEB_URL.equalsIgnoreCase(webUrl))) {
                emailTemplateMap.put(JsonKey.WEB_URL, webUrl);
            }

            String appUrl = System.getenv(SUNBIRD_APP_URL);
            if (StringUtils.isBlank(appUrl)) {
                appUrl = propertiesCache.getProperty(SUNBIRD_APP_URL);
            }

            if ((!StringUtils.isBlank(appUrl)) && (!SUNBIRD_APP_URL.equalsIgnoreCase(appUrl))) {
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

    /**
     * This method will send forgot password email
     * 
     * @param name String
     * @param email String
     * @param userId String
     */
    private void sendForgotPasswordEmail(String name, String email, String userId) {
        VelocityContext context = new VelocityContext();
        context.put(JsonKey.NAME, name);
        context.put(JsonKey.TEMPORARY_PASSWORD, ProjectUtil.generateRandomPassword());
        context.put(JsonKey.NOTE, propertiesCache.getProperty(JsonKey.MAIL_NOTE));
        context.put(JsonKey.ORG_NAME, propertiesCache.getProperty(JsonKey.ORG_NAME));
        String appUrl = System.getenv(JsonKey.SUNBIRD_APP_URL);
        if (StringUtils.isBlank(appUrl)) {
            appUrl = propertiesCache.getProperty(JsonKey.SUNBIRD_APP_URL);
        }
        context.put(JsonKey.WEB_URL, StringUtils.isBlank(System.getenv(SUNBIRD_WEB_URL))
                ? propertiesCache.getProperty(SUNBIRD_WEB_URL) : System.getenv(SUNBIRD_WEB_URL));
        if (!StringUtils.isBlank(appUrl) && !JsonKey.SUNBIRD_APP_URL.equalsIgnoreCase(appUrl)) {
            context.put(JsonKey.APP_URL, appUrl);
        }
        ProjectLogger.log("Starting to update password inside cassandra", LoggerEnum.INFO.name());
        updatePassword(userId, (String) context.get(JsonKey.TEMPORARY_PASSWORD));
        ProjectLogger.log("Password updated in cassandra and start sending email",
                LoggerEnum.INFO.name());
        boolean response = SendMail.sendMail(new String[] {email}, "Forgot password", context,
                "forgotpassword.vm");
        ProjectLogger.log("email sent resposne==" + response, LoggerEnum.INFO.name());
    }

    /**
     * This method will update user temporary password inside cassandra db.
     * 
     * @param userId String
     * @param password Stirng
     */
    private void updatePassword(String userId, String password) {
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.ID, userId);
        map.put(JsonKey.PASSWORD, OneWayHashing.encryptVal(password));
        map.put(JsonKey.TEMPORARY_PASSWORD, map.get(JsonKey.PASSWORD));
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), map);
    }

    /**
     * This method will update user login time under cassandra and Elasticsearch.
     * 
     * @param reqMap Map<String, Object>
     * @return boolean
     */
    private boolean updateUserLoginTime(Map<String, Object> reqMap) {
        ProjectLogger.log("Start saving user login time==", LoggerEnum.INFO.name());
        boolean response = false;
        String lastLoginTime = (String) reqMap.get(JsonKey.CURRENT_LOGIN_TIME);
        String userId = (String) reqMap.get(JsonKey.USER_ID);
        reqMap.clear();
        reqMap.put(JsonKey.LAST_LOGIN_TIME, lastLoginTime);
        reqMap.put(JsonKey.CURRENT_LOGIN_TIME, Long.toString(System.currentTimeMillis()));
        response = ElasticSearchUtil.updateData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName(), userId, reqMap);
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        reqMap.put(JsonKey.ID, userId);
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), reqMap);
        ProjectLogger.log("End saving user login time== " + response, LoggerEnum.INFO.name());
        return response;
    }

    private String getLastLoginTime(String userId, String time) {
        String lastLoginTime = "";
        if (Boolean
                .parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED))) {
            SSOManager manager = SSOServiceFactory.getInstance();
            lastLoginTime = manager.getLastLoginTime(userId);
        } else {
            lastLoginTime = time;
        }
        if (StringUtils.isBlank(lastLoginTime)) {
            return "0";
        }
        return lastLoginTime;
    }

}
