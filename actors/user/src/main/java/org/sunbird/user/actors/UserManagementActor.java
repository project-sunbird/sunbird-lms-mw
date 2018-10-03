package org.sunbird.user.actors;

import static org.sunbird.learner.util.Util.isNotNull;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.UserRequestValidator;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.dto.SearchDTO;
import org.sunbird.extension.user.UserExtension;
import org.sunbird.extension.user.impl.UserProviderRegistryImpl;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.SocialMediaType;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.models.user.User;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;

/**
 * This actor will handle course enrollment operation .
 *
 * @author Manzarul
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {
    "createUser",
    "updateUser",
    "getUserProfile",
    "getRoles",
    "getUserDetailsByLoginId",
    "profileVisibility",
    "unblockUser",
    "blockUser",
    "assignRoles",
    "userCurrentLogin",
    "getMediaTypes"
  },
  asyncTasks = {}
)
public class UserManagementActor extends BaseActor {
  private ObjectMapper mapper = new ObjectMapper();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private SSOManager ssoManager = SSOServiceFactory.getInstance();
  private EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);
  private DecryptionService decryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(
          null);
  private PropertiesCache propertiesCache = PropertiesCache.getInstance();
  private boolean isSSOEnabled =
      Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
  private Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
  private Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
  private static final boolean IS_REGISTRY_ENABLED =
      Boolean.parseBoolean(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_OPENSABER_BRIDGE_ENABLE));
  private ActorRef systemSettingActorRef =
      getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue());

  /** Receives the actor message and perform the course enrollment operation . */
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
    } else if (operation.equalsIgnoreCase(ActorOperations.GET_PROFILE.getValue())) {
      getUserProfile(request);
    } else if (operation.equalsIgnoreCase(ActorOperations.GET_ROLES.getValue())) {
      getRoles();
    } else if (operation.equalsIgnoreCase(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue())) {
      getUserDetailsByLoginId(request);
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
    } else if (operation.equalsIgnoreCase(ActorOperations.PROFILE_VISIBILITY.getValue())) {
      profileVisibility(request);
    } else {
      ProjectLogger.log("UNSUPPORTED OPERATION");
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
              ResponseCode.invalidOperationName.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  /**
   * This method will first check user exist with us or not. after that it will create private filed
   * Map, for creating private field map it will take store value from ES and then a separate map
   * for private field and remove those field from original map. if will user is sending some public
   * field list as well then it will take private field values from another ES index and update
   * values under original data.
   *
   * @param actorMessage
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void profileVisibility(Request actorMessage) {
    Map<String, Object> map = (Map) actorMessage.getRequest().get(JsonKey.USER);
    String userId = (String) map.get(JsonKey.USER_ID);
    List<String> privateList = (List) map.get(JsonKey.PRIVATE);
    List<String> publicList = (List) map.get(JsonKey.PUBLIC);

    // Remove duplicate entries from the list
    // Visibility of permanent fields cannot be changed
    if (CollectionUtils.isNotEmpty(privateList)) {
      privateList = privateList.stream().distinct().collect(Collectors.toList());
      Util.validateProfileVisibilityFields(
          privateList, JsonKey.PUBLIC_FIELDS, systemSettingActorRef);
    }

    if (CollectionUtils.isNotEmpty(publicList)) {
      publicList = publicList.stream().distinct().collect(Collectors.toList());
      Util.validateProfileVisibilityFields(
          publicList, JsonKey.PRIVATE_FIELDS, systemSettingActorRef);
    }

    Map<String, Object> esResult =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId);
    if (esResult == null || esResult.size() == 0) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    Map<String, Object> esPrivateResult =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.userprofilevisibility.getTypeName(),
            userId);
    Map<String, Object> responseMap = new HashMap<>();
    if (privateList != null && !privateList.isEmpty()) {
      responseMap = handlePrivateVisibility(privateList, esResult, esPrivateResult);
    }
    if (responseMap != null && !responseMap.isEmpty()) {
      Map<String, Object> privateDataMap = (Map<String, Object>) responseMap.get(JsonKey.DATA);
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
    Map<String, String> profileVisibilityMap =
        (Map<String, String>) esResult.get(JsonKey.PROFILE_VISIBILITY);
    if (null == profileVisibilityMap) {
      profileVisibilityMap = new HashMap<>();
    }
    if (privateList != null) {
      for (String key : privateList) {
        profileVisibilityMap.put(key, JsonKey.PRIVATE);
      }
    }
    if (publicList != null) {
      for (String key : publicList) {
        profileVisibilityMap.put(key, JsonKey.PUBLIC);
      }
      updateCassandraWithPrivateFiled(userId, profileVisibilityMap);
      esResult.put(JsonKey.PROFILE_VISIBILITY, profileVisibilityMap);
    }
    if (profileVisibilityMap.size() > 0) {
      updateCassandraWithPrivateFiled(userId, profileVisibilityMap);
      esResult.put(JsonKey.PROFILE_VISIBILITY, profileVisibilityMap);
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
    generateTeleEventForUser(null, userId, "profileVisibility");
  }

  private Map<String, Object> handlePrivateVisibility(
      List<String> privateFieldList, Map<String, Object> data, Map<String, Object> oldPrivateData) {
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
  private Map<String, Object> createPrivateFiledMap(Map<String, Object> map, List<String> fields) {
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
        } else if (field.contains(JsonKey.BADGE_ASSERTIONS + ".")) {
          privateMap.put(JsonKey.BADGE_ASSERTIONS, map.get(JsonKey.BADGE_ASSERTIONS));
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
  private void updateCassandraWithPrivateFiled(String userId, Map<String, String> privateFieldMap) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.ID, userId);
    reqMap.put(JsonKey.PROFILE_VISIBILITY, privateFieldMap);
    Response response =
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), reqMap);
    String val = (String) response.get(JsonKey.RESPONSE);
    ProjectLogger.log("Private field updated under cassandra==" + val);
  }

  /**
   * This method will first removed the remove the saved private data for the user and then it will
   * create new private data for that user.
   *
   * @param dataMap Map<String, Object> allData
   * @param privateDataMap Map<String, Object> only private data.
   * @param userId String
   * @return boolean
   */
  private boolean updateDataInES(
      Map<String, Object> dataMap, Map<String, Object> privateDataMap, String userId) {
    ElasticSearchUtil.createData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.userprofilevisibility.getTypeName(),
        userId,
        privateDataMap);
    ElasticSearchUtil.createData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        userId,
        dataMap);
    return true;
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
    if (Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED))) {
      boolean addedResponse = ssoManager.addUserLoginTime(userId);
      ProjectLogger.log("user login time added response is ==" + addedResponse);
    }
  }

  @SuppressWarnings("unchecked")
  private void getUserDetailsByLoginId(Request actorMessage) {
    Map<String, Object> userMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    if (null != userMap.get(JsonKey.LOGIN_ID)) {
      String loginId = (String) userMap.get(JsonKey.LOGIN_ID);
      try {
        loginId = encryptionService.encryptData((String) userMap.get(JsonKey.LOGIN_ID));
      } catch (Exception e) {
        ProjectCommonException exception =
            new ProjectCommonException(
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
      Map<String, Object> esResponse =
          ElasticSearchUtil.complexSearch(
              searchDto,
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.user.getTypeName());
      List<Map<String, Object>> userList =
          (List<Map<String, Object>>) esResponse.get(JsonKey.CONTENT);
      Map<String, Object> result = null;
      if (null != userList && !userList.isEmpty()) {
        result = userList.get(0);
      } else {
        throw new ProjectCommonException(
            ResponseCode.userNotFound.getErrorCode(),
            ResponseCode.userNotFound.getErrorMessage(),
            ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      }
      if (result == null || result.size() == 0) {
        throw new ProjectCommonException(
            ResponseCode.userNotFound.getErrorCode(),
            ResponseCode.userNotFound.getErrorMessage(),
            ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      }

      // check whether is_deletd true or false
      if (ProjectUtil.isNotNull(result)
          && result.containsKey(JsonKey.IS_DELETED)
          && ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))
          && (Boolean) result.get(JsonKey.IS_DELETED)) {
        throw new ProjectCommonException(
            ResponseCode.userAccountlocked.getErrorCode(),
            ResponseCode.userAccountlocked.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      fetchRootAndRegisterOrganisation(result);
      // having check for removing private filed from user , if call user and response
      // user data id is not same.
      String requestedById =
          (String) actorMessage.getRequest().getOrDefault(JsonKey.REQUESTED_BY, "");
      ProjectLogger.log(
          "requested By and requested user id == "
              + requestedById
              + "  "
              + (String) result.get(JsonKey.USER_ID));

      try {
        if (!(((String) result.get(JsonKey.USER_ID)).equalsIgnoreCase(requestedById))) {
          result = removeUserPrivateField(result);
        } else {
          // These values are set to ensure backward compatibility post introduction of global
          // settings in user profile visibility
          setCompleteProfileVisibilityMap(result);
          setDefaultUserProfileVisibility(result);

          // If the user requests his data then we are fetching the private data from
          // userprofilevisibility index
          // and merge it with user index data
          Map<String, Object> privateResult =
              ElasticSearchUtil.getDataByIdentifier(
                  ProjectUtil.EsIndex.sunbird.getIndexName(),
                  ProjectUtil.EsType.userprofilevisibility.getTypeName(),
                  (String) userMap.get(JsonKey.USER_ID));
          // fetch user external identity
          List<Map<String, String>> dbResExternalIds = fetchUserExternalIdentity(requestedById);
          result.put(JsonKey.EXTERNAL_IDS, dbResExternalIds);
          result.putAll(privateResult);
        }
      } catch (Exception e) {
        ProjectCommonException exception =
            new ProjectCommonException(
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
          List<String> requestFields = (List<String>) actorMessage.getRequest().get(JsonKey.FIELDS);
          if (requestFields != null) {
            if (!requestFields.contains(JsonKey.COMPLETENESS)) {
              result.remove(JsonKey.COMPLETENESS);
            }
            if (!requestFields.contains(JsonKey.MISSING_FIELDS)) {
              result.remove(JsonKey.MISSING_FIELDS);
            }
            if (requestFields.contains(JsonKey.LAST_LOGIN_TIME)) {
              result.put(
                  JsonKey.LAST_LOGIN_TIME,
                  Long.parseLong(
                      getLastLoginTime(
                          (String) userMap.get(JsonKey.USER_ID),
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
        UserUtility.decryptUserDataFrmES(result);
      } else {
        result = new HashMap<>();
        response.put(JsonKey.RESPONSE, result);
      }
      sender().tell(response, self());
      return;
    } else {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotFound.getErrorCode(),
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
        Map<String, Object> esResult =
            ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName(),
                rootOrgId);
        result.put(JsonKey.ROOT_ORG, esResult);
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
    Map<String, Object> userMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    Map<String, Object> result =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            (String) userMap.get(JsonKey.USER_ID));
    // check user found or not
    if (result == null || result.size() == 0) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    // check whether is_deletd true or false
    if (ProjectUtil.isNotNull(result)
        && result.containsKey(JsonKey.IS_DELETED)
        && ProjectUtil.isNotNull(result.get(JsonKey.IS_DELETED))
        && (Boolean) result.get(JsonKey.IS_DELETED)) {
      throw new ProjectCommonException(
          ResponseCode.userAccountlocked.getErrorCode(),
          ResponseCode.userAccountlocked.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    fetchRootAndRegisterOrganisation(result);
    // having check for removing private filed from user , if call user and response
    // user data id is not same.
    String requestedById =
        (String) actorMessage.getRequest().getOrDefault(JsonKey.REQUESTED_BY, "");
    ProjectLogger.log(
        "requested By and requested user id == "
            + requestedById
            + "  "
            + (String) userMap.get(JsonKey.USER_ID));
    try {
      if (!((String) userMap.get(JsonKey.USER_ID)).equalsIgnoreCase(requestedById)) {
        result = removeUserPrivateField(result);
      } else {
        // These values are set to ensure backward compatibility post introduction of global
        // settings in user profile visibility
        setCompleteProfileVisibilityMap(result);
        setDefaultUserProfileVisibility(result);

        // If the user requests his data then we are fetching the private data from
        // userprofilevisibility index
        // and merge it with user index data
        Map<String, Object> privateResult =
            ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.userprofilevisibility.getTypeName(),
                (String) userMap.get(JsonKey.USER_ID));
        // fetch user external identity
        List<Map<String, String>> dbResExternalIds =
            fetchUserExternalIdentity((String) userMap.get(JsonKey.USER_ID));
        result.put(JsonKey.EXTERNAL_IDS, dbResExternalIds);
        result.putAll(privateResult);
      }
    } catch (Exception e) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userDataEncryptionError.getErrorCode(),
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
          result.put(
              JsonKey.LAST_LOGIN_TIME,
              Long.parseLong(
                  getLastLoginTime(
                      (String) userMap.get(JsonKey.USER_ID),
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
      UserUtility.decryptUserDataFrmES(result);
      updateSkillWithEndoresmentCount(result);
      // loginId is used internally for checking the duplicate user
      result.remove(JsonKey.LOGIN_ID);
      result.remove(JsonKey.ENC_EMAIL);
      result.remove(JsonKey.ENC_PHONE);
      response.put(JsonKey.RESPONSE, result);
    } else {
      result = new HashMap<>();
      response.put(JsonKey.RESPONSE, result);
    }
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private void updateSkillWithEndoresmentCount(Map<String, Object> result) {
    if (MapUtils.isNotEmpty(result) && result.containsKey(JsonKey.SKILLS)) {
      List<Map<String, Object>> skillList = (List<Map<String, Object>>) result.get(JsonKey.SKILLS);
      for (Map<String, Object> skill : skillList) {
        skill.put(
            JsonKey.ENDORSEMENT_COUNT.toLowerCase(),
            (int) skill.getOrDefault(JsonKey.ENDORSEMENT_COUNT, 0));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, String>> fetchUserExternalIdentity(String userId) {
    Response response =
        cassandraOperation.getRecordsByIndexedProperty(
            JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, JsonKey.USER_ID, userId);
    List<Map<String, String>> dbResExternalIds = new ArrayList<>();
    if (null != response && null != response.getResult()) {
      dbResExternalIds = (List<Map<String, String>>) response.getResult().get(JsonKey.RESPONSE);
      if (null != dbResExternalIds) {
        dbResExternalIds
            .stream()
            .forEach(
                s -> {
                  if (StringUtils.isNotBlank(s.get(JsonKey.ORIGINAL_EXTERNAL_ID))
                      && StringUtils.isNotBlank(s.get(JsonKey.ORIGINAL_ID_TYPE))
                      && StringUtils.isNotBlank(s.get(JsonKey.ORIGINAL_PROVIDER))) {
                    s.put(
                        JsonKey.ID,
                        decryptionService.decryptData(s.get(JsonKey.ORIGINAL_EXTERNAL_ID)));
                    s.put(JsonKey.ID_TYPE, s.get(JsonKey.ORIGINAL_ID_TYPE));
                    s.put(JsonKey.PROVIDER, s.get(JsonKey.ORIGINAL_PROVIDER));

                  } else {
                    s.put(JsonKey.ID, decryptionService.decryptData(s.get(JsonKey.EXTERNAL_ID)));
                  }

                  s.remove(JsonKey.EXTERNAL_ID);
                  s.remove(JsonKey.ORIGINAL_EXTERNAL_ID);
                  s.remove(JsonKey.ORIGINAL_ID_TYPE);
                  s.remove(JsonKey.ORIGINAL_PROVIDER);
                  s.remove(JsonKey.CREATED_BY);
                  s.remove(JsonKey.LAST_UPDATED_BY);
                  s.remove(JsonKey.LAST_UPDATED_ON);
                  s.remove(JsonKey.CREATED_ON);
                  s.remove(JsonKey.USER_ID);
                  s.remove(JsonKey.SLUG);
                });
      }
    }
    return dbResExternalIds;
  }

  @SuppressWarnings("unchecked")
  private void fetchTopicOfAssociatedOrgs(Map<String, Object> result) {

    String userId = (String) result.get(JsonKey.ID);
    Map<String, Object> locationCache = new HashMap<>();
    Set<String> topicSet = new HashSet<>();

    // fetch all associated user orgs
    Response response1 =
        cassandraOperation.getRecordsByProperty(
            userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), JsonKey.USER_ID, userId);

    List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);

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

        Map<String, Object> esresult =
            ElasticSearchUtil.complexSearch(
                searchDTO,
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
                Response response3 =
                    cassandraOperation.getRecordById(
                        geoLocationDbInfo.getKeySpace(),
                        geoLocationDbInfo.getTableName(),
                        locationId);
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

  /** Method to update the user profile. */
  @SuppressWarnings("unchecked")
  private void updateUser(Request actorMessage) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> req = actorMessage.getRequest();
    Map<String, Object> requestMap = null;
    Map<String, Object> userMap = (Map<String, Object>) req.get(JsonKey.USER);
    actorMessage.getRequest().putAll(userMap);
    Util.getUserProfileConfig(systemSettingActorRef);
    UserRequestValidator.validateUpdateUser(actorMessage);
    Map<String, Object> userDbRecord = null;
    String extId = (String) userMap.get(JsonKey.EXTERNAL_ID);
    String provider = (String) userMap.get(JsonKey.EXTERNAL_ID_PROVIDER);
    String idType = (String) userMap.get(JsonKey.EXTERNAL_ID_TYPE);

    if ((StringUtils.isBlank((String) userMap.get(JsonKey.USER_ID))
            && StringUtils.isBlank((String) userMap.get(JsonKey.ID)))
        && StringUtils.isNotEmpty(extId)
        && StringUtils.isNotEmpty(provider)
        && StringUtils.isNotEmpty(idType)) {
      userDbRecord = Util.getUserFromExternalId(userMap);
      if (MapUtils.isEmpty(userDbRecord)) {
        throw new ProjectCommonException(
            ResponseCode.externalIdNotFound.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.externalIdNotFound.getErrorMessage(), extId, idType, provider),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      userMap.put(JsonKey.USER_ID, userDbRecord.get(JsonKey.USER_ID));
    }
    if (null != userMap.get(JsonKey.USER_ID)) {
      userMap.put(JsonKey.ID, userMap.get(JsonKey.USER_ID));
    } else {
      userMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    }

    if (isUserDeleted(userMap)) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.inactiveUser.getErrorCode(),
              ResponseCode.inactiveUser.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    try {
      User user = mapper.convertValue(userMap, User.class);
      if (CollectionUtils.isNotEmpty(user.getExternalIds())) {
        List<Map<String, String>> list =
            Util.copyAndConvertExternalIdsToLower(user.getExternalIds());
        user.setExternalIds(list);
        userMap.put(JsonKey.EXTERNAL_IDS, list);
      }
      Util.checkExternalIdUniqueness(user, JsonKey.UPDATE);
      Util.validateUserExternalIds(userMap);
    } catch (Exception ex) {
      sender().tell(ex, self());
      return;
    }
    // object of telemetry event...
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    if (userMap.containsKey(JsonKey.WEB_PAGES)) {
      SocialMediaType.validateSocialMedia(
          (List<Map<String, String>>) userMap.get(JsonKey.WEB_PAGES));
    }

    Util.checkPhoneUniqueness(userMap, JsonKey.UPDATE);
    Util.checkEmailUniqueness(userMap, JsonKey.UPDATE);

    // not allowing user to update the status,provider,userName
    removeFieldsFrmReq(userMap);

    if (!StringUtils.isBlank((String) userMap.get(JsonKey.EMAIL))) {
      boolean flag = checkEmailSameOrDiff(userMap);
      if (flag) {
        userMap.remove(JsonKey.EMAIL);
      }
    }

    /*
     * Update User Entity in Registry
     */
    if (IS_REGISTRY_ENABLED) {
      if (null == userDbRecord) {
        userDbRecord = Util.getUserbyUserId((String) userMap.get(JsonKey.USER_ID));
      }
      String registryId = (String) userDbRecord.get(JsonKey.REGISTRY_ID);
      UserExtension userExtension = new UserProviderRegistryImpl();
      if (StringUtils.isNotBlank(registryId)) {
        userMap.put(JsonKey.REGISTRY_ID, registryId);
        userExtension.update(userMap);
      } else {
        userExtension.create(userMap);
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
          new ProjectCommonException(
              ResponseCode.userDataEncryptionError.getErrorCode(),
              ResponseCode.userDataEncryptionError.getErrorMessage(),
              ResponseCode.SERVER_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    requestMap = new HashMap<>();
    User cassandraUser = mapper.convertValue(userMap, User.class);
    requestMap.putAll(mapper.convertValue(cassandraUser, Map.class));
    removeUnwanted(requestMap);

    Response result = null;
    try {
      result =
          cassandraOperation.updateRecord(
              usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), requestMap);
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

    // update the user external identity data
    try {
      Util.updateUserExtId(userMap);
    } catch (Exception ex) {
      result.getResult().put(JsonKey.ERROR_MSG, ex.getMessage());
    }

    sender().tell(result, self());

    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) userMap.get(JsonKey.USER_ID), JsonKey.USER, JsonKey.UPDATE, null);
    TelemetryUtil.telemetryProcessingCall(
        (Map<String, Object>) req.get(JsonKey.USER), targetObject, correlatedObject);

    if (((String) result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      Request userRequest = new Request();
      userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
      userRequest.getRequest().put(JsonKey.ID, userMap.get(JsonKey.ID));
      try {
        tellToAnother(userRequest);
      } catch (Exception ex) {
        ProjectLogger.log("Exception Occurred during saving user to Es while updating user : ", ex);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private boolean isUserDeleted(Map<String, Object> userMap) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Response response =
        cassandraOperation.getRecordById(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), (String) userMap.get(JsonKey.ID));
    List<Map<String, Object>> resList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
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
    userMap.remove(JsonKey.ROOT_ORG_ID);
    userMap.remove(JsonKey.LOGIN_ID);
    /**
     * Ignore all roles coming from req. With update user api, we are not allowing to update user
     * roles
     */
    userMap.remove(JsonKey.ROLES);
    // channel update is not allowed
    userMap.remove(JsonKey.CHANNEL);
  }

  @SuppressWarnings("unchecked")
  private void updateUserJobProfile(Map<String, Object> req, Map<String, Object> userMap) {
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
    List<Map<String, Object>> reqList =
        (List<Map<String, Object>>) userMap.get(JsonKey.JOB_PROFILE);
    for (Map<String, Object> reqMap : reqList) {
      if (reqMap.containsKey(JsonKey.IS_DELETED)
          && null != reqMap.get(JsonKey.IS_DELETED)
          && ((boolean) reqMap.get(JsonKey.IS_DELETED))
          && !StringUtils.isBlank((String) reqMap.get(JsonKey.ID))) {
        String addrsId = null;
        if (reqMap.containsKey(JsonKey.ADDRESS) && null != reqMap.get(JsonKey.ADDRESS)) {
          addrsId = (String) ((Map<String, Object>) reqMap.get(JsonKey.ADDRESS)).get(JsonKey.ID);
        } else {
          addrsId = getAddressId((String) reqMap.get(JsonKey.ID), jobProDbInfo);
        }
        if (null != addrsId) {
          deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);

          // genarate telemetry for user job profile address
          Map<String, Object> actions;
          if (reqMap.containsKey(JsonKey.ADDRESS) && null != reqMap.get(JsonKey.ADDRESS)) {
            actions = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
          } else {
            actions = new HashMap<>();
            actions.put("jobProfileAddressDeleted", "");
            actions.put(JsonKey.ID, addrsId);
          }
          telemetryGenerationForUserSubFieldsDeletion(
              actions, reqMap, JsonKey.ADDRESS, JsonKey.JOB_PROFILE);
        }
        deleteRecord(
            jobProDbInfo.getKeySpace(),
            jobProDbInfo.getTableName(),
            (String) reqMap.get(JsonKey.ID));
        telemetryGenerationForUserSubFieldsDeletion(
            reqMap, userMap, JsonKey.JOB_PROFILE, JsonKey.USER);
        continue;
      }
      processJobProfileInfo(reqMap, userMap, req, addrDbInfo, jobProDbInfo);
    }
  }

  @SuppressWarnings("unchecked")
  private void updateUserEducation(Map<String, Object> req, Map<String, Object> userMap) {
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
    List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap.get(JsonKey.EDUCATION);
    for (int i = 0; i < reqList.size(); i++) {
      Map<String, Object> reqMap = reqList.get(i);
      if (reqMap.containsKey(JsonKey.IS_DELETED)
          && null != reqMap.get(JsonKey.IS_DELETED)
          && ((boolean) reqMap.get(JsonKey.IS_DELETED))
          && !StringUtils.isBlank((String) reqMap.get(JsonKey.ID))) {
        String addrsId = null;
        if (reqMap.containsKey(JsonKey.ADDRESS) && null != reqMap.get(JsonKey.ADDRESS)) {
          addrsId = (String) ((Map<String, Object>) reqMap.get(JsonKey.ADDRESS)).get(JsonKey.ID);
        } else {
          addrsId = getAddressId((String) reqMap.get(JsonKey.ID), eduDbInfo);
        }
        if (null != addrsId) {
          // delete eductaion address
          deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
          Map<String, Object> actions;
          if (reqMap.containsKey(JsonKey.ADDRESS) && null != reqMap.get(JsonKey.ADDRESS)) {
            actions = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
          } else {
            actions = new HashMap<>();
            actions.put("educationAddressDeleted", "");
            actions.put(JsonKey.ID, addrsId);
          }
          telemetryGenerationForUserSubFieldsDeletion(
              actions, reqMap, JsonKey.ADDRESS, JsonKey.EDUCATION);
        }
        deleteRecord(
            eduDbInfo.getKeySpace(), eduDbInfo.getTableName(), (String) reqMap.get(JsonKey.ID));
        telemetryGenerationForUserSubFieldsDeletion(
            reqMap, userMap, JsonKey.EDUCATION, JsonKey.USER);
        continue;
      }
      processEducationInfo(reqMap, userMap, req, addrDbInfo, eduDbInfo);
    }
  }

  @SuppressWarnings("unchecked")
  private void updateUserAddress(Map<String, Object> req, Map<String, Object> userMap) {
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap.get(JsonKey.ADDRESS);
    for (int i = 0; i < reqList.size(); i++) {
      Map<String, Object> reqMap = reqList.get(i);
      if (reqMap.containsKey(JsonKey.IS_DELETED)
          && null != reqMap.get(JsonKey.IS_DELETED)
          && ((boolean) reqMap.get(JsonKey.IS_DELETED))
          && !StringUtils.isBlank((String) reqMap.get(JsonKey.ID))) {
        deleteRecord(
            addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), (String) reqMap.get(JsonKey.ID));
        telemetryGenerationForUserSubFieldsDeletion(reqMap, userMap, JsonKey.ADDRESS, JsonKey.USER);
        continue;
      }
      processUserAddress(reqMap, req, userMap, addrDbInfo);
    }
  }

  @SuppressWarnings("unchecked")
  private boolean checkEmailSameOrDiff(Map<String, Object> userMap) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Response response =
        cassandraOperation.getRecordById(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), (String) userMap.get(JsonKey.ID));
    List<Map<String, Object>> resList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
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

  private void processUserAddress(
      Map<String, Object> reqMap,
      Map<String, Object> req,
      Map<String, Object> userMap,
      DbInfo addrDbInfo) {
    Boolean isAddressUpdated = true;
    String encUserId = "";
    String encreqById = "";
    try {
      encUserId = encryptionService.encryptData((String) userMap.get(JsonKey.ID));
      encreqById = encryptionService.encryptData((String) req.get(JsonKey.REQUESTED_BY));
    } catch (Exception e1) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userDataEncryptionError.getErrorCode(),
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
      isAddressUpdated = false;
    } else {
      reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.UPDATED_BY, encreqById);
      reqMap.remove(JsonKey.USER_ID);
    }
    try {
      cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), reqMap);
      telemetryGenerationForUserSubFields(
          reqMap, userMap, isAddressUpdated, JsonKey.ADDRESS, JsonKey.USER);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  private String getAddressId(String id, DbInfo dbInfo) {
    String addressId = null;
    try {
      Response res =
          cassandraOperation.getPropertiesValueById(
              dbInfo.getKeySpace(), dbInfo.getTableName(), id, JsonKey.ADDRESS_ID);
      if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
        addressId =
            (String)
                (((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0))
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

  private void processJobProfileInfo(
      Map<String, Object> reqMap,
      Map<String, Object> userMap,
      Map<String, Object> req,
      DbInfo addrDbInfo,
      DbInfo jobProDbInfo) {
    String addrId = null;
    Boolean isProfileUpdated = true;
    Response addrResponse = null;
    if (!(reqMap.containsKey(JsonKey.ID))) {
      reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      isProfileUpdated = false;
    }
    if (reqMap.containsKey(JsonKey.ADDRESS)) {
      Boolean isProfileAddressUpdated = true;
      @SuppressWarnings("unchecked")
      Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
      if (!address.containsKey(JsonKey.ID)) {
        addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
        address.put(JsonKey.ID, addrId);
        address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
        isProfileAddressUpdated = false;
        isProfileUpdated = false;
      } else {
        addrId = (String) address.get(JsonKey.ID);
        address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
        address.remove(JsonKey.USER_ID);
      }
      try {
        addrResponse =
            cassandraOperation.upsertRecord(
                addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
        telemetryGenerationForUserSubFields(
            address, reqMap, isProfileAddressUpdated, JsonKey.ADDRESS, JsonKey.JOB_PROFILE);
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
    }
    if (null != addrResponse
        && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      reqMap.put(JsonKey.ADDRESS_ID, addrId);
      reqMap.remove(JsonKey.ADDRESS);
    }

    if (isProfileUpdated) {
      reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
      reqMap.remove(JsonKey.USER_ID);
    } else {
      reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    }
    try {
      cassandraOperation.upsertRecord(
          jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(), reqMap);
      telemetryGenerationForUserSubFields(
          reqMap, userMap, isProfileUpdated, JsonKey.JOB_PROFILE, JsonKey.USER);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  private void processEducationInfo(
      Map<String, Object> reqMap,
      Map<String, Object> userMap,
      Map<String, Object> req,
      DbInfo addrDbInfo,
      DbInfo eduDbInfo) {

    Boolean isEducationUpdated = true;
    Boolean isEducationAddressUpdated = true;
    String addrId = null;
    Response addrResponse = null;
    if (!(reqMap.containsKey(JsonKey.ID))) {
      reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      isEducationUpdated = false;
    }
    if (reqMap.containsKey(JsonKey.ADDRESS)) {
      @SuppressWarnings("unchecked")
      Map<String, Object> address = (Map<String, Object>) reqMap.get(JsonKey.ADDRESS);
      if (!address.containsKey(JsonKey.ID)) {
        addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
        address.put(JsonKey.ID, addrId);
        address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
        isEducationUpdated = false;
        isEducationAddressUpdated = false;
      } else {
        addrId = (String) address.get(JsonKey.ID);
        address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
        address.remove(JsonKey.USER_ID);
      }
      try {
        addrResponse =
            cassandraOperation.upsertRecord(
                addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
        telemetryGenerationForUserSubFields(
            address, reqMap, isEducationAddressUpdated, JsonKey.ADDRESS, JsonKey.EDUCATION);
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
      }
    }
    if (null != addrResponse
        && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      reqMap.put(JsonKey.ADDRESS_ID, addrId);
      reqMap.remove(JsonKey.ADDRESS);
    }
    try {
      if (null != reqMap.get(JsonKey.YEAR_OF_PASSING)) {
        reqMap.put(
            JsonKey.YEAR_OF_PASSING, ((BigInteger) reqMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
      } else {
        reqMap.put(JsonKey.YEAR_OF_PASSING, 0);
      }
    } catch (Exception ex) {
      reqMap.put(JsonKey.YEAR_OF_PASSING, 0);
      ProjectLogger.log(ex.getMessage(), ex);
    }
    try {
      if (null != reqMap.get(JsonKey.PERCENTAGE)) {
        reqMap.put(
            JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf(reqMap.get(JsonKey.PERCENTAGE))));
      } else {
        reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf("0")));
      }
    } catch (Exception ex) {
      reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf("0")));
      ProjectLogger.log(ex.getMessage(), ex);
    }

    if (isEducationUpdated) {
      reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
      reqMap.remove(JsonKey.USER_ID);
    } else {
      reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    }
    try {
      cassandraOperation.upsertRecord(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(), reqMap);
      telemetryGenerationForUserSubFields(
          reqMap, userMap, isEducationUpdated, JsonKey.EDUCATION, JsonKey.USER);
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
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), map);
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
      throw new ProjectCommonException(
          ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
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
    Map<String, Object> req = actorMessage.getRequest();
    Map<String, Object> userMap = (Map<String, Object>) req.get(JsonKey.USER);
    // remove these fields from req
    userMap.remove(JsonKey.ENC_EMAIL);
    userMap.remove(JsonKey.ENC_PHONE);
    userMap.remove(JsonKey.EMAIL_VERIFIED);
    userMap.put(JsonKey.CREATED_BY, req.get(JsonKey.REQUESTED_BY));
    actorMessage.getRequest().putAll(userMap);
    Util.getUserProfileConfig(systemSettingActorRef);
    try {
      String channel = Util.getCustodianChannel(userMap, systemSettingActorRef);
      String rootOrgId = Util.getRootOrgIdFromChannel(channel);
      userMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
      userMap.put(JsonKey.CHANNEL, channel);
    } catch (Exception ex) {
      sender().tell(ex, self());
      return;
    }
    String version = (String) actorMessage.getRequest().get(JsonKey.VERSION);
    if (StringUtils.isNotBlank(version) && JsonKey.VERSION_2.equalsIgnoreCase(version)) {
      UserRequestValidator.validateCreateUserV2(actorMessage);
      validateChannelAndOrganisationId(userMap);
    } else {
      // For V1
      UserRequestValidator.fieldsNotAllowed(Arrays.asList(JsonKey.ORGANISATION_ID), actorMessage);
      UserRequestValidator.validateCreateUser(actorMessage);
    }
    processUserRequest(userMap);
  }

  private void validateChannelAndOrganisationId(Map<String, Object> userMap) {
    String organisationId = (String) userMap.get(JsonKey.ORGANISATION_ID);
    if (StringUtils.isNotBlank(organisationId)) {
      Map<String, Object> orgMap = Util.getOrgDetails(organisationId);
      if (MapUtils.isEmpty(orgMap)) {
        ProjectCommonException.throwClientErrorException(ResponseCode.invalidOrgData, null);
      }
      String subOrgRootOrgId = "";
      if ((boolean) orgMap.get(JsonKey.IS_ROOT_ORG)) {
        subOrgRootOrgId = (String) orgMap.get(JsonKey.ID);
      } else {
        subOrgRootOrgId = (String) orgMap.get(JsonKey.ROOT_ORG_ID);
      }
      String rootOrgId = (String) userMap.get(JsonKey.ROOT_ORG_ID);
      if (!rootOrgId.equalsIgnoreCase(subOrgRootOrgId)) {
        ProjectCommonException.throwClientErrorException(
            ResponseCode.parameterMismatch,
            MessageFormat.format(
                ResponseCode.parameterMismatch.getErrorMessage(),
                StringFormatter.joinByComma(JsonKey.CHANNEL, JsonKey.ORGANISATION_ID)));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void processUserRequest(Map<String, Object> userMap) {
    ProjectLogger.log("processUserRequest method started..");
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Map<String, Object> requestMap = null;
    Util.checkPhoneUniqueness(userMap, JsonKey.CREATE);
    Util.checkEmailUniqueness(userMap, JsonKey.CREATE);
    Map<String, Object> emailTemplateMap = new HashMap<>(userMap);
    if (userMap.containsKey(JsonKey.WEB_PAGES)) {
      SocialMediaType.validateSocialMedia(
          (List<Map<String, String>>) userMap.get(JsonKey.WEB_PAGES));
    }
    // create loginId to ensure uniqueness for combination of userName and channel
    String loginId = Util.getLoginId(userMap);
    userMap.put(JsonKey.LOGIN_ID, loginId);
    emailTemplateMap.put(JsonKey.USERNAME, loginId);
    try {
      User user = mapper.convertValue(userMap, User.class);
      Util.checkUserExistOrNot(user);
      if (CollectionUtils.isNotEmpty(user.getExternalIds())) {
        List<Map<String, String>> list =
            Util.copyAndConvertExternalIdsToLower(user.getExternalIds());
        user.setExternalIds(list);
        userMap.put(JsonKey.EXTERNAL_IDS, list);
      }
      Util.checkExternalIdUniqueness(user, JsonKey.CREATE);
    } catch (Exception ex) {
      sender().tell(ex, self());
      return;
    }

    /** will ignore roles coming from req, Only public role is applicable for user by default */
    userMap.remove(JsonKey.ROLES);
    List<String> roles = new ArrayList<>();
    roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
    userMap.put(JsonKey.ROLES, roles);

    /*
     * Create User Entity in Registry
     */
    if (IS_REGISTRY_ENABLED) {
      UserExtension userExtension = new UserProviderRegistryImpl();
      userExtension.create(userMap);
    }

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
          ProjectCommonException exception =
              new ProjectCommonException(
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
      userMap.put(
          JsonKey.USER_ID, OneWayHashing.encryptVal((String) userMap.get(JsonKey.USERNAME)));
      userMap.put(JsonKey.ID, OneWayHashing.encryptVal((String) userMap.get(JsonKey.USERNAME)));
    }

    userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
    if (StringUtils.isNotBlank((String) userMap.get(JsonKey.PASSWORD))) {
      userMap.put(
          JsonKey.PASSWORD, OneWayHashing.encryptVal((String) userMap.get(JsonKey.PASSWORD)));
    }
    try {
      UserUtility.encryptUserData(userMap);
    } catch (Exception e1) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userDataEncryptionError.getErrorCode(),
              ResponseCode.userDataEncryptionError.getErrorMessage(),
              ResponseCode.SERVER_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    requestMap = new HashMap<>();
    User cassandraUser = mapper.convertValue(userMap, User.class);
    requestMap.putAll(mapper.convertValue(cassandraUser, Map.class));
    removeUnwanted(requestMap);
    // update db with emailVerified as false (default)
    requestMap.put(JsonKey.EMAIL_VERIFIED, false);

    // Since global settings are introduced, profile visibility map should be empty during user
    // creation
    requestMap.put(JsonKey.PROFILE_VISIBILITY, new HashMap<String, String>());

    if (!StringUtils.isBlank((String) requestMap.get(JsonKey.COUNTRY_CODE))) {
      requestMap.put(
          JsonKey.COUNTRY_CODE, propertiesCache.getProperty(JsonKey.SUNBIRD_DEFAULT_COUNTRY_CODE));
    }
    requestMap.put(JsonKey.IS_DELETED, false);
    Response response = null;
    try {
      response =
          cassandraOperation.insertRecord(
              usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), requestMap);
    } catch (ProjectCommonException exception) {
      sender().tell(exception, self());
      return;
    } finally {
      if (null == response && isSSOEnabled) {
        ssoManager.removeUser(userMap);
      }
      /*
       * Delete User Entity in Registry if cassandra insert fails
       */
      if (null == response && IS_REGISTRY_ENABLED) {
        UserExtension userExtension = new UserProviderRegistryImpl();
        userExtension.delete(userMap);
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
            encCreatedById =
                encryptionService.encryptData((String) userMap.get(JsonKey.CREATED_BY));
          } catch (Exception e) {
            ProjectCommonException exception =
                new ProjectCommonException(
                    ResponseCode.userDataEncryptionError.getErrorCode(),
                    ResponseCode.userDataEncryptionError.getErrorMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          }
          reqMap.put(JsonKey.CREATED_BY, encCreatedById);
          reqMap.put(JsonKey.USER_ID, encUserId);
          try {
            cassandraOperation.insertRecord(
                addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), reqMap);
            telemetryGenerationForUserSubFields(
                reqMap, userMap, false, JsonKey.ADDRESS, JsonKey.USER);
          } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
          }
        }
      }
      if (userMap.containsKey(JsonKey.EDUCATION)) {
        insertEducationDetails(userMap);
      }
      if (userMap.containsKey(JsonKey.JOB_PROFILE)) {
        insertJobProfileDetails(userMap);
      }
      registerUserToOrg(userMap);

      try {
        // update the user external identity data
        Util.addUserExtIds(userMap);
      } catch (Exception ex) {
        ProjectLogger.log(
            "UserManagementActor:processUserRequest: Exception occurred while updating user external identity table.",
            ex);
      }
    }

    response.put(JsonKey.ACCESSTOKEN, accessToken);
    sender().tell(response, self());

    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("UserManagementActor:processUserRequest: User creation success");
      Request userRequest = new Request();
      userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
      userRequest.getRequest().put(JsonKey.ID, userMap.get(JsonKey.ID));
      ProjectLogger.log(
          "UserManagementActor:processUserRequest: Trigger sync of user details to ES");
      try {
        tellToAnother(userRequest);
      } catch (Exception ex) {
        ProjectLogger.log(
            "UserManagementActor:processUserRequest: Exception occurred with error message = "
                + ex.getMessage(),
            ex);
      }
    } else {
      ProjectLogger.log("UserManagementActor:processUserRequest: User creation failure");
    }

    // object of telemetry event...
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) userMap.get(JsonKey.ID), JsonKey.USER, JsonKey.CREATE, null);
    TelemetryUtil.telemetryProcessingCall(userMap, targetObject, correlatedObject);
    sendEmailAndSms(userMap, emailTemplateMap);
  }

  private void sendEmailAndSms(Map<String, Object> userMap, Map<String, Object> emailTemplateMap) {
    // generate required action link and shorten the url
    UserUtility.decryptUserData(userMap);
    userMap.put(JsonKey.USERNAME, userMap.get(JsonKey.LOGIN_ID));
    userMap.put(JsonKey.REDIRECT_URI, Util.getSunbirdWebUrlPerTenent(userMap));
    Util.getUserRequiredActionLink(userMap);
    // user created successfully send the onboarding mail
    // putting rootOrgId to get web url per tenant while sending mail
    emailTemplateMap.put(JsonKey.ROOT_ORG_ID, userMap.get(JsonKey.ROOT_ORG_ID));
    emailTemplateMap.put(JsonKey.SET_PASSWORD_LINK, userMap.get(JsonKey.SET_PASSWORD_LINK));
    emailTemplateMap.put(JsonKey.VERIFY_EMAIL_LINK, userMap.get(JsonKey.VERIFY_EMAIL_LINK));
    emailTemplateMap.put(JsonKey.REDIRECT_URI, userMap.get(JsonKey.REDIRECT_URI));
    Request welcomeMailReqObj = Util.sendOnboardingMail(emailTemplateMap);
    if (null != welcomeMailReqObj) {
      tellToAnother(welcomeMailReqObj);
    }
    ProjectLogger.log("calling Send SMS method:", LoggerEnum.INFO);
    if (StringUtils.isNotBlank((String) userMap.get(JsonKey.PHONE))) {
      Util.sendSMS(userMap);
    }
  }

  private void registerUserToOrg(Map<String, Object> userMap) {
    // Register user to given orgId(not root orgId)
    String organisationId = (String) userMap.get(JsonKey.ORGANISATION_ID);
    if (StringUtils.isNotBlank(organisationId)) {
      String hashTagId = Util.getHashTagIdFromOrgId((String) userMap.get(JsonKey.ORGANISATION_ID));
      userMap.put(JsonKey.HASHTAGID, hashTagId);
      Util.registerUserToOrg(userMap);
    }
    if ((StringUtils.isNotBlank(organisationId)
            && !organisationId.equalsIgnoreCase((String) userMap.get(JsonKey.ROOT_ORG_ID)))
        || StringUtils.isBlank(organisationId)) {
      // Add user to root org
      userMap.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.ROOT_ORG_ID));
      String hashTagId = Util.getHashTagIdFromOrgId((String) userMap.get(JsonKey.ROOT_ORG_ID));
      userMap.put(JsonKey.HASHTAGID, hashTagId);
      Util.registerUserToOrg(userMap);
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
          addrResponse =
              cassandraOperation.insertRecord(
                  addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
          telemetryGenerationForUserSubFields(
              address, reqMap, false, JsonKey.ADDRESS, JsonKey.JOB_PROFILE);
        } catch (Exception e) {
          ProjectLogger.log(e.getMessage(), e);
        }
      }
      if (null != addrResponse
          && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
        reqMap.put(JsonKey.ADDRESS_ID, addrId);
        reqMap.remove(JsonKey.ADDRESS);
      }
      reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
      try {
        cassandraOperation.insertRecord(
            jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(), reqMap);
        telemetryGenerationForUserSubFields(
            reqMap, userMap, false, JsonKey.JOB_PROFILE, JsonKey.USER);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void insertEducationDetails(Map<String, Object> userMap) {
    Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
    List<Map<String, Object>> reqList = (List<Map<String, Object>>) userMap.get(JsonKey.EDUCATION);
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
          addrResponse =
              cassandraOperation.insertRecord(
                  addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
          telemetryGenerationForUserSubFields(
              address, reqMap, false, JsonKey.ADDRESS, JsonKey.EDUCATION);
        } catch (Exception e) {
          ProjectLogger.log(e.getMessage(), e);
        }
      }
      if (null != addrResponse
          && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
        reqMap.put(JsonKey.ADDRESS_ID, addrId);
        reqMap.remove(JsonKey.ADDRESS);
      }
      try {
        if (null != reqMap.get(JsonKey.YEAR_OF_PASSING)) {
          reqMap.put(
              JsonKey.YEAR_OF_PASSING,
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
          reqMap.put(
              JsonKey.PERCENTAGE,
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
        cassandraOperation.insertRecord(eduDbInfo.getKeySpace(), eduDbInfo.getTableName(), reqMap);
        telemetryGenerationForUserSubFields(
            reqMap, userMap, false, JsonKey.EDUCATION, JsonKey.USER);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }
  }

  private void telemetryGenerationForUserSubFields(
      Map<String, Object> reqMap,
      Map<String, Object> userMap,
      boolean isUpdated,
      String type,
      String correlatedOjectType) {

    String currentState = JsonKey.CREATE;
    if (isUpdated) {
      currentState = JsonKey.UPDATE;
    }
    Map<String, Object> targetObject =
        TelemetryUtil.generateTargetObject(
            (String) reqMap.get(JsonKey.ID), type, currentState, null);
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    TelemetryUtil.generateCorrelatedObject(
        (String) userMap.get(JsonKey.ID), correlatedOjectType, null, correlatedObject);
    TelemetryUtil.telemetryProcessingCall(reqMap, targetObject, correlatedObject);
  }

  private void telemetryGenerationForUserSubFieldsDeletion(
      Map<String, Object> reqMap,
      Map<String, Object> userMap,
      String type,
      String correlatedObjectType) {

    String currentState = JsonKey.DELETE;
    Map<String, Object> targetObject =
        TelemetryUtil.generateTargetObject(
            (String) reqMap.get(JsonKey.ID), type, currentState, null);
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    TelemetryUtil.generateCorrelatedObject(
        (String) userMap.get(JsonKey.ID), correlatedObjectType, null, correlatedObject);
    TelemetryUtil.telemetryProcessingCall(reqMap, targetObject, correlatedObject);
  }

  private void removeUnwanted(Map<String, Object> reqMap) {
    reqMap.remove(JsonKey.ADDRESS);
    reqMap.remove(JsonKey.EDUCATION);
    reqMap.remove(JsonKey.JOB_PROFILE);
    reqMap.remove(JsonKey.ORGANISATION);
    reqMap.remove(JsonKey.EMAIL_VERIFIED);
    reqMap.remove(JsonKey.REGISTERED_ORG);
    reqMap.remove(JsonKey.ROOT_ORG);
    reqMap.remove(JsonKey.IDENTIFIER);
    reqMap.remove(JsonKey.ORGANISATIONS);
    reqMap.remove(JsonKey.IS_DELETED);
    reqMap.remove(JsonKey.EXTERNAL_ID);
    reqMap.remove(JsonKey.ID_TYPE);
    reqMap.remove(JsonKey.EXTERNAL_ID_TYPE);
    reqMap.remove(JsonKey.PROVIDER);
    reqMap.remove(JsonKey.EXTERNAL_ID_PROVIDER);
    reqMap.remove(JsonKey.EXTERNAL_IDS);
    reqMap.remove(JsonKey.ORGANISATION_ID);
  }

  /** This method will provide the complete role structure.. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void getRoles() {
    Util.DbInfo roleDbInfo = Util.dbInfoMap.get(JsonKey.ROLE);
    Util.DbInfo roleGroupDbInfo = Util.dbInfoMap.get(JsonKey.ROLE_GROUP);
    Util.DbInfo urlActionDbInfo = Util.dbInfoMap.get(JsonKey.URL_ACTION);
    Response mergeResponse = new Response();
    List<Map<String, Object>> resposnemap = new ArrayList<>();
    List<Map<String, Object>> list = null;
    Response response =
        cassandraOperation.getAllRecords(roleDbInfo.getKeySpace(), roleDbInfo.getTableName());
    Response rolegroup =
        cassandraOperation.getAllRecords(
            roleGroupDbInfo.getKeySpace(), roleGroupDbInfo.getTableName());
    Response urlAction =
        cassandraOperation.getAllRecords(
            urlActionDbInfo.getKeySpace(), urlActionDbInfo.getTableName());
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
   * This method will find the action from role action mapping it will return action id, action name
   * and list of urls.
   *
   * @param urlActionListMap List<Map<String,Object>>
   * @param actionName String
   * @return Map<String,Object>
   */
  private Map<String, Object> getRoleAction(
      List<Map<String, Object>> urlActionListMap, String actionName) {
    Map<String, Object> response = new HashMap<>();
    if (urlActionListMap != null && !(urlActionListMap.isEmpty())) {
      for (Map<String, Object> map : urlActionListMap) {
        if (map.get(JsonKey.ID).equals(actionName)) {
          response.put(JsonKey.ID, map.get(JsonKey.ID));
          response.put(JsonKey.NAME, map.get(JsonKey.NAME));
          response.put(
              JsonKey.URLS,
              map.get(JsonKey.URL) != null ? map.get(JsonKey.URL) : new ArrayList<String>());
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
  private Map<String, Object> getSubRoleListMap(
      List<Map<String, Object>> urlActionListMap, String roleName) {
    Map<String, Object> response = new HashMap<>();
    if (urlActionListMap != null && !(urlActionListMap.isEmpty())) {
      for (Map<String, Object> map : urlActionListMap) {
        if (map.get(JsonKey.ID).equals(roleName)) {
          response.put(JsonKey.ID, map.get(JsonKey.ID));
          response.put(JsonKey.NAME, map.get(JsonKey.NAME));
          response.put(
              JsonKey.URL_ACTION_ID,
              map.get(JsonKey.URL_ACTION_ID) != null
                  ? map.get(JsonKey.URL_ACTION_ID)
                  : new ArrayList<>());
          return response;
        }
      }
    }
    return response;
  }

  /**
   * Method to block the user , it performs only soft delete from Cassandra , ES , Keycloak
   *
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void blockUser(Request actorMessage) {

    ProjectLogger.log("Method call  " + "deleteUser");
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> userMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    if (ProjectUtil.isNull(userMap.get(JsonKey.USER_ID))) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    String userId = (String) userMap.get(JsonKey.USER_ID);
    Map<String, Object> userDbRecord = Util.getUserbyUserId(userId);
    if (null == userDbRecord) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotFound.getErrorCode(),
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
    // delete from registry
    /*if (IS_REGISTRY_ENABLED) {
      Map<String, Object> regMap = new HashMap<>();
      regMap.put(JsonKey.REGISTRY_ID, userDbRecord.get(JsonKey.REGISTRY_ID));
      UserExtension userExtension = new UserProviderRegistryImpl();
      userExtension.delete(regMap);
    }*/
    // soft delete from cassandra--
    Response response =
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), dbMap);
    ProjectLogger.log("USER DELETED " + userId);
    sender().tell(response, self());

    // update record in elasticsearch ......
    dbMap.remove(JsonKey.ID);
    dbMap.remove(JsonKey.USER_ID);
    ElasticSearchUtil.updateData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        userId,
        dbMap);
    generateTeleEventForUser(null, userId, "blockUser");
  }

  /**
   * This method will assign roles to users or user organizations.
   *
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void assignRoles(Request actorMessage) {
    UserRequestValidator.validateAssignRole(actorMessage);
    Map<String, Object> requestMap = actorMessage.getRequest();

    if (null != requestMap.get(JsonKey.ROLES)
        && !((List<String>) requestMap.get(JsonKey.ROLES)).isEmpty()) {
      String msg = Util.validateRoles((List<String>) requestMap.get(JsonKey.ROLES));
      if (!msg.equalsIgnoreCase(JsonKey.SUCCESS)) {
        throw new ProjectCommonException(
            ResponseCode.invalidRole.getErrorCode(),
            ResponseCode.invalidRole.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }

    // object of telemetry event...
    String userId = (String) requestMap.get(JsonKey.USER_ID);
    String externalId = (String) requestMap.get(JsonKey.EXTERNAL_ID);
    String provider = (String) requestMap.get(JsonKey.PROVIDER);
    String organisationId = (String) requestMap.get(JsonKey.ORGANISATION_ID);
    String hashTagId = null;
    Map<String, Object> map = null;
    // have a check if organisation id is provided then need to get hashtagId
    if (StringUtils.isNotBlank(organisationId)) {
      map =
          ElasticSearchUtil.getDataByIdentifier(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName(),
              organisationId);
      if (MapUtils.isNotEmpty(map)) {
        hashTagId = (String) map.get(JsonKey.HASHTAGID);
        requestMap.put(JsonKey.HASHTAGID, hashTagId);
      }
    } else {
      SearchDTO searchDto = new SearchDTO();
      Map<String, Object> filter = new HashMap<>();
      filter.put(JsonKey.EXTERNAL_ID, externalId);
      filter.put(JsonKey.PROVIDER, provider);
      searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filter);
      Map<String, Object> esResponse =
          ElasticSearchUtil.complexSearch(
              searchDto,
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName());
      List<Map<String, Object>> list = (List<Map<String, Object>>) esResponse.get(JsonKey.CONTENT);

      if (!list.isEmpty()) {
        map = list.get(0);
        organisationId = (String) map.get(JsonKey.ID);
        requestMap.put(JsonKey.ORGANISATION_ID, organisationId);
        // get org hashTagId and keep inside request map.
        hashTagId = (String) map.get(JsonKey.HASHTAGID);
        requestMap.put(JsonKey.HASHTAGID, hashTagId);
      }
    }

    // throw error if provided orgId or ExtenralId with Provider is not valid
    if (MapUtils.isEmpty(map)) {
      String errorMsg =
          StringUtils.isNotEmpty(organisationId)
              ? ProjectUtil.formatMessage(
                  ResponseMessage.Message.INVALID_PARAMETER_VALUE,
                  organisationId,
                  JsonKey.ORGANISATION_ID)
              : ProjectUtil.formatMessage(
                  ResponseMessage.Message.INVALID_PARAMETER_VALUE,
                  StringFormatter.joinByComma(externalId, provider),
                  StringFormatter.joinByAnd(JsonKey.EXTERNAL_ID, JsonKey.PROVIDER));
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidParameterValue.getErrorCode(),
              errorMsg,
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // update userOrg role with requested roles.
    Map<String, Object> userOrgDBMap = new HashMap<>();
    userOrgDBMap.put(JsonKey.ORGANISATION_ID, organisationId);
    userOrgDBMap.put(JsonKey.USER_ID, userId);
    Util.DbInfo userOrgDb = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Response response =
        cassandraOperation.getRecordsByProperties(
            userOrgDb.getKeySpace(), userOrgDb.getTableName(), userOrgDBMap);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (list.isEmpty()) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidUsrOrgData.getErrorCode(),
              ResponseCode.invalidUsrOrgData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    // Add default role into Requested Roles if it is not provided and then update into DB
    List<String> roles = (List<String>) requestMap.get(JsonKey.ROLES);
    if (!roles.contains(ProjectUtil.UserRole.PUBLIC.name()))
      roles.add(ProjectUtil.UserRole.PUBLIC.name());
    userOrgDBMap.put(JsonKey.ROLES, roles);
    userOrgDBMap.put(JsonKey.ID, list.get(0).get(JsonKey.ID));
    if (StringUtils.isNotBlank(hashTagId)) {
      userOrgDBMap.put(JsonKey.HASHTAGID, hashTagId);
    }
    userOrgDBMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    userOrgDBMap.put(JsonKey.UPDATED_BY, requestMap.get(JsonKey.REQUESTED_BY));
    userOrgDBMap.put(JsonKey.ROLES, roles);
    response =
        cassandraOperation.updateRecord(
            userOrgDb.getKeySpace(), userOrgDb.getTableName(), userOrgDBMap);
    sender().tell(response, self());
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      updateRoleToEs(userOrgDBMap, JsonKey.ORGANISATION, userId, organisationId);
    } else {
      ProjectLogger.log("no call for ES to save user");
    }
    generateTeleEventForUser(requestMap, userId, "userLevel");
  }

  private void generateTeleEventForUser(
      Map<String, Object> requestMap, String userId, String objectType) {
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    Map<String, Object> targetObject =
        TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
    Map<String, Object> telemetryAction = new HashMap<>();
    if (objectType.equalsIgnoreCase("orgLevel")) {
      telemetryAction.put("AssignRole", "role assigned at org level");
      if (null != requestMap) {
        TelemetryUtil.generateCorrelatedObject(
            (String) requestMap.get(JsonKey.ORGANISATION_ID),
            JsonKey.ORGANISATION,
            null,
            correlatedObject);
      }
    } else {
      if (objectType.equalsIgnoreCase("userLevel")) {
        telemetryAction.put("AssignRole", "role assigned at user level");
      } else if (objectType.equalsIgnoreCase("blockUser")) {
        telemetryAction.put("BlockUser", "user blocked");
      } else if (objectType.equalsIgnoreCase("unBlockUser")) {
        telemetryAction.put("UnBlockUser", "user unblocked");
      } else if (objectType.equalsIgnoreCase("profileVisibility")) {
        telemetryAction.put("ProfileVisibility", "profile Visibility setting changed");
      }
    }
    TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, correlatedObject);
  }

  private void updateRoleToEs(
      Map<String, Object> tempMap, String type, String userid, String orgId) {

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
          "Exception Occurred during saving user to Es while joinUserOrganisation : ", ex);
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
    Util.getUserProfileConfig(systemSettingActorRef);
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> userMap = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
    if (ProjectUtil.isNull(userMap.get(JsonKey.USER_ID))) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    String userId = (String) userMap.get(JsonKey.USER_ID);
    Response resultFrUserId =
        cassandraOperation.getRecordById(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userId);
    List<Map<String, Object>> dbResult =
        (List<Map<String, Object>>) resultFrUserId.get(JsonKey.RESPONSE);
    if (dbResult.isEmpty()) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userNotFound.getErrorCode(),
              ResponseCode.userNotFound.getErrorMessage(),
              ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    Map<String, Object> dbUser = dbResult.get(0);
    if (dbUser.containsKey(JsonKey.IS_DELETED)
        && isNotNull(dbUser.get(JsonKey.IS_DELETED))
        && !((Boolean) dbUser.get(JsonKey.IS_DELETED))) {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.userAlreadyActive.getErrorCode(),
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
    Response response =
        cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), dbMap);
    ProjectLogger.log("USER UNLOCKED " + userId);
    sender().tell(response, self());

    // update record in elasticsearch ......
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("UserManagementActor:unBlockUser : updating user data to ES.");
      Request userRequest = new Request();
      userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
      userRequest.getRequest().put(JsonKey.ID, userId);
      try {
        tellToAnother(userRequest);
      } catch (Exception ex) {
        ProjectLogger.log(
            "UserManagementActor:unBlockUser : Exception occurred while unblocking user : ", ex);
      }
    } else {
      ProjectLogger.log("UserManagementActor:unBlockUser : no call for ES to save user");
    }
    generateTeleEventForUser(null, userId, "unBlockUser");
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

  private void getMediaTypes() {
    Response response = SocialMediaType.getMediaTypeFromDB();
    sender().tell(response, self());
  }

  private String getLastLoginTime(String userId, String time) {
    String lastLoginTime = "";
    if (Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED))) {
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

  private void setCompleteProfileVisibilityMap(Map<String, Object> userMap) {
    Map<String, String> profileVisibilityMap =
        (Map<String, String>) userMap.get(JsonKey.PROFILE_VISIBILITY);
    Map<String, String> completeProfileVisibilityMap =
        Util.getCompleteProfileVisibilityMap(profileVisibilityMap, systemSettingActorRef);
    userMap.put(JsonKey.PROFILE_VISIBILITY, completeProfileVisibilityMap);
  }

  private void setDefaultUserProfileVisibility(Map<String, Object> userMap) {
    userMap.put(
        JsonKey.DEFAULT_PROFILE_FIELD_VISIBILITY,
        ProjectUtil.getConfigValue(JsonKey.SUNBIRD_USER_PROFILE_FIELD_DEFAULT_VISIBILITY));
  }
}
