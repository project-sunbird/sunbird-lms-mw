package org.sunbird.user.actors;

import static org.sunbird.learner.util.Util.isNotNull;

import akka.actor.ActorRef;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
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
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.UserRequestValidator;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.extension.user.UserExtension;
import org.sunbird.extension.user.impl.UserProviderRegistryImpl;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.UserActorOperations;
import org.sunbird.user.util.UserUtil;

@ActorConfig(
  tasks = {
    "createUser",
    "updateUser",
    "getUserDetailsByLoginId",
    "getUserProfile",
    "getUserProfileV2"
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
  private boolean isSSOEnabled =
      Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
  private Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
  private Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
  private static final boolean IS_REGISTRY_ENABLED =
      Boolean.parseBoolean(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_OPENSABER_BRIDGE_ENABLE));
  private UserRequestValidator userRequestValidator = new UserRequestValidator();
  private UserService userService = new UserServiceImpl();
  private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private static InterServiceCommunication interServiceCommunication =
      InterServiceCommunicationFactory.getInstance();
  private ActorRef systemSettingActorRef = null;

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    if (systemSettingActorRef == null) {
      ProjectLogger.log(
          "UserManagementActor:onReceive: systemSettingActorRef is initialised first time.",
          LoggerEnum.INFO.name());
      systemSettingActorRef = getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue());
    } else {
      ProjectLogger.log(
          "UserManagementActor:onReceive: systemSettingActorRef is already initialised.",
          LoggerEnum.INFO.name());
    }
    String operation = request.getOperation();
    switch (operation) {
      case "createUser":
        createUser(request);
        break;
      case "updateUser":
        updateUser(request);
        break;
      case "getUserProfile":
        getUserProfile(request);
        break;
      case "getUserProfileV2":
        getUserProfileV2(request);
        break;
      case "getUserDetailsByLoginId":
        getUserDetailsByLoginId(request);
        break;
      default:
        onReceiveUnsupportedOperation("UserManagementActor");
        break;
    }
  }

  @SuppressWarnings("unchecked")
  private void getUserDetailsByLoginId(Request actorMessage) {
    actorMessage.toLower();
    Map<String, Object> userMap = actorMessage.getRequest();
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
          (String) actorMessage.getContext().getOrDefault(JsonKey.REQUESTED_BY, "");
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
            addExtraFieldsInUserProfileResponse(
                result, String.join(",", requestFields), (String) userMap.get(JsonKey.USER_ID));
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

  private void updateUserOrgInfo(List<Map<String, Object>> userOrgs) {
    Map<String, Map<String, Object>> orgInfoMap = fetchAllOrgsById(userOrgs);
    Map<String, Map<String, Object>> locationInfoMap = fetchAllLocationsById(orgInfoMap);
    prepUserOrgInfoWithAdditionalData(userOrgs, orgInfoMap, locationInfoMap);
  }

  private void updateRoleMasterInfo(Map<String, Object> result) {
    Set<Entry<String, Object>> roleSet = DataCacheHandler.getRoleMap().entrySet();
    List<Map<String, String>> roleList = new ArrayList<>();
    roleSet
        .parallelStream()
        .forEach(
            (roleSetItem) -> {
              Map<String, String> roleMap = new HashMap<>();
              roleMap.put(JsonKey.ID, roleSetItem.getKey());
              roleMap.put(JsonKey.NAME, (String) roleSetItem.getValue());
              roleList.add(roleMap);
            });
    result.put(JsonKey.ROLE_LIST, roleList);
  }

  private void prepUserOrgInfoWithAdditionalData(
      List<Map<String, Object>> userOrgs,
      Map<String, Map<String, Object>> orgInfoMap,
      Map<String, Map<String, Object>> locationInfoMap) {
    for (Map<String, Object> usrOrg : userOrgs) {
      Map<String, Object> orgInfo = orgInfoMap.get(usrOrg.get(JsonKey.ORGANISATION_ID));
      usrOrg.put(JsonKey.ORG_NAME, orgInfo.get(JsonKey.ORG_NAME));
      usrOrg.put(JsonKey.CHANNEL, orgInfo.get(JsonKey.CHANNEL));
      usrOrg.put(JsonKey.HASHTAGID, orgInfo.get(JsonKey.HASHTAGID));
      usrOrg.put(JsonKey.LOCATION_IDS, orgInfo.get(JsonKey.LOCATION_IDS));
      usrOrg.put(
          JsonKey.LOCATIONS,
          prepLocationFields((List<String>) orgInfo.get(JsonKey.LOCATION_IDS), locationInfoMap));
    }
  }

  private void removeExcludedFieldsFromUserProfileResponse(
      Map<String, Object> response, List<String> excludeFields) {
    if (CollectionUtils.isNotEmpty(excludeFields)) {
      for (String key : excludeFields) {
        response.remove(key);
      }
    }
  }

  private Map<String, Map<String, Object>> fetchAllOrgsById(List<Map<String, Object>> userOrgs) {
    List<String> orgIds =
        userOrgs
            .stream()
            .map(m -> (String) m.get(JsonKey.ORGANISATION_ID))
            .distinct()
            .collect(Collectors.toList());
    List<String> fields =
        Arrays.asList(
            JsonKey.ORG_NAME, JsonKey.CHANNEL, JsonKey.HASHTAGID, JsonKey.LOCATION_IDS, JsonKey.ID);

    Map<String, Map<String, Object>> orgInfoMap =
        getEsResultByListOfIds(orgIds, fields, EsType.organisation);
    return orgInfoMap;
  }

  private Map<String, Map<String, Object>> fetchAllLocationsById(
      Map<String, Map<String, Object>> orgInfoMap) {
    List<String> searchLocations = new ArrayList<>();
    for (Map<String, Object> org : orgInfoMap.values()) {
      List<String> locations = (List<String>) org.get(JsonKey.LOCATION_IDS);
      if (locations != null) {
        for (String location : locations) {
          if (!searchLocations.contains(location)) {
            searchLocations.add(location);
          }
        }
      }
    }
    List<String> locationFields =
        Arrays.asList(JsonKey.CODE, JsonKey.NAME, JsonKey.TYPE, JsonKey.PARENT_ID, JsonKey.ID);
    Map<String, Map<String, Object>> locationInfoMap =
        getEsResultByListOfIds(searchLocations, locationFields, EsType.location);
    return locationInfoMap;
  }

  private List<Map<String, Object>> prepLocationFields(
      List<String> locationIds, Map<String, Map<String, Object>> locationInfoMap) {
    List<Map<String, Object>> retList = new ArrayList<>();
    if (locationIds != null) {
      for (String locationId : locationIds) {
        retList.add(locationInfoMap.get(locationId));
      }
    }
    return retList;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Map<String, Object>> getEsResultByListOfIds(
      List<String> orgIds, List<String> fields, EsType typeToSearch) {

    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.ID, orgIds);

    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    searchDTO.setFields(fields);

    Map<String, Object> result =
        ElasticSearchUtil.complexSearch(
            searchDTO, ProjectUtil.EsIndex.sunbird.getIndexName(), typeToSearch.getTypeName());

    List<Map<String, Object>> esContent = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
    return esContent
        .stream()
        .collect(
            Collectors.toMap(
                obj -> {
                  return (String) obj.get("id");
                },
                val -> val));
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
   * Method to get user profile (version 1).
   *
   * @param actorMessage Request containing user ID
   */
  private void getUserProfile(Request actorMessage) {
    Response response = getUserProfileData(actorMessage);
    sender().tell(response, self());
  }

  /**
   * Method to get user profile (version 2).
   *
   * @param actorMessage Request containing user ID
   */
  private void getUserProfileV2(Request actorMessage) {
    Response response = getUserProfileData(actorMessage);
    SystemSettingClient systemSetting = new SystemSettingClientImpl();
    Object excludeFieldList =
        systemSetting.getSystemSettingByFieldAndKey(
            systemSettingActorRef,
            JsonKey.USER_PROFILE_CONFIG,
            JsonKey.SUNBIRD_USER_PROFILE_READ_EXCLUDED_FIELDS,
            new TypeReference<List<String>>() {});
    if (excludeFieldList != null) {
      removeExcludedFieldsFromUserProfileResponse(
          (Map<String, Object>) response.get(JsonKey.RESPONSE), (List<String>) excludeFieldList);
    } else {
      ProjectLogger.log(
          "UserManagementActor:getUserProfileV2: System setting userProfileConfig.read.excludedFields not configured.",
          LoggerEnum.INFO.name());
    }
    sender().tell(response, self());
  }

  private Response getUserProfileData(Request actorMessage) {
    Map<String, Object> userMap = actorMessage.getRequest();
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
        (String) actorMessage.getContext().getOrDefault(JsonKey.REQUESTED_BY, "");
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
      throw new ProjectCommonException(
          ResponseCode.userDataEncryptionError.getErrorCode(),
          ResponseCode.userDataEncryptionError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    if (null != actorMessage.getContext().get(JsonKey.FIELDS)) {
      String requestFields = (String) actorMessage.getContext().get(JsonKey.FIELDS);
      addExtraFieldsInUserProfileResponse(
          result, requestFields, (String) userMap.get(JsonKey.USER_ID));
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
    return response;
  }

  private void addExtraFieldsInUserProfileResponse(
      Map<String, Object> result, String fields, String userId) {
    if (!StringUtils.isBlank(fields)) {
      if (!fields.contains(JsonKey.COMPLETENESS)) {
        result.remove(JsonKey.COMPLETENESS);
      }
      if (!fields.contains(JsonKey.MISSING_FIELDS)) {
        result.remove(JsonKey.MISSING_FIELDS);
      }
      if (fields.contains(JsonKey.LAST_LOGIN_TIME)) {
        result.put(
            JsonKey.LAST_LOGIN_TIME,
            Long.parseLong(getLastLoginTime(userId, (String) result.get(JsonKey.LAST_LOGIN_TIME))));
      } else {
        result.remove(JsonKey.LAST_LOGIN_TIME);
      }
      if (fields.contains(JsonKey.TOPIC)) {
        // fetch the topic details of all user associated orgs and append in the result
        fetchTopicOfAssociatedOrgs(result);
      }
      if (fields.contains(JsonKey.ORGANISATIONS)) {
        updateUserOrgInfo((List) result.get(JsonKey.ORGANISATIONS));
      }
      if (fields.contains(JsonKey.ROLES)) {
        updateRoleMasterInfo(result);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void updateSkillWithEndoresmentCount(Map<String, Object> result) {
    if (MapUtils.isNotEmpty(result) && result.containsKey(JsonKey.SKILLS)) {
      List<Map<String, Object>> skillList = (List<Map<String, Object>>) result.get(JsonKey.SKILLS);
      if (CollectionUtils.isEmpty(skillList)) {
        return;
      }
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

  private void updateUser(Request actorMessage) {
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    actorMessage.toLower();
    Util.getUserProfileConfig(systemSettingActorRef);
    userService.validateUserId(actorMessage);
    Map<String, Object> userMap = actorMessage.getRequest();
    userRequestValidator.validateUpdateUserRequest(actorMessage);
    Map<String, Object> userDbRecord = UserUtil.validateExternalIdsAndReturnActiveUser(userMap);

    User user = mapper.convertValue(userMap, User.class);
    UserUtil.validateExternalIds(user, JsonKey.UPDATE);
    userMap.put(JsonKey.EXTERNAL_IDS, user.getExternalIds());
    UserUtil.validateUserPhoneEmailAndWebPages(user, JsonKey.UPDATE);
    // not allowing user to update the status,provider,userName
    removeFieldsFrmReq(userMap);
    // if we are updating email then need to update isEmailVerified flag inside keycloak
    UserUtil.checkEmailSameOrDiff(userMap, userDbRecord);
    if (IS_REGISTRY_ENABLED) {
      UserUtil.updateUserToRegistry(userMap, (String) userDbRecord.get(JsonKey.REGISTRY_ID));
    }

    if (isSSOEnabled) {
      UserUtil.upsertUserInKeycloak(userMap, JsonKey.UPDATE);
    }
    userMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    userMap.put(JsonKey.UPDATED_BY, actorMessage.getContext().get(JsonKey.REQUESTED_BY));
    Map<String, Object> requestMap = UserUtil.encryptUserData(userMap);
    removeUnwanted(requestMap);
    Response response =
        cassandraOperation.updateRecord(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), requestMap);
    Response resp = null;
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      Map<String, Object> userRequest = new HashMap<>(userMap);
      userRequest.put(JsonKey.OPERATION_TYPE, JsonKey.UPDATE);
      resp = saveUserAttributes(userRequest);
    }
    // Enable this when you want to send full response of user attributes
    // response.putAll(resp.getResult());
    sender().tell(response, self());
    if (null != resp) {
      Map<String, Object> completeUserDetails = new HashMap<>(userDbRecord);
      completeUserDetails.putAll(requestMap);
      saveUserDetailsToEs(completeUserDetails);
    } else {
      ProjectLogger.log("UserManagementActor:updateUser: User update fail");
    }
    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) userMap.get(JsonKey.USER_ID), JsonKey.USER, JsonKey.UPDATE, null);
    TelemetryUtil.telemetryProcessingCall(userMap, targetObject, correlatedObject);
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

  /**
   * Method to create the new user , Username should be unique .
   *
   * @param actorMessage Request
   */
  private void createUser(Request actorMessage) {
    actorMessage.toLower();
    Map<String, Object> userMap = actorMessage.getRequest();
    String version = (String) actorMessage.getContext().get(JsonKey.VERSION);
    if (StringUtils.isNotBlank(version) && JsonKey.VERSION_2.equalsIgnoreCase(version)) {
      userRequestValidator.validateCreateUserV2Request(actorMessage);
      validateChannelAndOrganisationId(userMap);
    } else {
      userRequestValidator.validateCreateUserV1Request(actorMessage);
    }
    // remove these fields from req
    userMap.remove(JsonKey.ENC_EMAIL);
    userMap.remove(JsonKey.ENC_PHONE);
    userMap.remove(JsonKey.EMAIL_VERIFIED);
    userMap.put(JsonKey.CREATED_BY, actorMessage.getContext().get(JsonKey.REQUESTED_BY));
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

  private void processUserRequest(Map<String, Object> userMap) {
    Map<String, Object> requestMap = null;
    UserUtil.setUserDefaultValue(userMap);
    User user = mapper.convertValue(userMap, User.class);
    UserUtil.checkUserExistOrNot(user);
    UserUtil.validateExternalIds(user, JsonKey.CREATE);
    userMap.put(JsonKey.EXTERNAL_IDS, user.getExternalIds());
    UserUtil.validateUserPhoneEmailAndWebPages(user, JsonKey.CREATE);
    if (IS_REGISTRY_ENABLED) {
      UserExtension userExtension = new UserProviderRegistryImpl();
      userExtension.create(userMap);
    }

    if (isSSOEnabled) {
      UserUtil.upsertUserInKeycloak(userMap, JsonKey.CREATE);
    } else {
      userMap.put(
          JsonKey.USER_ID, OneWayHashing.encryptVal((String) userMap.get(JsonKey.USERNAME)));
      userMap.put(JsonKey.ID, OneWayHashing.encryptVal((String) userMap.get(JsonKey.USERNAME)));
    }
    if (StringUtils.isNotBlank((String) userMap.get(JsonKey.PASSWORD))) {
      userMap.put(JsonKey.PASSWORD, null);
    }
    requestMap = UserUtil.encryptUserData(userMap);
    removeUnwanted(requestMap);
    Response response = null;
    try {
      response =
          cassandraOperation.insertRecord(
              usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), requestMap);
    } finally {
      if (null == response && isSSOEnabled) {
        ssoManager.removeUser(userMap);
      }
      if (null == response && IS_REGISTRY_ENABLED) {
        UserExtension userExtension = new UserProviderRegistryImpl();
        userExtension.delete(userMap);
      }
      response.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    }
    Response resp = null;
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      Map<String, Object> userRequest = new HashMap<>();
      userRequest.putAll(userMap);
      userRequest.put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
      resp = saveUserAttributes(userRequest);
    } else {
      ProjectLogger.log("UserManagementActor:processUserRequest: User creation failure");
    }
    // Enable this when you want to send full response of user attributes
    // response.putAll(resp.getResult());
    sender().tell(response, self());
    if (null != resp) {
      saveUserDetailsToEs(userMap);
    }
    sendEmailAndSms(requestMap);
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) userMap.get(JsonKey.ID), JsonKey.USER, JsonKey.CREATE, null);
    TelemetryUtil.telemetryProcessingCall(userMap, targetObject, correlatedObject);
  }

  private void sendEmailAndSms(Map<String, Object> userMap) {
    // sendEmailAndSms
    Request EmailAndSmsRequest = new Request();
    EmailAndSmsRequest.getRequest().putAll(userMap);
    EmailAndSmsRequest.setOperation(UserActorOperations.PROCESS_ONBOARDING_MAIL_AND_SMS.getValue());
    tellToAnother(EmailAndSmsRequest);
  }

  private void saveUserDetailsToEs(Map<String, Object> completeUserMap) {
    Request userRequest = new Request();
    userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
    userRequest.getRequest().put(JsonKey.ID, completeUserMap.get(JsonKey.ID));
    ProjectLogger.log(
        "UserManagementActor:saveUserDetailsToEs: Trigger sync of user details to ES");
    tellToAnother(userRequest);
  }

  private Response saveUserAttributes(Map<String, Object> userMap) {
    Request request = new Request();
    request.setOperation(UserActorOperations.SAVE_USER_ATTRIBUTES.getValue());
    request.getRequest().putAll(userMap);
    ProjectLogger.log("UserManagementActor:saveUserAttributes");
    try {
      return (Response)
          interServiceCommunication.getResponse(
              getActorRef(UserActorOperations.SAVE_USER_ATTRIBUTES.getValue()), request);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return null;
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

  @SuppressWarnings("unchecked")
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
