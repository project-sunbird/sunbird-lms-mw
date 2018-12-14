package org.sunbird.user.actors;

import static org.sunbird.learner.util.Util.isNotNull;

import akka.actor.ActorRef;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
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
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;

@ActorConfig(
  tasks = {"getUserDetailsByLoginId", "getUserProfile", "getUserProfileV2"},
  asyncTasks = {}
)
public class UserProfileReadActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private DecryptionService decryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(
          null);
  private EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);
  private Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
  private Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
  private ActorRef systemSettingActorRef = null;

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    if (systemSettingActorRef == null) {
      systemSettingActorRef = getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue());
    }
    String operation = request.getOperation();
    switch (operation) {
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
        onReceiveUnsupportedOperation("UserProfileReadActor");
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
      ProjectCommonException.throwClientErrorException(ResponseCode.userAccountlocked);
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
      ProjectCommonException.throwServerErrorException(ResponseCode.userDataEncryptionError);
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
      updateTncInfo(result);
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

  @SuppressWarnings({"unchecked", "rawtypes"})
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

  private void updateUserOrgInfo(List<Map<String, Object>> userOrgs) {
    Map<String, Map<String, Object>> orgInfoMap = fetchAllOrgsById(userOrgs);
    Map<String, Map<String, Object>> locationInfoMap = fetchAllLocationsById(orgInfoMap);
    prepUserOrgInfoWithAdditionalData(userOrgs, orgInfoMap, locationInfoMap);
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

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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

  /**
   * Method to get user profile (version 2).
   *
   * @param actorMessage Request containing user ID
   */
  @SuppressWarnings("unchecked")
  private void getUserProfileV2(Request actorMessage) {
    Response response = getUserProfileData(actorMessage);
    SystemSettingClient systemSetting = new SystemSettingClientImpl();
    Object excludedFieldList =
        systemSetting.getSystemSettingByFieldAndKey(
            systemSettingActorRef,
            JsonKey.USER_PROFILE_CONFIG,
            JsonKey.SUNBIRD_USER_PROFILE_READ_EXCLUDED_FIELDS,
            new TypeReference<List<String>>() {});
    if (excludedFieldList != null) {
      removeExcludedFieldsFromUserProfileResponse(
          (Map<String, Object>) response.get(JsonKey.RESPONSE), (List<String>) excludedFieldList);
    } else {
      ProjectLogger.log(
          "UserProfileReadActor:getUserProfileV2: System setting userProfileConfig.read.excludedFields not configured.",
          LoggerEnum.INFO.name());
    }
    sender().tell(response, self());
  }

  private void removeExcludedFieldsFromUserProfileResponse(
      Map<String, Object> response, List<String> excludeFields) {
    if (CollectionUtils.isNotEmpty(excludeFields)) {
      for (String key : excludeFields) {
        response.remove(key);
      }
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
        updateTncInfo(result);
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

  private void updateTncInfo(Map<String, Object> result){
    SystemSettingClient systemSettingClient = new SystemSettingClientImpl();
    SystemSetting tncSystemSetting =
            systemSettingClient.getSystemSettingByField(
                    getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()), JsonKey.TNC_CONFIG);
    if (tncSystemSetting != null) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> tncCofigMap =
                mapper.readValue(tncSystemSetting.getValue(), Map.class);
        String tncLatestVersion = (String) tncCofigMap.get(JsonKey.LATEST_VERSION);
        result.put(JsonKey.TNC_LATEST_VERSION, tncLatestVersion);
        String tncUserAcceptedVersion = (String) result.get(JsonKey.TNC_ACCEPTED_VERSION);
        String tncUserAcceptedOn = (String) result.get(JsonKey.TNC_ACCEPTED_ON);
        if (StringUtils.isEmpty(tncUserAcceptedVersion)
                || !tncUserAcceptedVersion.equalsIgnoreCase(tncLatestVersion) || StringUtils.isEmpty(tncUserAcceptedOn) ) {
          result.put(JsonKey.PROMPT_TNC, true);
        } else {
          result.put(JsonKey.PROMPT_TNC, false);
        }

        if (tncCofigMap.containsKey(tncLatestVersion)) {
          String url = (String) ((Map) tncCofigMap.get(tncLatestVersion)).get(JsonKey.URL);
          result.put(JsonKey.TNC_LATEST_VERSION_URL, url);
        } else {
          result.put(JsonKey.PROMPT_TNC, false);
          ProjectLogger.log(
                  "UserManagementActor:getUserProfileData: URL is not present in config");
        }
      } catch (Exception e) {
        ProjectLogger.log(
                "UserManagementActor:getUserProfileData: Exception occurred during parse with error message = "
                        + e.getMessage(),
                LoggerEnum.ERROR.name());
        ProjectCommonException.throwServerErrorException(
                ResponseCode.errorConfigLoadParseString,
                ProjectUtil.formatMessage(
                        ResponseCode.errorConfigLoadParseString.getErrorMessage(), new Object[] {"tnc"}));
      }
    }
  }
}
