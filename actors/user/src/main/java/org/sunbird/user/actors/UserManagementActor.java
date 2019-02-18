package org.sunbird.user.actors;

import akka.actor.ActorRef;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.actorutil.location.impl.LocationClientImpl;
import org.sunbird.actorutil.org.OrganisationClient;
import org.sunbird.actorutil.org.impl.OrganisationClientImpl;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.UserRequestValidator;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.util.ContentStoreUtil;
import org.sunbird.extension.user.UserExtension;
import org.sunbird.extension.user.impl.UserProviderRegistryImpl;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.organisation.external.identity.service.OrgExternalService;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.models.organisation.Organisation;
import org.sunbird.models.user.User;
import org.sunbird.models.user.UserType;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.UserActorOperations;
import org.sunbird.user.util.UserUtil;

@ActorConfig(
  tasks = {"createUser", "updateUser"},
  asyncTasks = {}
)
public class UserManagementActor extends BaseActor {
  private ObjectMapper mapper = new ObjectMapper();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private SSOManager ssoManager = SSOServiceFactory.getInstance();
  private static final boolean IS_REGISTRY_ENABLED =
      Boolean.parseBoolean(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_OPENSABER_BRIDGE_ENABLE));
  private UserRequestValidator userRequestValidator = new UserRequestValidator();
  private UserService userService = UserServiceImpl.getInstance();
  private SystemSettingClient systemSettingClient = SystemSettingClientImpl.getInstance();
  private OrganisationClient organisationClient = new OrganisationClientImpl();
  private OrgExternalService orgExternalService = new OrgExternalService();
  private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private static InterServiceCommunication interServiceCommunication =
      InterServiceCommunicationFactory.getInstance();
  private ActorRef systemSettingActorRef = null;

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    cacheFrameworkFieldsConfig();
    if (systemSettingActorRef == null) {
      systemSettingActorRef = getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue());
    }
    String operation = request.getOperation();
    switch (operation) {
      case "createUser":
        createUser(request);
        break;
      case "updateUser":
        updateUser(request);
        break;
      default:
        onReceiveUnsupportedOperation("UserManagementActor");
    }
  }

  private void cacheFrameworkFieldsConfig() {
    if (MapUtils.isEmpty(DataCacheHandler.getFrameworkFieldsConfig())) {
      Map<String, List<String>> frameworkFieldsConfig =
          systemSettingClient.getSystemSettingByFieldAndKey(
              getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
              JsonKey.USER_PROFILE_CONFIG,
              JsonKey.FRAMEWORK,
              new TypeReference<Map<String, List<String>>>() {});
      DataCacheHandler.setFrameworkFieldsConfig(frameworkFieldsConfig);
    }
  }

  @SuppressWarnings("unchecked")
  private void updateUser(Request actorMessage) {
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    actorMessage.toLower();
    Util.getUserProfileConfig(systemSettingActorRef);
    String callerId = (String) actorMessage.getContext().get(JsonKey.CALLER_ID);
    boolean isPrivate = false;
    if (actorMessage.getContext().containsKey(JsonKey.PRIVATE)) {
      isPrivate = (boolean) actorMessage.getContext().get(JsonKey.PRIVATE);
    }
    if (!isPrivate) {
      actorMessage.getRequest().remove(JsonKey.USER_ORG);
      if (StringUtils.isNotBlank(callerId)) {
        userService.validateUploader(actorMessage);
      } else {
        userService.validateUserId(actorMessage);
      }
    }
    Map<String, Object> userMap = actorMessage.getRequest();
    userRequestValidator.validateUpdateUserRequest(actorMessage);
    Map<String, Object> userDbRecord = UserUtil.validateExternalIdsAndReturnActiveUser(userMap);
    validateUserFrameworkData(userMap, userDbRecord);
    validateUserTypeForUpdate(userMap);
    User user = mapper.convertValue(userMap, User.class);
    UserUtil.validateExternalIds(user, JsonKey.UPDATE);
    userMap.put(JsonKey.EXTERNAL_IDS, user.getExternalIds());
    UserUtil.validateUserPhoneEmailAndWebPages(user, JsonKey.UPDATE);
    // not allowing user to update the status,provider,userName
    removeFieldsFrmReq(userMap);
    // if we are updating email then need to update isEmailVerified flag inside keycloak
    UserUtil.checkEmailSameOrDiff(userMap, userDbRecord);
    convertValidatedLocationCodesToIDs(userMap);
    if (IS_REGISTRY_ENABLED) {
      UserUtil.updateUserToRegistry(userMap, (String) userDbRecord.get(JsonKey.REGISTRY_ID));
    }
    UserUtil.upsertUserInKeycloak(userMap, JsonKey.UPDATE);
    userMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    if (StringUtils.isBlank(callerId)) {
      userMap.put(JsonKey.UPDATED_BY, actorMessage.getContext().get(JsonKey.REQUESTED_BY));
    }
    Map<String, Object> requestMap = UserUtil.encryptUserData(userMap);
    removeUnwanted(requestMap);
    if (requestMap.containsKey(JsonKey.TNC_ACCEPTED_ON)) {
      requestMap.put(
          JsonKey.TNC_ACCEPTED_ON, new Timestamp((Long) requestMap.get(JsonKey.TNC_ACCEPTED_ON)));
    }
    Response response =
        cassandraOperation.updateRecord(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), requestMap);

    if (StringUtils.isNotBlank(callerId)) {
      userMap.put(JsonKey.ROOT_ORG_ID, actorMessage.getContext().get(JsonKey.ROOT_ORG_ID));
    }
    Response resp = null;
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      if (isPrivate) {
        updateUserOrganisations(actorMessage);
      }
      Map<String, Object> userRequest = new HashMap<>(userMap);
      userRequest.put(JsonKey.OPERATION_TYPE, JsonKey.UPDATE);
      resp = saveUserAttributes(userRequest);
    } else {
      ProjectLogger.log("UserManagementActor:updateUser: User update failure");
    }
    response.put(
        JsonKey.ERRORS,
        ((Map<String, Object>) resp.getResult().get(JsonKey.RESPONSE)).get(JsonKey.ERRORS));
    sender().tell(response, self());
    if (null != resp) {
      Map<String, Object> completeUserDetails = new HashMap<>(userDbRecord);
      completeUserDetails.putAll(requestMap);
      saveUserDetailsToEs(completeUserDetails);
    }
    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) userMap.get(JsonKey.USER_ID), JsonKey.USER, JsonKey.UPDATE, null);
    TelemetryUtil.telemetryProcessingCall(userMap, targetObject, correlatedObject);
  }

  private void updateUserOrganisations(Request actorMessage) {
    if (null != actorMessage.getRequest().get(JsonKey.USER_ORG)) {
      ProjectLogger.log(
          "UserManagementActor:updateUserOrganisations : "
              + "updateUserOrganisation Called with valide data",
          LoggerEnum.INFO);
      List<Map<String, Object>> orgList =
          (List<Map<String, Object>>) actorMessage.getRequest().get(JsonKey.USER_ORG);
      String userId = (String) actorMessage.getContext().get(JsonKey.USER_ID);
      List<Map<String, Object>> orgListDb = UserUtil.getUserOrgDetails(userId);
      Map<String, Object> orgDbMap = new HashMap<>();
      if (CollectionUtils.isNotEmpty(orgListDb)) {
        orgListDb.forEach(org -> orgDbMap.put((String) org.get(JsonKey.ORGANISATION_ID), org));
      }
      List<String> userOrgTobeRemoved = new ArrayList<>();

      for (Map<String, Object> org : orgList) {
        if (MapUtils.isNotEmpty(org)) {
          String id = (String) org.get(JsonKey.ORGANISATION_ID);
          org.put(JsonKey.USER_ID, userId);
          org.put(JsonKey.IS_DELETED, false);

          if (null != id && orgDbMap.containsKey(id)) {
            org.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            org.put(JsonKey.UPDATED_BY, actorMessage.getContext().get(JsonKey.REQUESTED_BY));
            UserUtil.updateUserOrg(
                org,
                new HashMap<String, Object>() {
                  {
                    put(JsonKey.ID, ((Map<String, Object>) orgDbMap.get(id)).get(JsonKey.ID));
                  }
                });
            orgDbMap.remove(id);
          } else {
            org.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
            org.put(JsonKey.ADDED_BY, actorMessage.getContext().get(JsonKey.REQUESTED_BY));
            org.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv()));
            UserUtil.createUserOrg(org);
          }
        }
      }
      Set<String> ids = orgDbMap.keySet();
      for (String id : ids) {
        userOrgTobeRemoved.add((String) ((Map<String, Object>) orgDbMap.get(id)).get(JsonKey.ID));
      }
      if (CollectionUtils.isNotEmpty(userOrgTobeRemoved)) {
        UserUtil.deleteUserOrg(userOrgTobeRemoved);
      }
      ProjectLogger.log(
          "UserManagementActor:updateUserOrganisations : " + "updateUserOrganisation Completed",
          LoggerEnum.INFO);
    }
  }

  private void validateUserTypeForUpdate(Map<String, Object> userMap) {
    if (userMap.containsKey(JsonKey.USER_TYPE)) {
      String userType = (String) userMap.get(JsonKey.USER_TYPE);
      if (UserType.TEACHER.getTypeName().equalsIgnoreCase(userType)) {
        String custodianChannel = null;
        String custodianRootOrgId = null;
        User user = userService.getUserById((String) userMap.get(JsonKey.USER_ID));
        try {
          custodianChannel =
              userService.getCustodianChannel(new HashMap<>(), systemSettingActorRef);
          custodianRootOrgId = userService.getRootOrgIdFromChannel(custodianChannel);
        } catch (Exception ex) {
          ProjectLogger.log(
              "UserManagementActor: validateUserTypeForUpdate :"
                  + " Exception Occured while fetching Custodian Org ",
              LoggerEnum.INFO);
        }
        if (StringUtils.isNotBlank(custodianRootOrgId)
            && user.getRootOrgId().equalsIgnoreCase(custodianRootOrgId)) {
          ProjectCommonException.throwClientErrorException(
              ResponseCode.errorTeacherCannotBelongToCustodianOrg,
              ResponseCode.errorTeacherCannotBelongToCustodianOrg.getErrorMessage());
        }
      } else {
        userMap.put(JsonKey.USER_TYPE, UserType.OTHER.getTypeName());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void validateUserFrameworkData(
      Map<String, Object> userRequestMap, Map<String, Object> userDbRecord) {
    if (userRequestMap.containsKey(JsonKey.FRAMEWORK)) {
      Map<String, Object> framework = (Map<String, Object>) userRequestMap.get(JsonKey.FRAMEWORK);
      List<String> frameworkIdList;
      if (framework.get(JsonKey.ID) instanceof String) {
        String frameworkIdString = (String) framework.remove(JsonKey.ID);
        frameworkIdList = new ArrayList<>();
        frameworkIdList.add(frameworkIdString);
        framework.put(JsonKey.ID, frameworkIdList);
      } else {
        frameworkIdList = (List<String>) framework.get(JsonKey.ID);
      }

      userRequestMap.put(JsonKey.FRAMEWORK, framework);
      List<String> frameworkFields =
          DataCacheHandler.getFrameworkFieldsConfig().get(JsonKey.FIELDS);
      List<String> frameworkMandatoryFields =
          DataCacheHandler.getFrameworkFieldsConfig().get(JsonKey.MANDATORY_FIELDS);
      userRequestValidator.validateMandatoryFrameworkFields(
          userRequestMap, frameworkFields, frameworkMandatoryFields);
      Map<String, Object> rootOrgMap =
          Util.getOrgDetails((String) userDbRecord.get(JsonKey.ROOT_ORG_ID));
      String hashtagId = (String) rootOrgMap.get(JsonKey.HASHTAGID);

      verifyFrameworkId(hashtagId, frameworkIdList);
      Map<String, List<Map<String, String>>> frameworkCachedValue =
          getFrameworkDetails(frameworkIdList.get(0));
      ((Map<String, Object>) userRequestMap.get(JsonKey.FRAMEWORK)).remove(JsonKey.ID);
      userRequestValidator.validateFrameworkCategoryValues(userRequestMap, frameworkCachedValue);
      ((Map<String, Object>) userRequestMap.get(JsonKey.FRAMEWORK))
          .put(JsonKey.ID, frameworkIdList);
    }
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
    String callerId = (String) actorMessage.getContext().get(JsonKey.CALLER_ID);
    String version = (String) actorMessage.getContext().get(JsonKey.VERSION);
    if (StringUtils.isNotBlank(version) && JsonKey.VERSION_2.equalsIgnoreCase(version)) {
      userRequestValidator.validateCreateUserV2Request(actorMessage);
      if (StringUtils.isNotBlank(callerId)) {
        userMap.put(JsonKey.ROOT_ORG_ID, actorMessage.getContext().get(JsonKey.ROOT_ORG_ID));
      }
    } else {
      userRequestValidator.validateCreateUserV1Request(actorMessage);
    }
    validateChannelAndOrganisationId(userMap);

    // remove these fields from req
    userMap.remove(JsonKey.ENC_EMAIL);
    userMap.remove(JsonKey.ENC_PHONE);
    actorMessage.getRequest().putAll(userMap);
    Util.getUserProfileConfig(systemSettingActorRef);
    boolean isCustodianOrg = false;
    if (StringUtils.isBlank(callerId)) {
      userMap.put(JsonKey.CREATED_BY, actorMessage.getContext().get(JsonKey.REQUESTED_BY));
      try {
        if (StringUtils.isBlank((String) userMap.get(JsonKey.CHANNEL))
            && StringUtils.isBlank((String) userMap.get(JsonKey.ROOT_ORG_ID))) {
          String channel = userService.getCustodianChannel(userMap, systemSettingActorRef);
          String rootOrgId = userService.getRootOrgIdFromChannel(channel);
          userMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
          userMap.put(JsonKey.CHANNEL, channel);
          isCustodianOrg = true;
        }
      } catch (Exception ex) {
        sender().tell(ex, self());
        return;
      }
    }
    validateUserType(userMap, isCustodianOrg);
    if (userMap.containsKey(JsonKey.ORG_EXTERNAL_ID)) {
      String orgExternalId = (String) userMap.get(JsonKey.ORG_EXTERNAL_ID);
      String channel = (String) userMap.get(JsonKey.CHANNEL);
      String orgId =
          orgExternalService.getOrgIdFromOrgExternalIdAndProvider(orgExternalId, channel);
      if (StringUtils.isBlank(orgId)) {
        ProjectLogger.log(
            "UserManagementActor:createUser: No organisation with orgExternalId = "
                + orgExternalId
                + " and channel = "
                + channel,
            LoggerEnum.ERROR.name());
        ProjectCommonException.throwClientErrorException(
            ResponseCode.invalidParameterValue,
            MessageFormat.format(
                ResponseCode.invalidParameterValue.getErrorMessage(),
                orgExternalId,
                JsonKey.ORG_EXTERNAL_ID));
      }
      if (userMap.containsKey(JsonKey.ORGANISATION_ID)
          && !orgId.equals((String) userMap.get(JsonKey.ORGANISATION_ID))) {
        ProjectLogger.log(
            "UserManagementActor:createUser Mismatch of organisation from orgExternalId="
                + orgExternalId
                + " and channel="
                + channel
                + " as organisationId="
                + orgId
                + " and request organisationId="
                + userMap.get(JsonKey.ORGANISATION_ID),
            LoggerEnum.ERROR.name());
        throwParameterMismatchException(JsonKey.ORG_EXTERNAL_ID, JsonKey.ORGANISATION_ID);
      }
      userMap.remove(JsonKey.ORG_EXTERNAL_ID);
      userMap.put(JsonKey.ORGANISATION_ID, orgId);
    }
    processUserRequest(userMap, callerId);
  }

  private void validateUserType(Map<String, Object> userMap, boolean isCustodianOrg) {
    String userType = (String) userMap.get(JsonKey.USER_TYPE);
    if (StringUtils.isNotBlank(userType)) {
      if (userType.equalsIgnoreCase(UserType.TEACHER.getTypeName()) && isCustodianOrg) {
        ProjectCommonException.throwClientErrorException(
            ResponseCode.errorTeacherCannotBelongToCustodianOrg,
            ResponseCode.errorTeacherCannotBelongToCustodianOrg.getErrorMessage());
      } else if (UserType.TEACHER.getTypeName().equalsIgnoreCase(userType)) {
        String custodianChannel = null;
        String custodianRootOrgId = null;
        try {
          custodianChannel =
              userService.getCustodianChannel(new HashMap<>(), systemSettingActorRef);
          custodianRootOrgId = userService.getRootOrgIdFromChannel(custodianChannel);
        } catch (Exception ex) {
          ProjectLogger.log(
              "UserManagementActor: validateUserType :"
                  + " Exception Occured while fetching Custodian Org ",
              LoggerEnum.INFO);
        }
        if (StringUtils.isNotBlank(custodianRootOrgId)
            && ((String) userMap.get(JsonKey.ROOT_ORG_ID)).equalsIgnoreCase(custodianRootOrgId)) {
          ProjectCommonException.throwClientErrorException(
              ResponseCode.errorTeacherCannotBelongToCustodianOrg,
              ResponseCode.errorTeacherCannotBelongToCustodianOrg.getErrorMessage());
        }
      }
    } else {
      userMap.put(JsonKey.USER_TYPE, UserType.OTHER.getTypeName());
    }
  }

  private void validateChannelAndOrganisationId(Map<String, Object> userMap) {
    String organisationId = (String) userMap.get(JsonKey.ORGANISATION_ID);
    String requestedChannel = (String) userMap.get(JsonKey.CHANNEL);

    String subOrgRootOrgId = "";
    if (StringUtils.isNotBlank(organisationId)) {
      Organisation organisation = organisationClient.esGetOrgById(organisationId);
      if (null == organisation) {
        ProjectCommonException.throwClientErrorException(ResponseCode.invalidOrgData);
      }
      if (organisation.isRootOrg()) {
        subOrgRootOrgId = organisation.getId();
        if (StringUtils.isNotBlank(requestedChannel)
            && !requestedChannel.equalsIgnoreCase(organisation.getChannel())) {
          throwParameterMismatchException(JsonKey.CHANNEL, JsonKey.ORGANISATION_ID);
        }
        userMap.put(JsonKey.CHANNEL, organisation.getChannel());
      } else {
        subOrgRootOrgId = organisation.getRootOrgId();
        Organisation subOrgRootOrg = organisationClient.esGetOrgById(subOrgRootOrgId);
        if (null != subOrgRootOrg) {
          if (StringUtils.isNotBlank(requestedChannel)
              && !requestedChannel.equalsIgnoreCase(subOrgRootOrg.getChannel())) {
            throwParameterMismatchException(JsonKey.CHANNEL, JsonKey.ORGANISATION_ID);
          }
          userMap.put(JsonKey.CHANNEL, subOrgRootOrg.getChannel());
        }
      }
      userMap.put(JsonKey.ROOT_ORG_ID, subOrgRootOrgId);
    }
    String rootOrgId = "";
    if (StringUtils.isNotBlank(requestedChannel)) {
      rootOrgId = userService.getRootOrgIdFromChannel(requestedChannel);
      if (StringUtils.isNotBlank(subOrgRootOrgId) && !rootOrgId.equalsIgnoreCase(subOrgRootOrgId)) {
        throwParameterMismatchException(JsonKey.CHANNEL, JsonKey.ORGANISATION_ID);
      }
      userMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
    }
  }

  private void throwParameterMismatchException(String... param) {
    ProjectCommonException.throwClientErrorException(
        ResponseCode.parameterMismatch,
        MessageFormat.format(
            ResponseCode.parameterMismatch.getErrorMessage(), StringFormatter.joinByComma(param)));
  }

  @SuppressWarnings("unchecked")
  private void processUserRequest(Map<String, Object> userMap, String callerId) {
    Map<String, Object> requestMap = null;
    UserUtil.setUserDefaultValue(userMap, callerId);
    User user = mapper.convertValue(userMap, User.class);
    UserUtil.validateExternalIds(user, JsonKey.CREATE);
    userMap.put(JsonKey.EXTERNAL_IDS, user.getExternalIds());
    UserUtil.validateUserPhoneEmailAndWebPages(user, JsonKey.CREATE);
    convertValidatedLocationCodesToIDs(userMap);
    if (IS_REGISTRY_ENABLED) {
      UserExtension userExtension = new UserProviderRegistryImpl();
      userExtension.create(userMap);
    }
    UserUtil.toLower(userMap);
    UserUtil.upsertUserInKeycloak(userMap, JsonKey.CREATE);
    requestMap = UserUtil.encryptUserData(userMap);
    removeUnwanted(requestMap);
    requestMap.put(JsonKey.IS_DELETED, false);
    Response response = null;
    try {
      response =
          cassandraOperation.insertRecord(
              usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), requestMap);
    } finally {
      if (null == response) {
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
      userRequest.put(JsonKey.CALLER_ID, callerId);
      resp = saveUserAttributes(userRequest);
    } else {
      ProjectLogger.log("UserManagementActor:processUserRequest: User creation failure");
    }
    // Enable this when you want to send full response of user attributes
    Map<String, Object> esResponse = new HashMap<>();
    esResponse.putAll((Map<String, Object>) resp.getResult().get(JsonKey.RESPONSE));
    esResponse.putAll(requestMap);
    response.put(
        JsonKey.ERRORS,
        ((Map<String, Object>) resp.getResult().get(JsonKey.RESPONSE)).get(JsonKey.ERRORS));
    sender().tell(response, self());
    if (null != resp) {
      saveUserDetailsToEs(esResponse);
    }
    requestMap.put(JsonKey.PASSWORD, userMap.get(JsonKey.PASSWORD));
    if (StringUtils.isNotBlank(callerId)) {
      sendEmailAndSms(requestMap);
    }
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();

    targetObject =
        TelemetryUtil.generateTargetObject(
            (String) userMap.get(JsonKey.ID), JsonKey.USER, JsonKey.CREATE, null);
    TelemetryUtil.telemetryProcessingCall(userMap, targetObject, correlatedObject);
  }

  private void convertValidatedLocationCodesToIDs(Map<String, Object> userMap) {
    if (userMap.containsKey(JsonKey.LOCATION_CODES)
        && !CollectionUtils.isEmpty((List<String>) userMap.get(JsonKey.LOCATION_CODES))) {
      LocationClientImpl locationClient = new LocationClientImpl();
      List<String> locationIdList =
          locationClient.getRelatedLocationIds(
              getActorRef(LocationActorOperation.GET_RELATED_LOCATION_IDS.getValue()),
              (List<String>) userMap.get(JsonKey.LOCATION_CODES));
      if (locationIdList != null && !locationIdList.isEmpty()) {
        userMap.put(JsonKey.LOCATION_IDS, locationIdList);
        userMap.remove(JsonKey.LOCATION_CODES);
      } else {
        ProjectCommonException.throwClientErrorException(
            ResponseCode.invalidParameterValue,
            MessageFormat.format(
                ResponseCode.invalidParameterValue.getErrorMessage(),
                JsonKey.LOCATION_CODES,
                (List<String>) userMap.get(JsonKey.LOCATION_CODES)));
      }
    }
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

  @SuppressWarnings("unchecked")
  public static void verifyFrameworkId(String hashtagId, List<String> frameworkIdList) {
    List<String> frameworks = DataCacheHandler.getHashtagIdFrameworkIdMap().get(hashtagId);
    String frameworkId = frameworkIdList.get(0);
    if (frameworks != null && frameworks.contains(frameworkId)) {
      return;
    } else {
      Map<String, List<Map<String, String>>> frameworkDetails = getFrameworkDetails(frameworkId);
      if (frameworkDetails == null)
        throw new ProjectCommonException(
            ResponseCode.errorNoFrameworkFound.getErrorCode(),
            ResponseCode.errorNoFrameworkFound.getErrorMessage(),
            ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
  }

  public static Map<String, List<Map<String, String>>> getFrameworkDetails(String frameworkId) {
    if (DataCacheHandler.getFrameworkCategoriesMap().get(frameworkId) == null) {
      handleGetFrameworkDetails(frameworkId);
    }
    return DataCacheHandler.getFrameworkCategoriesMap().get(frameworkId);
  }

  @SuppressWarnings("unchecked")
  private static void handleGetFrameworkDetails(String frameworkId) {
    Map<String, Object> response = ContentStoreUtil.readFramework(frameworkId);
    Map<String, List<Map<String, String>>> frameworkCacheMap = new HashMap<>();
    List<String> supportedfFields = DataCacheHandler.getFrameworkFieldsConfig().get(JsonKey.FIELDS);
    Map<String, Object> result = (Map<String, Object>) response.get(JsonKey.RESULT);
    if (MapUtils.isNotEmpty(result)) {
      Map<String, Object> frameworkDetails = (Map<String, Object>) result.get(JsonKey.FRAMEWORK);
      if (MapUtils.isNotEmpty(frameworkDetails)) {
        List<Map<String, Object>> frameworkCategories =
            (List<Map<String, Object>>) frameworkDetails.get(JsonKey.CATEGORIES);
        if (CollectionUtils.isNotEmpty(frameworkCategories)) {
          for (Map<String, Object> frameworkCategoriesValue : frameworkCategories) {
            String frameworkField = (String) frameworkCategoriesValue.get(JsonKey.CODE);
            if (supportedfFields.contains(frameworkField)) {
              List<Map<String, String>> listOfFields = new ArrayList<>();
              List<Map<String, Object>> frameworkTermList =
                  (List<Map<String, Object>>) frameworkCategoriesValue.get(JsonKey.TERMS);
              if (CollectionUtils.isNotEmpty(frameworkTermList)) {
                for (Map<String, Object> frameworkTerm : frameworkTermList) {
                  String id = (String) frameworkTerm.get(JsonKey.IDENTIFIER);
                  String name = (String) frameworkTerm.get(JsonKey.NAME);
                  Map<String, String> writtenValue = new HashMap<>();
                  writtenValue.put(JsonKey.ID, id);
                  writtenValue.put(JsonKey.NAME, name);
                  listOfFields.add(writtenValue);
                }
              }
              if (StringUtils.isNotBlank(frameworkField)
                  && CollectionUtils.isNotEmpty(listOfFields))
                frameworkCacheMap.put(frameworkField, listOfFields);
            }
            if (MapUtils.isNotEmpty(frameworkCacheMap))
              DataCacheHandler.updateFrameworkCategoriesMap(frameworkId, frameworkCacheMap);
          }
        }
      }
    }
  }
}
