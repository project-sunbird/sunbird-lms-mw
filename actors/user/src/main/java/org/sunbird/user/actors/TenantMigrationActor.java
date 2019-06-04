package org.sunbird.user.actors;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.organisation.external.identity.service.OrgExternalService;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.UserActorOperations;
import org.sunbird.user.util.UserUtil;

/**
 * This class contains method and business logic to migrate user from custodian org to some other
 * root org.
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {"userTenantMigrate"},
  asyncTasks = {}
)
public class TenantMigrationActor extends BaseActor {
  private UserService userService = UserServiceImpl.getInstance();
  private OrgExternalService orgExternalService = new OrgExternalService();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private Util.DbInfo usrOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
  private static InterServiceCommunication interServiceCommunication =
      InterServiceCommunicationFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private ActorRef systemSettingActorRef = null;

  @Override
  public void onReceive(Request request) throws Throwable {
    ProjectLogger.log("TenantMigrationActor:onReceive called.", LoggerEnum.INFO.name());
    String operation = request.getOperation();
    if (systemSettingActorRef == null) {
      systemSettingActorRef = getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue());
    }
    switch (operation) {
      case "userTenantMigrate":
        migrateUser(request);
        break;
      default:
        onReceiveUnsupportedOperation("TenantMigrationActor");
    }
  }

  @SuppressWarnings("unchecked")
  private void migrateUser(Request request) {
    ProjectLogger.log("TenantMigrationActor:migrateUser called.", LoggerEnum.INFO.name());
    Map<String, Object> userDetails =
        userService.esGetPublicUserProfileById((String) request.getRequest().get(JsonKey.USER_ID));
    validateUserCustodianOrgId((String) userDetails.get(JsonKey.ROOT_ORG_ID));
    validateChannelAndGetRootOrgId(request);
    String orgId = validateOrgExternalIdOrOrgIdAndGetOrgId(request.getRequest());
    request.getRequest().put(JsonKey.ORG_ID, orgId);
    Map<String, Object> userUpdateRequest = createUserUpdateRequest(request);
    // Update user channel and rootOrgId
    Response response =
        cassandraOperation.updateRecord(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userUpdateRequest);
    if (null == response
        || null == (String) response.get(JsonKey.RESPONSE)
        || (null != (String) response.get(JsonKey.RESPONSE)
            && !((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS))) {
      // throw exception for migration failed
      ProjectCommonException.throwServerErrorException(ResponseCode.errorUserMigrationFailed);
    }
    ProjectLogger.log(
        "TenantMigrationActor:migrateUser user record got updated.", LoggerEnum.INFO.name());
    // Update user org details
    Response userOrgResponse =
        updateUserOrg(request, (List<Map<String, Object>>) userDetails.get(JsonKey.ORGANISATIONS));
    // Update user externalIds
    Response userExternalIdsResponse = updateUserExternalIds(request);
    // Collect all the error message
    List<Map<String, Object>> userOrgErrMsgList = new ArrayList<>();
    if (MapUtils.isNotEmpty(userOrgResponse.getResult())
        && CollectionUtils.isNotEmpty(
            (List<Map<String, Object>>) userOrgResponse.getResult().get(JsonKey.ERRORS))) {
      userOrgErrMsgList =
          (List<Map<String, Object>>) userOrgResponse.getResult().get(JsonKey.ERRORS);
    }
    List<Map<String, Object>> userExtIdErrMsgList = new ArrayList<>();
    if (MapUtils.isNotEmpty(userExternalIdsResponse.getResult())
        && CollectionUtils.isNotEmpty(
            (List<Map<String, Object>>) userExternalIdsResponse.getResult().get(JsonKey.ERRORS))) {
      userExtIdErrMsgList =
          (List<Map<String, Object>>) userExternalIdsResponse.getResult().get(JsonKey.ERRORS);
    }
    userOrgErrMsgList.addAll(userExtIdErrMsgList);
    response.getResult().put(JsonKey.ERRORS, userOrgErrMsgList);
    // send the response
    sender().tell(response, self());
    // save user data to ES
    saveUserDetailsToEs((String) request.getRequest().get(JsonKey.USER_ID));
  }

  private String validateOrgExternalIdOrOrgIdAndGetOrgId(Map<String, Object> migrateReq) {
    ProjectLogger.log(
        "TenantMigrationActor:validateOrgExternalIdOrOrgIdAndGetOrgId called.",
        LoggerEnum.INFO.name());
    String orgId = "";
    if (StringUtils.isNotBlank((String) migrateReq.get(JsonKey.ORG_ID))
        || StringUtils.isNotBlank((String) migrateReq.get(JsonKey.ORG_EXTERNAL_ID))) {
      if (StringUtils.isNotBlank((String) migrateReq.get(JsonKey.ORG_ID))) {
        orgId = (String) migrateReq.get(JsonKey.ORG_ID);
        Map<String, Object> result =
            ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName(),
                orgId);
        if (MapUtils.isEmpty(result)) {
          ProjectLogger.log(
              "TenantMigrationActor:validateOrgExternalIdOrOrgIdAndGetOrgId called. OrgId is Invalid",
              LoggerEnum.INFO.name());
          ProjectCommonException.throwClientErrorException(ResponseCode.invalidOrgId);
        } else {
          String reqOrgRootOrgId = (String) result.get(JsonKey.ROOT_ORG_ID);
          if (StringUtils.isNotBlank(reqOrgRootOrgId)
              && !reqOrgRootOrgId.equalsIgnoreCase((String) migrateReq.get(JsonKey.ROOT_ORG_ID))) {
            ProjectCommonException.throwClientErrorException(
                ResponseCode.parameterMismatch,
                MessageFormat.format(
                    ResponseCode.parameterMismatch.getErrorMessage(),
                    StringFormatter.joinByComma(JsonKey.CHANNEL, JsonKey.ORG_ID)));
          }
        }
      } else if (StringUtils.isNotBlank((String) migrateReq.get(JsonKey.ORG_EXTERNAL_ID))) {
        orgId =
            orgExternalService.getOrgIdFromOrgExternalIdAndProvider(
                (String) migrateReq.get(JsonKey.ORG_EXTERNAL_ID),
                (String) migrateReq.get(JsonKey.CHANNEL));
        if (StringUtils.isBlank(orgId)) {
          ProjectLogger.log(
              "TenantMigrationActor:validateOrgExternalIdOrOrgIdAndGetOrgId called. OrgExternalId is Invalid",
              LoggerEnum.INFO.name());
          ProjectCommonException.throwClientErrorException(
              ResponseCode.invalidParameterValue,
              MessageFormat.format(
                  ResponseCode.invalidParameterValue.getErrorMessage(),
                  JsonKey.ORG_EXTERNAL_ID,
                  (String) migrateReq.get(JsonKey.ORG_EXTERNAL_ID)));
        }
      }
    }
    return orgId;
  }

  private void validateUserCustodianOrgId(String rootOrgId) {
    String custodianOrgId = userService.getCustodianOrgId(systemSettingActorRef);
    if (!rootOrgId.equalsIgnoreCase(custodianOrgId)) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.parameterMismatch,
          MessageFormat.format(
              ResponseCode.parameterMismatch.getErrorMessage(),
              "user rootOrgId and custodianOrgId"));
    }
  }

  private void saveUserDetailsToEs(String userId) {
    Request userRequest = new Request();
    userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
    userRequest.getRequest().put(JsonKey.ID, userId);
    ProjectLogger.log(
        "TenantMigrationActor:saveUserDetailsToEs: Trigger sync of user details to ES",
        LoggerEnum.INFO.name());
    tellToAnother(userRequest);
  }

  private Response updateUserExternalIds(Request request) {
    ProjectLogger.log("TenantMigrationActor:updateUserExternalIds called.", LoggerEnum.INFO.name());
    Response response = new Response();
    Map<String, Object> userExtIdsReq = new HashMap<>();
    userExtIdsReq.put(JsonKey.ID, request.getRequest().get(JsonKey.USER_ID));
    userExtIdsReq.put(JsonKey.USER_ID, request.getRequest().get(JsonKey.USER_ID));
    userExtIdsReq.put(JsonKey.EXTERNAL_IDS, request.getRequest().get(JsonKey.EXTERNAL_IDS));
    try {
      User user = mapper.convertValue(userExtIdsReq, User.class);
      UserUtil.validateExternalIds(user, JsonKey.CREATE);
      userExtIdsReq.put(JsonKey.EXTERNAL_IDS, user.getExternalIds());
      Request userequest = new Request();
      userequest.setOperation(UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue());
      userExtIdsReq.put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
      userequest.getRequest().putAll(userExtIdsReq);
      response =
          (Response)
              interServiceCommunication.getResponse(
                  getActorRef(UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue()),
                  userequest);
      ProjectLogger.log(
          "TenantMigrationActor:updateUserExternalIds user externalIds got updated.",
          LoggerEnum.INFO.name());
    } catch (Exception ex) {
      ProjectLogger.log(
          "TenantMigrationActor:updateUserExternalIds:Exception occurred while updating user externalIds.",
          ex);
      List<Map<String, Object>> errMsgList = new ArrayList<>();
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.ERROR_MSG, ex.getMessage());
      errMsgList.add(map);
      response.getResult().put(JsonKey.ERRORS, errMsgList);
    }
    return response;
  }

  private Response updateUserOrg(Request request, List<Map<String, Object>> userOrgList) {
    ProjectLogger.log("TenantMigrationActor:updateUserOrg called.", LoggerEnum.INFO.name());
    Response response = new Response();
    deleteOldUserOrgMapping(userOrgList);
    Map<String, Object> userDetails = request.getRequest();
    // add mapping root org
    createUserOrgRequestAndUpdate(
        (String) userDetails.get(JsonKey.USER_ID), (String) userDetails.get(JsonKey.ROOT_ORG_ID));
    String orgId = (String) userDetails.get(JsonKey.ORG_ID);
    if (StringUtils.isNotBlank(orgId)
        && !((String) userDetails.get(JsonKey.ROOT_ORG_ID)).equalsIgnoreCase(orgId)) {
      try {
        createUserOrgRequestAndUpdate((String) userDetails.get(JsonKey.USER_ID), orgId);
        ProjectLogger.log(
            "TenantMigrationActor:updateUserOrg user org data got updated.",
            LoggerEnum.INFO.name());
      } catch (Exception ex) {
        ProjectLogger.log(
            "TenantMigrationActor:updateUserOrg:Exception occurred while updating user Org.", ex);
        List<Map<String, Object>> errMsgList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.ERROR_MSG, ex.getMessage());
        errMsgList.add(map);
        response.getResult().put(JsonKey.ERRORS, errMsgList);
      }
    }
    return response;
  }

  private void createUserOrgRequestAndUpdate(String userId, String orgId) {
    Map<String, Object> userOrgRequest = new HashMap<>();
    userOrgRequest.put(JsonKey.ID, userId);
    String hashTagId = Util.getHashTagIdFromOrgId(orgId);
    userOrgRequest.put(JsonKey.HASHTAGID, hashTagId);
    userOrgRequest.put(JsonKey.ORGANISATION_ID, orgId);
    List<String> roles = new ArrayList<>();
    roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
    userOrgRequest.put(JsonKey.ROLES, roles);
    Util.registerUserToOrg(userOrgRequest);
  }

  private void deleteOldUserOrgMapping(List<Map<String, Object>> userOrgList) {
    ProjectLogger.log(
        "TenantMigrationActor:deleteOldUserOrgMapping: delete old user org association started.",
        LoggerEnum.INFO.name());
    for (Map<String, Object> userOrg : userOrgList) {
      cassandraOperation.deleteRecord(
          usrOrgDbInfo.getKeySpace(),
          usrOrgDbInfo.getTableName(),
          (String) userOrg.get(JsonKey.ID));
    }
  }

  private void validateChannelAndGetRootOrgId(Request request) {
    String rootOrgId = "";
    String channel = (String) request.getRequest().get(JsonKey.CHANNEL);
    if (StringUtils.isNotBlank(channel)) {
      rootOrgId = userService.getRootOrgIdFromChannel(channel);
      request.getRequest().put(JsonKey.ROOT_ORG_ID, rootOrgId);
    }
  }

  private Map<String, Object> createUserUpdateRequest(Request request) {
    Map<String, Object> userRequest = new HashMap<>();
    userRequest.put(JsonKey.ID, request.getRequest().get(JsonKey.USER_ID));
    userRequest.put(JsonKey.CHANNEL, request.getRequest().get(JsonKey.CHANNEL));
    userRequest.put(JsonKey.ROOT_ORG_ID, request.getRequest().get(JsonKey.ROOT_ORG_ID));
    return userRequest;
  }
}
