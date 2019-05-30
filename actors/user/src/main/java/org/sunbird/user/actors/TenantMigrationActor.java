package org.sunbird.user.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
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

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
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
    Map<String, Object> userDetails = getUser((String) request.getRequest().get(JsonKey.USER_ID));
    validateChannelAndGetRootOrgId(request);
    Map<String, Object> userUpdateRequest = createUserUpdateRequest(request);
    // Update user channel and rootOrgId
    Response response =
        cassandraOperation.updateRecord(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userUpdateRequest);
    if (!((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      // throw exception for migration failed
      ProjectCommonException.throwServerErrorException(ResponseCode.errorUserMigrationFailed);
    }
    // Update user org details
    Response userOrgResponse =
        updateUserOrg(request, (List<Map<String, Object>>) userDetails.get(JsonKey.ORGANISATIONS));
    // Update user externalIds
    Response userExternalIdsResponse = updateUserExternalIds(request);
    // Collect all the error message
    List<Map<String, Object>> userOrgErrMsgList =
        (List<Map<String, Object>>) userOrgResponse.getResult().get(JsonKey.ERRORS);
    List<Map<String, Object>> userExtIdErrMsgList =
        (List<Map<String, Object>>) userExternalIdsResponse.getResult().get(JsonKey.ERRORS);
    userOrgErrMsgList.addAll(userExtIdErrMsgList);
    response.getResult().put(JsonKey.ERRORS, userOrgErrMsgList);
    // send the response
    sender().tell(response, self());
    // save user data to ES
    saveUserDetailsToEs((String) request.getRequest().get(JsonKey.USER_ID));
  }

  private void saveUserDetailsToEs(String userId) {
    Request userRequest = new Request();
    userRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
    userRequest.getRequest().put(JsonKey.ID, userId);
    ProjectLogger.log(
        "TenantMigrationActor:saveUserDetailsToEs: Trigger sync of user details to ES");
    tellToAnother(userRequest);
  }

  private Response updateUserExternalIds(Request request) {
    Response response = new Response();
    Map<String, Object> userExtIdsReq = createExternalIdsRequest(request);
    try {
      User user = mapper.convertValue(userExtIdsReq, User.class);
      UserUtil.validateExternalIds(user, JsonKey.CREATE);
      Request userequest = new Request();
      userequest.setOperation(UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS.getValue());
      userExtIdsReq.put(JsonKey.OPERATION_TYPE, JsonKey.CREATE);
      userequest.getRequest().putAll(userExtIdsReq);
      response =
          (Response)
              interServiceCommunication.getResponse(
                  getActorRef(ActorOperations.GET_USER_PROFILE.getValue()), request);
    } catch (ProjectCommonException ex) {
      ProjectLogger.log(
          "TenantMigrationActor:updateUserExternalIds:Exception occurred while updating user externalIds,",
          ex);
      List<Map<String, Object>> errMsgList = new ArrayList<>();
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.ERROR_MSG, ex.getMessage());
      errMsgList.add(map);
      response.getResult().put(JsonKey.ERRORS, errMsgList);
    }
    return response;
  }

  private Map<String, Object> createExternalIdsRequest(Request request) {
    Map<String, Object> req = request.getRequest();
    List<Map<String, Object>> externalIdsList = new ArrayList<>();
    Map<String, Object> externalIDMap = Collections.emptyMap();
    if (StringUtils.isNotBlank((String) req.get(JsonKey.USER_EXTERNAL_ID))) {
      externalIDMap = new HashMap<>();
      externalIDMap.put(JsonKey.EXTERNAL_ID, req.get(JsonKey.USER_EXTERNAL_ID));
      if (StringUtils.isNotBlank((String) req.get(JsonKey.ID_TYPE))
          && StringUtils.isNotBlank((String) req.get(JsonKey.PROVIDER))) {
        externalIDMap.put(JsonKey.ID_TYPE, req.get(JsonKey.ID_TYPE));
        externalIDMap.put(JsonKey.PROVIDER, req.get(JsonKey.PROVIDER));
      } else {
        externalIDMap.put(JsonKey.ID_TYPE, req.get(JsonKey.CHANNEL));
        externalIDMap.put(JsonKey.PROVIDER, req.get(JsonKey.CHANNEL));
      }
      externalIdsList.add(externalIDMap);
      Map<String, Object> userMap = new HashMap<>();
      userMap.put(JsonKey.ID, req.get(JsonKey.USER_ID));
      userMap.put(JsonKey.USER_ID, req.get(JsonKey.USER_ID));
      userMap.put(JsonKey.EXTERNAL_IDS, externalIdsList);
      return userMap;
    }
    return Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getUser(String userId) {
    Request request = new Request();
    request.setOperation(ActorOperations.GET_USER_PROFILE.getValue());
    request.getRequest().put(JsonKey.USER_ID, userId);
    Response response =
        (Response)
            interServiceCommunication.getResponse(
                getActorRef(ActorOperations.GET_USER_PROFILE.getValue()), request);
    if (null != response
        && MapUtils.isNotEmpty(response.getResult())
        && MapUtils.isNotEmpty((Map<String, Object>) response.getResult().get(JsonKey.RESPONSE))) {
      return (Map<String, Object>) response.getResult().get(JsonKey.RESPONSE);
    } else {
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidUserId);
    }
    return null;
  }

  private Response updateUserOrg(Request request, List<Map<String, Object>> userOrgList) {
    Response response = new Response();
    deleteOldUserOrgMapping(userOrgList);
    Map<String, Object> userDetails = request.getRequest();
    createUserOrgRequestAndUpdate(
        (String) userDetails.get(JsonKey.USER_ID), (String) userDetails.get(JsonKey.ROOT_ORG_ID));
    if (StringUtils.isNotBlank((String) userDetails.get(JsonKey.ORG_EXTERNAL_ID))) {
      String orgId =
          orgExternalService.getOrgIdFromOrgExternalIdAndProvider(
              (String) userDetails.get(JsonKey.ORG_EXTERNAL_ID),
              (String) userDetails.get(JsonKey.CHANNEL));
      try {
        createUserOrgRequestAndUpdate((String) userDetails.get(JsonKey.USER_ID), orgId);
      } catch (ProjectCommonException ex) {
        ProjectLogger.log(
            "TenantMigrationActor:updateUserOrg:Exception occurred while updating user externalIds.",
            ex);
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
