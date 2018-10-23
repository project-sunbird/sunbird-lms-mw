package org.sunbird.user.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.role.service.RoleService;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

/**
 * This actor will handle course enrollment operation .
 *
 * @author Manzarul
 * @author Amit Kumar
 * @author sudhirgiri
 */
@ActorConfig(
  tasks = {"getRoles", "assignRoles"},
  asyncTasks = {}
)
public class UserRoleActor extends BaseActor {

  private UserService userService = UserServiceImpl.getInstance();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  /** Receives the actor message and perform the user status operation . */
  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    String operation = request.getOperation();

    switch (operation) {
      case "getRoles":
        getRoles();
        break;

      case "assignRoles":
        assignRoles(request);
        break;

      default:
        onReceiveUnsupportedOperation("UserRoleActor");
    }
  }

  /** This method will provide the complete role structure.. */
  private void getRoles() {
    ProjectLogger.log("getRoles  called");
    Response response = RoleService.getUserRoles();
    sender().tell(response, self());
  }

  /**
   * This method will assign roles to users or user organizations.
   *
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void assignRoles(Request actorMessage) {
    ProjectLogger.log("assignRoles called");
    Map<String, Object> requestMap = actorMessage.getRequest();
    RoleService.validateRoles((List<String>) requestMap.get(JsonKey.ROLES));
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
    Map<String, Object> userOrgDBMap = userService.getUserByUserIdAndOrgId(userId, organisationId);
    Util.DbInfo userOrgDb = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    if (MapUtils.isEmpty(userOrgDBMap)) {
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
    if (StringUtils.isNotBlank(hashTagId)) {
      userOrgDBMap.put(JsonKey.HASHTAGID, hashTagId);
    }
    userOrgDBMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    userOrgDBMap.put(JsonKey.UPDATED_BY, requestMap.get(JsonKey.REQUESTED_BY));
    userOrgDBMap.put(JsonKey.ROLES, roles);
    Response response =
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
      }
    }
    TelemetryUtil.telemetryProcessingCall(telemetryAction, targetObject, correlatedObject);
  }
}
