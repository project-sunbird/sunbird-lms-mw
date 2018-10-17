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
import org.sunbird.learner.actors.role.RoleDao;
import org.sunbird.learner.actors.role.dao.impl.RoleDaoImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.models.role.Role;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.dao.UserDao;
import org.sunbird.user.dao.impl.UserDaoImpl;

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

  private UserDao userDao = UserDaoImpl.getInstance();
  private RoleDao roleDao = RoleDaoImpl.getInstance();
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
        ProjectLogger.log("UNSUPPORTED OPERATION");
        ProjectCommonException exception =
            new ProjectCommonException(
                ResponseCode.invalidOperationName.getErrorCode(),
                ResponseCode.invalidOperationName.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
    }
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
    List<Role> roleList = roleDao.getAllRecords();
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
   * This method will assign roles to users or user organizations.
   *
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void assignRoles(Request actorMessage) {
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
}
