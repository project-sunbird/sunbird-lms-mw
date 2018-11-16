package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.actorutil.user.UserClient;
import org.sunbird.actorutil.user.impl.UserClientImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTask;
import org.sunbird.learner.actors.role.service.RoleService;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;
import org.sunbird.validator.user.UserBulkUploadRequestValidator;

@ActorConfig(
  tasks = {},
  asyncTasks = {"userBulkUploadBackground"}
)
public class UserBulkUploadBackgroundJobActor extends BaseBulkUploadBackgroundJobActor {
  private UserClient userClient = new UserClientImpl();
  private SystemSettingClient systemSettingClient = new SystemSettingClientImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    Util.initializeContext(request, TelemetryEnvKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    if (operation.equalsIgnoreCase("userBulkUploadBackground")) {

      Map outputColumns =
          systemSettingClient.getSystemSettingByFieldAndKey(
              getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
              "userProfileConfig",
              "csv.outputColumns",
              new TypeReference<Map>() {});

      String[] outputColumnsOrder =
          systemSettingClient.getSystemSettingByFieldAndKey(
              getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
              "userProfileConfig",
              "csv.outputColumnsOrder",
              new TypeReference<String[]>() {});

      handleBulkUploadBackground(
          request,
          (baseBulkUpload) -> {
            processBulkUpload(
                (BulkUploadProcess) baseBulkUpload,
                (tasks) -> {
                  processTasks(
                      (List<BulkUploadProcessTask>) tasks, ((BulkUploadProcess) baseBulkUpload));
                  return null;
                },
                outputColumns,
                outputColumnsOrder != null
                    ? outputColumnsOrder
                    : (String[]) request.get(JsonKey.FIELDS));
            return null;
          });
    } else {
      onReceiveUnsupportedOperation("UserBulkUploadBackgroundJobActor");
    }
  }

  private void processTasks(
      List<BulkUploadProcessTask> bulkUploadProcessTasks, BulkUploadProcess bulkUploadProcess) {
    for (BulkUploadProcessTask task : bulkUploadProcessTasks) {
      try {
        if (task.getStatus() != null
            && task.getStatus() != ProjectUtil.BulkProcessStatus.COMPLETED.getValue()) {
          processUser(
              task, bulkUploadProcess.getOrganisationId(), bulkUploadProcess.getUploadedBy());
          task.setLastUpdatedOn(new Timestamp(System.currentTimeMillis()));
          task.setIterationId(task.getIterationId() + 1);
        }
      } catch (Exception ex) {
        task.setFailureResult(ex.getMessage());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void processUser(BulkUploadProcessTask task, String organisationId, String uploadedBy) {
    ProjectLogger.log("UserBulkUploadBackgroundJobActor: processUser called", LoggerEnum.INFO);
    String data = task.getData();
    Map<String, Object> orgMap = null;
    try {
      Map<String, Object> userMap = mapper.readValue(data, Map.class);
      String[] mandatoryColumnsObject =
          systemSettingClient.getSystemSettingByFieldAndKey(
              getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
              "userProfileConfig",
              "csv.mandatoryColumns",
              new TypeReference<String[]>() {});
      if (mandatoryColumnsObject != null) {
        validateMandatoryFields(userMap, task, mandatoryColumnsObject);
      }
      if (userMap.get(JsonKey.PHONE) != null) {
        userMap.put(JsonKey.PHONE_VERIFIED, true);
      }
      try {
        String roles = (String) userMap.get(JsonKey.ROLES);
        if (roles != null) {
          userMap.put(JsonKey.ROLES, Arrays.asList(roles.split("\\\\s*,\\\\s*")));
          RoleService.validateRoles((List<String>) userMap.get(JsonKey.ROLES));
        }
        UserBulkUploadRequestValidator.validateUserBulkUploadRequest(userMap);
      } catch (Exception ex) {
        setTaskStatus(
            task, ProjectUtil.BulkProcessStatus.FAILED, ex.getMessage(), userMap, JsonKey.CREATE);
        return;
      }
      if (StringUtils.isNotBlank((String) userMap.get(JsonKey.ORG_ID))
          || StringUtils.isNotBlank((String) userMap.get(JsonKey.ORG_EXTERNAL_ID))) {
        orgMap = getOrgDetails(userMap);
        if (MapUtils.isEmpty(orgMap)) {
          setTaskStatus(
              task,
              ProjectUtil.BulkProcessStatus.FAILED,
              ResponseCode.invalidOrgId.getErrorMessage(),
              userMap,
              JsonKey.CREATE);
          return;
        } else {
          if (StringUtils.isNotBlank((String) userMap.get(JsonKey.ORG_ID))
              && StringUtils.isNotBlank((String) userMap.get(JsonKey.ORG_EXTERNAL_ID))) {
            if (!((String) userMap.get(JsonKey.ORG_ID))
                .equalsIgnoreCase((String) orgMap.get(JsonKey.ID))) {
              String message =
                  MessageFormat.format(
                      ResponseCode.errorConflictingProperties.getErrorMessage(),
                      JsonKey.ORGANISATION_ID,
                      userMap.get(JsonKey.ORG_ID),
                      JsonKey.EXTERNAL_ID,
                      userMap.get(JsonKey.EXTERNAL_ID));
              setTaskStatus(
                  task, ProjectUtil.BulkProcessStatus.FAILED, message, userMap, JsonKey.CREATE);
              return;
            }
          } else {
            if (StringUtils.isNotBlank((String) userMap.get(JsonKey.ORG_EXTERNAL_ID))) {
              userMap.put(JsonKey.ORGANISATION_ID, orgMap.get(JsonKey.ID));
            } else {
              userMap.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.ORG_ID));
            }
          }
        }
      }
      if (!((String) orgMap.get(JsonKey.ROOT_ORG_ID)).equalsIgnoreCase(organisationId)) {
        setTaskStatus(
            task,
            ProjectUtil.BulkProcessStatus.FAILED,
            ResponseCode.invalidRootOrganisationId.getErrorMessage(),
            userMap,
            JsonKey.CREATE);
        return;
      }
      User user = mapper.convertValue(userMap, User.class);
      user.setId((String) userMap.get(JsonKey.USER_ID));
      user.setRootOrgId(organisationId);
      if (StringUtils.isEmpty(user.getId())) {
        user.setCreatedBy(uploadedBy);
        callCreateUser(user, task, (String) orgMap.get(JsonKey.ORG_NAME));
      } else {
        callUpdateUser(user, task, (String) orgMap.get(JsonKey.ORG_NAME));
      }
    } catch (Exception e) {
      task.setStatus(ProjectUtil.BulkProcessStatus.FAILED.getValue());
    }
  }

  @SuppressWarnings("unchecked")
  private void callCreateUser(User user, BulkUploadProcessTask task, String orgName)
      throws JsonProcessingException {
    ProjectLogger.log("UserBulkUploadBackgroundJobActor: callCreateUser called", LoggerEnum.INFO);
    Map<String, Object> row = mapper.convertValue(user, Map.class);
    String userId;
    try {
      userId = userClient.createUser(getActorRef(ActorOperations.CREATE_USER.getValue()), row);
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserBulkUploadBackgroundJobActor:callCreateUser: Exception occurred with error message = "
              + ex.getMessage(),
          LoggerEnum.INFO);
      setTaskStatus(
          task, ProjectUtil.BulkProcessStatus.FAILED, ex.getMessage(), row, JsonKey.CREATE);
      return;
    }

    if (StringUtils.isEmpty(userId)) {
      ProjectLogger.log(
          "UserBulkUploadBackgroundJobActor:callCreateUser: User ID is null !", LoggerEnum.ERROR);
      setTaskStatus(
          task,
          ProjectUtil.BulkProcessStatus.FAILED,
          ResponseCode.internalError.getErrorMessage(),
          row,
          JsonKey.CREATE);
    } else {
      row.put(JsonKey.ID, userId);
      row.put(JsonKey.ORG_NAME, orgName);
      setSuccessTaskStatus(task, ProjectUtil.BulkProcessStatus.COMPLETED, row, JsonKey.CREATE);
    }
  }

  @SuppressWarnings("unchecked")
  private void callUpdateUser(User user, BulkUploadProcessTask task, String orgName)
      throws JsonProcessingException {
    ProjectLogger.log("UserBulkUploadBackgroundJobActor: callUpdateUser called", LoggerEnum.INFO);
    Map<String, Object> row = mapper.convertValue(user, Map.class);
    try {
      row.put(JsonKey.USER_ID, user.getId());
      row.put(JsonKey.ORG_NAME, orgName);
      userClient.updateUser(getActorRef(ActorOperations.UPDATE_USER.getValue()), row);
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserBulkUploadBackgroundJobActor:callUpdateUser: Exception occurred with error message = "
              + ex.getMessage(),
          LoggerEnum.INFO);
      row.put(JsonKey.ERROR_MSG, ex.getMessage());
      setTaskStatus(
          task, ProjectUtil.BulkProcessStatus.FAILED, ex.getMessage(), row, JsonKey.UPDATE);
    }
    if (task.getStatus() != ProjectUtil.BulkProcessStatus.FAILED.getValue()) {
      task.setData(mapper.writeValueAsString(row));
      setSuccessTaskStatus(task, ProjectUtil.BulkProcessStatus.COMPLETED, row, JsonKey.UPDATE);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getOrgDetails(Map<String, Object> userMap) {
    Map<String, Object> organization = null;
    if (StringUtils.isNotBlank((String) userMap.get(JsonKey.EXTERNAL_ID))) {
      Map<String, Object> filters = new HashMap<>();
      filters.put(JsonKey.EXTERNAL_ID, userMap.get(JsonKey.EXTERNAL_ID));
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.FILTERS, filters);
      SearchDTO searchDto = Util.createSearchDto(map);
      Map<String, Object> result =
          ElasticSearchUtil.complexSearch(
              searchDto,
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName());
      List<Map<String, Object>> orgMapList =
          (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
      if (CollectionUtils.isNotEmpty(orgMapList)) {
        organization = orgMapList.get(0);
      }
    } else if (StringUtils.isNotBlank((String) userMap.get(JsonKey.ORG_ID))) {
      organization =
          ElasticSearchUtil.getDataByIdentifier(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName(),
              (String) userMap.get(JsonKey.ORG_ID));
    }
    return organization;
  }

  @Override
  public void preProcessResult(Map<String, Object> result) {
    UserUtility.decryptUserData(result);
    Util.addMaskEmailAndPhone(result);
  }
}
