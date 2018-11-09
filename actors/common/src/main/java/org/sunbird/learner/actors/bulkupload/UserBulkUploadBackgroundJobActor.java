package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.actorutil.user.UserClient;
import org.sunbird.actorutil.user.impl.UserClientImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcess;
import org.sunbird.learner.actors.bulkupload.model.BulkUploadProcessTask;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;

@ActorConfig(
    tasks = {},
    asyncTasks = {"userBulkUploadBackground"})
public class UserBulkUploadBackgroundJobActor extends BaseBulkUploadBackgroundJobActor {
  private UserClient userClient = new UserClientImpl();
  private SystemSettingClient systemSettingClient = new SystemSettingClientImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    Util.initializeContext(request, TelemetryEnvKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    if (operation.equalsIgnoreCase("userBulkUploadBackground")) {
      Map supportedColumns =
          systemSettingClient.getSystemSettingByFieldAndKey(
              getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
              "userProfileConfig",
              "csv.supportedColumns",
              new TypeReference<Map>() {});
      String[] supportedColumnsOrder =
          systemSettingClient.getSystemSettingByFieldAndKey(
              getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()),
              "userProfileConfig",
              "csv.supportedColumnsOrder",
              new TypeReference<String[]>() {});

      handleBulkUploadBackground(
          request,
          (baseBulkUpload) -> {
            processBulkUpload(
                (BulkUploadProcess) baseBulkUpload,
                (tasks) -> {
                  processTasks((List<BulkUploadProcessTask>) tasks);
                  return null;
                },
                supportedColumns,
                supportedColumnsOrder != null
                    ? supportedColumnsOrder
                    : (String[]) request.get(JsonKey.FIELDS));
            return null;
          });
    } else {
      onReceiveUnsupportedOperation("UserBulkUploadBackgroundJobActor");
    }
  }

  private void processTasks(List<BulkUploadProcessTask> bulkUploadProcessTasks) {
    for (BulkUploadProcessTask task : bulkUploadProcessTasks) {
      try {
        if (task.getStatus() != null
            && task.getStatus() != ProjectUtil.BulkProcessStatus.COMPLETED.getValue()) {
          processUser(task);
          task.setLastUpdatedOn(new Timestamp(System.currentTimeMillis()));
          task.setIterationId(task.getIterationId() + 1);
        }
      } catch (Exception ex) {
        task.setFailureResult(ex.getMessage());
      }
    }
  }

  private void processUser(BulkUploadProcessTask task) {
    ProjectLogger.log("UserBulkUploadBackgroundJobActor: processUser called", LoggerEnum.INFO);
    String data = task.getData();
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
      if (null != userMap.get(JsonKey.ROLES)) {
    	  String roles = (String)userMap.get(JsonKey.ROLES);
    	  userMap.put(JsonKey.ROLES, Arrays.asList(roles.split("\\\\s*,\\\\s*")));

        
      }
      User user = mapper.convertValue(userMap, User.class);
      user.setId((String) userMap.get(JsonKey.USER_ID));
      if (StringUtils.isEmpty(user.getId())) {
        callCreateUser(user, task);
      } else {
        callUpdateUser(user, task);
      }
    } catch (Exception e) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
    }
  }

  private void callCreateUser(User user, BulkUploadProcessTask task)
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
      setSuccessTaskStatus(task, ProjectUtil.BulkProcessStatus.COMPLETED, row, JsonKey.CREATE);
    }
  }

  private void callUpdateUser(User user, BulkUploadProcessTask task)
      throws JsonProcessingException {
    ProjectLogger.log("UserBulkUploadBackgroundJobActor: callUpdateUser called", LoggerEnum.INFO);
    Map<String, Object> row = mapper.convertValue(user, Map.class);
    try {
      row.put(JsonKey.ORGANISATION_ID, user.getId());
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

    task.setData(mapper.writeValueAsString(row));
    setSuccessTaskStatus(task, ProjectUtil.BulkProcessStatus.COMPLETED, row, JsonKey.UPDATE);
  }
}
