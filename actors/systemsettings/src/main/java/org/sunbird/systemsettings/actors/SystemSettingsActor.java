package org.sunbird.systemsettings.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.systemsettings.model.SystemSetting;
import org.sunbird.systemsettings.service.SystemSettingService;
import org.sunbird.systemsettings.service.impl.SystemSettingServiceImpl;

@ActorConfig(
  tasks = {"getSystemSettingById", "getAllSystemSettings", "updateSystemSettingById"},
  asyncTasks = {}
)
public class SystemSettingsActor extends BaseActor {
  private final ObjectMapper mapper = new ObjectMapper();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private SystemSetting systemSetting;
  private final SystemSettingService systemSettingService =
      new SystemSettingServiceImpl(cassandraOperation);
  private final Integer WRITE_SETTINGS_RETRIES_ALLOWED = 1;

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, TelemetryEnvKey.SYSTEM_SETTINGS);
    ExecutionContext.setRequestId(request.getRequestId());
    if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_SYSTEM_SETTING.getValue())) {
      getSystemSettingById(request);
    } else if (request
        .getOperation()
        .equalsIgnoreCase(ActorOperations.GET_ALL_SYSTEM_SETTINGS.getValue())) {
      getAllSystemSettings();
    } else if (request
        .getOperation()
        .equalsIgnoreCase(ActorOperations.SET_SYSTEM_SETTING.getValue())) {
      updateSystemSettingById(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }
  /** @param actorMessage Instance of Request class contains the Setting id data */
  @SuppressWarnings("unchecked")
  private void getSystemSettingById(Request actorMessage) {
    ProjectLogger.log("getSystemSettingById method call started", LoggerEnum.DEBUG.name());
    try {
      Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest();
      SystemSetting initSetting = systemSettingService.readSetting((String) req.get(JsonKey.ID));
      Response response = new Response();
      response.put(JsonKey.RESPONSE, initSetting);
      sender().tell(response, self());
    } catch (Exception e) {
      System.out.println("exception is --> " + e.getMessage());
      ProjectCommonException.throwServerErrorException(
          ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private void getAllSystemSettings() {
    ProjectLogger.log("getAllSystemSettings method call started", LoggerEnum.DEBUG.name());
    try {
      Response response = systemSettingService.readAllSettings();
      sender().tell(response, self());
    } catch (Exception e) {
      ProjectLogger.log(
          "Exception at getAllSystemSettings method " + e.getMessage(), LoggerEnum.DEBUG.name());
      ProjectCommonException.throwServerErrorException(
          ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private void updateSystemSettingById(Request actorMessage) {
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest();
    SystemSetting setting = mapper.convertValue(req, SystemSetting.class);
    Response result = writeSystemSetting(setting);
    sender().tell(result, self());
  }

  private Response writeSystemSetting(SystemSetting systemSetting) {
    Response response = new Response();
    ProjectLogger.log("SystemInitActor: writeSystemSetting called", LoggerEnum.DEBUG.name());
    for (int i = 0; i <= WRITE_SETTINGS_RETRIES_ALLOWED; i++) {
      try {
        response = systemSettingService.setSetting(systemSetting);
        ProjectLogger.log(
            "Insert operation result for "
                + systemSetting.getField()
                + response.getResult().get(JsonKey.RESPONSE),
            LoggerEnum.DEBUG.name());
        break;
      } catch (Exception e) {
        ProjectLogger.log(
            "Exception while updating " + systemSetting.getField() + e.getMessage(),
            LoggerEnum.DEBUG.name());
        if (i == WRITE_SETTINGS_RETRIES_ALLOWED) {
          ProjectLogger.log(
              "Max retries reached while updating default RootOrgId. ", LoggerEnum.DEBUG.name());
          ProjectCommonException.throwServerErrorException(
              ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
        }
      }
    }
    return response;
  }
}
