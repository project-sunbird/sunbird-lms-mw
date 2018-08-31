package org.sunbird.systemsettings.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
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
  tasks = {"getSystemSetting", "getAllSystemSettings", "setSystemSetting"},
  asyncTasks = {}
)
public class SystemSettingsActor extends BaseActor {
  private final ObjectMapper mapper = new ObjectMapper();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private final SystemSettingService systemSettingService =
      new SystemSettingServiceImpl(cassandraOperation);

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, TelemetryEnvKey.SYSTEM_SETTINGS);
    ExecutionContext.setRequestId(request.getRequestId());

    switch (request.getOperation()) {
      case "getSystemSetting":
        getSystemSetting(request);
        break;
      case "getAllSystemSettings":
        getAllSystemSettings();
        break;
      case "setSystemSetting":
        setSystemSetting(request);
        break;
      default:
        onReceiveUnsupportedOperation(request.getOperation());
        break;
    }
  }
  /** @param actorMessage Instance of Request class contains the Setting id data */
  @SuppressWarnings("unchecked")
  private void getSystemSetting(Request actorMessage) {
    ProjectLogger.log("getSystemSettingById method call started", LoggerEnum.DEBUG.name());

    Map<String, Object> req = actorMessage.getRequest();
    SystemSetting setting = systemSettingService.readSetting((String) req.get(JsonKey.ID));
    if (setting == null) {
      throw new ProjectCommonException(
          ResponseCode.resourceNotFound.getErrorCode(),
          ResponseCode.resourceNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    } else {
      Response response = new Response();
      response.put(JsonKey.RESPONSE, setting);
      sender().tell(response, self());
    }
  }

  @SuppressWarnings("unchecked")
  private void getAllSystemSettings() {
    ProjectLogger.log("getAllSystemSettings method call started", LoggerEnum.DEBUG.name());
    Response response = systemSettingService.readAllSettings();
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private void setSystemSetting(Request actorMessage) {
    Map<String, Object> req = actorMessage.getRequest();
    SystemSetting setting = mapper.convertValue(req, SystemSetting.class);
    Response result = writeSystemSetting(setting);
    sender().tell(result, self());
  }

  private Response writeSystemSetting(SystemSetting systemSetting) {
    Response response = new Response();
    ProjectLogger.log("SystemInitActor: writeSystemSetting called", LoggerEnum.DEBUG.name());
    response = systemSettingService.setSetting(systemSetting);
    return response;
  }
}
