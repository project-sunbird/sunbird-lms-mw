package org.sunbird.systemsettings.actors;

import static org.sunbird.learner.util.Util.isNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
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
import org.sunbird.init.model.SystemSetting;
import org.sunbird.init.service.SystemSettingService;
import org.sunbird.init.service.impl.SystemSettingServiceImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.common.models.util.TelemetryEnvKey;
/**
 * This actor class contains actor methods for System Settings
 *
 * @author Loganathan Shanmugam
 */
@ActorConfig(
  tasks = {"getSystemSettingById","getAllSystemSettings"},
  asyncTasks = {}
)
public class SystemSettingsActor extends BaseActor {
  private ObjectMapper mapper = new ObjectMapper();
  private SystemSetting systemSetting;
  private SystemSettingService systemSettingService = new SystemSettingServiceImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, TelemetryEnvKey.SYSTEM_SETTINGS);
    ExecutionContext.setRequestId(request.getRequestId());
    if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_SYSTEM_SETTING_BY_ID.getValue())) {
      getSystemSettingById(request);
    }
    else if(request.getOperation().equalsIgnoreCase(ActorOperations.GET_ALL_SYSTEM_SETTINGS.getValue())) {
      getAllSystemSettings();
    }
    else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }
  /**
   * This method reads the System Setting using its id
   *
   * @param actorMessage Instance of Request class contains the Setting id data
   */
  @SuppressWarnings("unchecked")
  private void getSystemSettingById(Request actorMessage) {
    ProjectLogger.log("getSystemSettingById method call started",LoggerEnum.DEBUG.name());
    try {
      Map<String, Object> req =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.SYSTEM_SETTINGS);
      SystemSetting initSetting = systemSettingService.readSetting((String)req.get(JsonKey.ID));
      Response response = new Response();
    response.put(JsonKey.RESPONSE, initSetting);
    sender().tell(response, self());
    } catch (Exception e) {
      ProjectCommonException.throwServerErrorException(
          ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
    }
  }

  /**
   * This Method reads all System Settings from the System Settings table
   *
   */
  @SuppressWarnings("unchecked")
  private void getAllSystemSettings() {
    ProjectLogger.log("getAllSystemSettings method call started",LoggerEnum.DEBUG.name());
    try {
    Response response = systemSettingService.readAllSettings();
    sender().tell(response, self());
    } catch (Exception e) {
      ProjectLogger.log("Exception at getAllSystemSettings method " + e.getMessage(),LoggerEnum.DEBUG.name());
      ProjectCommonException.throwServerErrorException(
          ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
    }
  }
}
