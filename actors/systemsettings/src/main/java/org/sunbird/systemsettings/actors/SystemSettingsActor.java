package org.sunbird.systemsettings.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.models.systemsetting.SystemSetting;
import org.sunbird.systemsettings.dao.impl.SystemSettingDaoImpl;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

@ActorConfig(
        tasks = {"getSystemSetting", "getAllSystemSettings", "setSystemSetting"},
        asyncTasks = {}
)
public class SystemSettingsActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
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

  private void getSystemSetting(Request actorMessage) {
    String value = DataCacheHandler.getConfigSettings().get(actorMessage.getContext().get(JsonKey.FIELD));
    ProjectLogger.log("SystemSettingsActor:getSystemSetting: got field value from cache.",LoggerEnum.INFO.name());
    SystemSetting setting = null;
    if (value != null) {
      setting =
              new SystemSetting(
                      (String) actorMessage.getContext().get(JsonKey.FIELD),
                      (String) actorMessage.getContext().get(JsonKey.FIELD),
                      value);
    } else {
      SystemSettingDaoImpl systemSettingDaoImpl =
                new SystemSettingDaoImpl(ServiceFactory.getInstance());
      setting =
              systemSettingDaoImpl.readByField((String) actorMessage.getContext().get(JsonKey.FIELD));
      ProjectLogger.log("SystemSettingsActor:getSystemSetting:got field value from db",LoggerEnum.INFO.name());
      if(null == setting){
          throw new ProjectCommonException(
                  ResponseCode.resourceNotFound.getErrorCode(),
                  ResponseCode.resourceNotFound.getErrorMessage(),
                  ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());

      }
      DataCacheHandler.getConfigSettings().put((String) actorMessage.getContext().get(JsonKey.FIELD), setting.getValue());
    }
    Response response = new Response();
    response.put(JsonKey.RESPONSE, setting);
    sender().tell(response, self());
  }

  private void getAllSystemSettings() {
    ProjectLogger.log("SystemSettingsActor: getAllSystemSettings called", LoggerEnum.DEBUG.name());
    SystemSettingDaoImpl systemSettingDaoImpl =
              new SystemSettingDaoImpl(ServiceFactory.getInstance());
    List<SystemSetting> allSystemSettings = systemSettingDaoImpl.readAll();
    Response response = new Response();
    response.put(JsonKey.RESPONSE, allSystemSettings);
    sender().tell(response, self());
  }

  private void setSystemSetting(Request actorMessage) {
    ProjectLogger.log("SystemSettingsActor: setSystemSetting called", LoggerEnum.DEBUG.name());

    Map<String, Object> request = actorMessage.getRequest();
    String id = (String) request.get(JsonKey.ID);
    String field = (String) request.get(JsonKey.FIELD);
    if (JsonKey.PHONE_UNIQUE.equalsIgnoreCase(field)
            || JsonKey.EMAIL_UNIQUE.equalsIgnoreCase(field)
            || JsonKey.PHONE_UNIQUE.equalsIgnoreCase(id)
            || JsonKey.EMAIL_UNIQUE.equalsIgnoreCase(id)) {
      ProjectCommonException.throwClientErrorException(
              ResponseCode.errorUpdateSettingNotAllowed,
              MessageFormat.format(ResponseCode.errorUpdateSettingNotAllowed.getErrorMessage(), field));
    }
    ObjectMapper mapper = new ObjectMapper();
    SystemSetting systemSetting = mapper.convertValue(request, SystemSetting.class);
    SystemSettingDaoImpl systemSettingDaoImpl =
              new SystemSettingDaoImpl(ServiceFactory.getInstance());
    Response response = systemSettingDaoImpl.write(systemSetting);
    sender().tell(response, self());
  }
}