package org.sunbird.systemsettings.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.MessageFormat;
import java.util.*;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.systemsetting.SystemSetting;
import org.sunbird.systemsettings.dao.impl.SystemSettingDaoImpl;

@ActorConfig(
  tasks = {"getSystemSetting", "getAllSystemSettings", "setSystemSetting"},
  asyncTasks = {}
)
public class SystemSettingsActor extends BaseActor {
  private final ObjectMapper mapper = new ObjectMapper();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private final SystemSettingDaoImpl systemSettingDaoImpl =
      new SystemSettingDaoImpl(cassandraOperation);

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

  @SuppressWarnings("unchecked")
  private void getSystemSetting(Request actorMessage) {
    ProjectLogger.log("SystemSettingsActor: getSystemSetting called", LoggerEnum.INFO.name());
    ProjectLogger.log(
        "SystemSettingsActor:getSystemSetting: request is " + actorMessage.getRequest(),
        LoggerEnum.INFO.name());
    SystemSetting setting =
        systemSettingDaoImpl.readByField((String) actorMessage.getContext().get(JsonKey.FIELD));

    if (setting == null) {
      throw new ProjectCommonException(
          ResponseCode.resourceNotFound.getErrorCode(),
          ResponseCode.resourceNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }

    Response response = new Response();
    response.put(JsonKey.RESPONSE, setting);
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private void getAllSystemSettings() {
    ProjectLogger.log("SystemSettingsActor: getAllSystemSettings called", LoggerEnum.DEBUG.name());
    List<SystemSetting> allSystemSettings = systemSettingDaoImpl.readAll();
    Response response = new Response();
    response.put(JsonKey.RESPONSE, allSystemSettings);
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private void setSystemSetting(Request actorMessage) {
    ProjectLogger.log("SystemSettingsActor: setSystemSetting called", LoggerEnum.DEBUG.name());

    Map<String, Object> req = actorMessage.getRequest();
    if (((String) req.get(JsonKey.FIELD)).equalsIgnoreCase(JsonKey.PHONE_UNIQUE)) {
      checkEmailOrPhoneData(
          (String) req.get(JsonKey.FIELD),
          (String) req.get(JsonKey.VALUE),
          JsonKey.ENC_PHONE,
          ResponseCode.duplicatePhoneData,
          JsonKey.PHONE);
    }
    if (((String) req.get(JsonKey.FIELD)).equalsIgnoreCase(JsonKey.EMAIL_UNIQUE)) {
      checkEmailOrPhoneData(
          (String) req.get(JsonKey.FIELD),
          (String) req.get(JsonKey.VALUE),
          JsonKey.ENC_EMAIL,
          ResponseCode.duplicateEmailData,
          JsonKey.EMAIL);
    }
    SystemSetting systemSetting = mapper.convertValue(req, SystemSetting.class);
    Response response = systemSettingDaoImpl.write(systemSetting);
    sender().tell(response, self());
  }

  private void checkEmailOrPhoneData(
      String field, String value, String facetsKey, ResponseCode responseCode, String objectType) {
    SystemSetting systemSetting = systemSettingDaoImpl.readByField(field);

    if (systemSetting != null) {
      boolean dbUniqueValue = Boolean.parseBoolean(systemSetting.getValue());
      boolean reqUniqueValue = Boolean.parseBoolean(value);

      SearchDTO searchDto = null;
      if ((!dbUniqueValue) && reqUniqueValue) {
        searchDto = new SearchDTO();
        searchDto.setLimit(0);
        Map<String, String> facets = new HashMap<>();
        facets.put(facetsKey, null);
        List<Map<String, String>> list = new ArrayList<>();
        list.add(facets);
        searchDto.setFacets(list);
        Map<String, Object> esResponse =
            ElasticSearchUtil.complexSearch(
                searchDto,
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName());
        if (null != esResponse) {
          List<Map<String, Object>> facetsRes =
              (List<Map<String, Object>>) esResponse.get(JsonKey.FACETS);
          if (null != facetsRes && !facetsRes.isEmpty()) {
            Map<String, Object> map = facetsRes.get(0);
            List<Map<String, Object>> values = (List<Map<String, Object>>) map.get("values");
            for (Map<String, Object> result : values) {
              long count = (long) result.get(JsonKey.COUNT);
              if (count > 1) {
                throw new ProjectCommonException(
                    responseCode.getErrorCode(),
                    MessageFormat.format(responseCode.getErrorMessage(), objectType),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
              }
            }
          }
        }
      }
    }
  }
}
