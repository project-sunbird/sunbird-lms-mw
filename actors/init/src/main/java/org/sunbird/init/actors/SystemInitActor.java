package org.sunbird.init.actors;

import static org.sunbird.learner.util.Util.isNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.Slug;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.init.model.SystemSetting;
import org.sunbird.init.service.SystemSettingService;
import org.sunbird.init.service.impl.SystemSettingServiceImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.models.organization.Organization;

/**
 * This actor class contains actor methods for System initialisation
 *
 * @author Loganathan
 */
@ActorConfig(
  tasks = {"systemInitRootOrg"},
  asyncTasks = {}
)
public class SystemInitActor extends BaseActor {
  private final CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private SystemSetting systemSetting;
  private SystemSettingService systemSettingService = new SystemSettingServiceImpl();

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, TelemetryEnvKey.ORGANISATION);
    ExecutionContext.setRequestId(request.getRequestId());
    if (request.getOperation().equalsIgnoreCase(ActorOperations.SYSTEM_INIT_ROOT_ORG.getValue())) {
      systemInitRootOrg(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }
  /**
   * This Method to creates the first root organization after validating the data
   *
   * @param actorMessage Instance of Request class contains the organisation data to be created
   */
  @SuppressWarnings("unchecked")
  private void systemInitRootOrg(Request actorMessage) {
    ProjectLogger.log("systemInitRootOrg method call started");
    throwExceptionIfRootOrgAlreadyInitialised();
    Map<String, Object> req =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    validateChannel(req);
    req.put(JsonKey.CREATED_BY, JsonKey.INITIALISER);
    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    req.put(JsonKey.ID, uniqueId);
    req.put(JsonKey.ROOT_ORG_ID, uniqueId);
    req.put(JsonKey.HASHTAGID, uniqueId);
    req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    req.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
    req.put(JsonKey.IS_ROOT_ORG, true);
    req.put(JsonKey.IS_DEFAULT, true);

    String slug = Slug.makeSlug((String) req.get(JsonKey.CHANNEL), true);
    boolean bool = isSlugUnique(slug);
    if (bool) {
      req.put(JsonKey.SLUG, slug);
    } else {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.slugIsNotUnique, ResponseCode.slugIsNotUnique.getErrorMessage());
    }

    boolean isChannelRegistered = Util.registerChannel(req);
    if (!isChannelRegistered) {
      ProjectCommonException.throwServerErrorException(
          ResponseCode.channelRegFailed, ResponseCode.channelRegFailed.getErrorMessage());
    }

    Organization org = mapper.convertValue(req, Organization.class);
    req = mapper.convertValue(org, Map.class);
    Response result =
        cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), req);
    addInitialisationFlagToSystemSettings();
    ProjectLogger.log("Org data saved into cassandra.");
    ProjectLogger.log("Created org id is ----." + uniqueId);
    result.getResult().put(JsonKey.ORGANISATION_ID, uniqueId);
    sender().tell(result, self());
    Request orgReq = new Request();
    orgReq.getRequest().put(JsonKey.ORGANISATION, req);
    orgReq.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
    ProjectLogger.log("Calling background job to save org data into ES" + uniqueId);
    tellToAnother(orgReq);
  }

  /**
   * This method checks if the given channel slug value is unique
   *
   * @param slug input slug value need to checked.
   * @return returns true if slug unique or return false
   */
  private boolean isSlugUnique(String slug) {
    if (StringUtils.isNotBlank(slug)) {
      Map<String, Object> filters = new HashMap<>();
      filters.put(JsonKey.SLUG, slug);
      filters.put(JsonKey.IS_ROOT_ORG, true);
      Map<String, Object> esResult =
          elasticSearchComplexSearch(
              filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
      if (isNotNull(esResult)
          && esResult.containsKey(JsonKey.CONTENT)
          && isNotNull(esResult.get(JsonKey.CONTENT))) {
        return (((List) esResult.get(JsonKey.CONTENT)).isEmpty());
      }
    }
    return false;
  }

  /** This method writes the rootOrgInitialised status(true) to system settings */
  private void addInitialisationFlagToSystemSettings() {
    try {
      this.systemSetting =
          new SystemSetting(
              JsonKey.IS_ROOT_ORG_INITIALISED,
              JsonKey.IS_ROOT_ORG_INITIALISED,
              String.valueOf(true));
      Response response = systemSettingService.writeSetting(systemSetting);
      ProjectLogger.log(
          "Insert operation result for initialised status =  "
              + response.getResult().get(JsonKey.RESPONSE),
          LoggerEnum.DEBUG.name());
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  /**
   * If system is already initalised i.e if isRootOrgInitialised of system-settings is true then
   * throws exception
   */
  private void throwExceptionIfRootOrgAlreadyInitialised() {
    ProjectLogger.log(
        "SystemInitActor: checkIfRootOrgAlreadyInitialised called", LoggerEnum.DEBUG.name());
    try {
      this.systemSetting = systemSettingService.readSetting(JsonKey.IS_ROOT_ORG_INITIALISED);
      ProjectLogger.log(
          "SystemInitActor:checkIfRootOrgAlreadyInitialised: isInitialisedRootOrg ="
              + this.systemSetting,
          LoggerEnum.DEBUG.name());
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    if (isNotNull(this.systemSetting)
        && isNotNull(this.systemSetting.getValue())
        && this.systemSetting.getValue().equals("true")) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.systemAlreadyInitialised,
          ResponseCode.systemAlreadyInitialised.getErrorMessage());
    }
  }

  /**
   * This method will do the channel uniqueness validation
   *
   * @param req request map conatins the request data of organisation
   */
  private void validateChannel(Map<String, Object> req) {
    if (!validateChannelForUniqueness((String) req.get(JsonKey.CHANNEL))) {
      ProjectLogger.log("Channel validation failed");
      ProjectCommonException.throwClientErrorException(
          ResponseCode.channelUniquenessInvalid,
          ResponseCode.channelUniquenessInvalid.getErrorMessage());
    }
  }

  /**
   * validates if channel is already present in the organisation
   *
   * @param channel channel value of the organisation
   * @return boolean returns true if channel is unique ,else false
   */
  @SuppressWarnings("unchecked")
  private boolean validateChannelForUniqueness(String channel) {
    if (!StringUtils.isBlank(channel)) {
      Map<String, Object> filters = new HashMap<>();
      filters.put(JsonKey.CHANNEL, channel);
      filters.put(JsonKey.IS_ROOT_ORG, true);
      Map<String, Object> esResult =
          elasticSearchComplexSearch(
              filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
      if (isNotNull(esResult)
          && esResult.containsKey(JsonKey.CONTENT)
          && isNotNull(esResult.get(JsonKey.CONTENT))
          && ((List) esResult.get(JsonKey.CONTENT)).size() > 0) {
        return false;
      }
    }
    return true;
  }

  private Map<String, Object> elasticSearchComplexSearch(
      Map<String, Object> filters, String index, String type) {
    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    return ElasticSearchUtil.complexSearch(searchDTO, index, type);
  }
}
