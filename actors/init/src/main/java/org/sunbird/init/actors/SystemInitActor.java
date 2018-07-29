package org.sunbird.init.actors;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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

/**
 * This actor class contains actor methods for System initialisation
 *
 * @author Loganathan Shanmugam
 */
@ActorConfig(
  tasks = {"systemInitRootOrg"},
  asyncTasks = {}
)
public class SystemInitActor extends BaseActor {
  private final CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private SystemSetting systemSetting;
  private SystemSettingService systemSettingService = new SystemSettingServiceImpl();
  private Integer WRITE_SETTINGS_RETRIES_ALLOWED = 1;

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
   * This Method to initalises the first root org creation after validating the data
   *
   * @param actorMessage Instance of Request class contains the organisation data to be created
   */
  @SuppressWarnings("unchecked")
  private void systemInitRootOrg(Request actorMessage) {
      ProjectLogger.log("systemInitRootOrg method call started");
      SystemSetting initStatus = getRootOrgInitialisationStatus();
      if (isNull(initStatus) || isNull(initStatus.getValue())) {
        setRootOrgInitialisationStatus(JsonKey.STARTED);
        checkAndCreateRootOrg(actorMessage);
        setRootOrgInitialisationStatus(JsonKey.COMPLETED);
      } else if (initStatus.getValue().equals(JsonKey.STARTED)) {
        checkAndCreateRootOrg(actorMessage);
        setRootOrgInitialisationStatus(JsonKey.COMPLETED);
      } else if (initStatus.getValue().equals(JsonKey.COMPLETED)) {
        ProjectCommonException.throwClientErrorException(
                ResponseCode.systemAlreadyInitialised,
                ResponseCode.systemAlreadyInitialised.getErrorMessage());
      } else {
        ProjectCommonException.throwServerErrorException(
                ResponseCode.internalError,
                ResponseCode.internalError.getErrorMessage());
      }
  }

/**
 * This methods checks cassandra and returns rootOrg info if exists
 *
 * @return Map<String,Object> contains the orgData
 */
private Map<String, Object>  getRootOrgIfExists(){
  Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
  Map<String, Object> queryMap = new HashMap<String, Object>();
  queryMap.put(JsonKey.IS_ROOT_ORG,true);
  Response response =
          cassandraOperation.getRecordsByProperties(
                  orgDbInfo.getKeySpace(),
                  orgDbInfo.getTableName(),
                  queryMap);
  List<Map<String, Object>> orgList =
          (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
  if(orgList.size() > 0){
    return orgList.get(0);
  }
  return null;
}

  /**
 * This method checks for rootOrg exists in es or not
 * 
 * @return Boolean true if exists else false
 */
  private boolean isRootOrgIndexExists() {
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.IS_ROOT_ORG, true);
    Map<String, Object> esResult =
            doElasticSearchComplexSearch(
            filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
    if (isNotNull(esResult)
        && esResult.containsKey(JsonKey.CONTENT)
        && isNotNull(esResult.get(JsonKey.CONTENT))
        && ((List) esResult.get(JsonKey.CONTENT)).size() > 0) {
      return true;
    }
    return false;
  }

  /**
   * This methods frames the db request data for init rootOrg
   *
   * @param actorMessage actor data got create rootOrg
   * @return request object with org data
   */

  private Map<String, Object> getFirstRootOrgParams(Request actorMessage){
    Map<String, Object> req =
            (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
    req.put(JsonKey.CREATED_BY, JsonKey.INITIALISER);
    req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    req.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
    req.put(JsonKey.IS_ROOT_ORG, true);
    req.put(JsonKey.IS_DEFAULT, true);
    return req;
  }

  /**
   * This method saves the orgData into elastic search
   * @param req request data to save organisation into ES
   */
  private void saveRootOrgInfoToElastic(Map<String, Object> req) {
    Request orgIndexReq = new Request();
    orgIndexReq.getRequest().put(JsonKey.ORGANISATION, req);
    orgIndexReq.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
    ProjectLogger.log("Calling background job to save org data into ES " + req.get(JsonKey.ID));
    tellToAnother(orgIndexReq);
  }

  /**
 * This method creates rootOrg if it is not exists already
 * 
 * @param actorMessage instance of Request class with rootOrg data.
 */
  private void checkAndCreateRootOrg(Request actorMessage) {
    Map<String, Object> rootOrgDetails = getRootOrgIfExists();
    Response result = new Response();
    Map<String, Object> req = getFirstRootOrgParams(actorMessage);
    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    if (isNull(rootOrgDetails)) {
      validateChannelForUniqueness(req);
      req.put(JsonKey.ID, uniqueId);
      req.put(JsonKey.ROOT_ORG_ID, uniqueId);
      req.put(JsonKey.HASHTAGID, uniqueId);
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
      String slug = Slug.makeSlug((String) req.get(JsonKey.CHANNEL), true);
      if (isSlugUnique(slug)) {
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
      result = cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), req);
      ProjectLogger.log("Org data saved into cassandra.Created org id is ----." + uniqueId);
      result.getResult().put(JsonKey.ORGANISATION_ID, uniqueId);
      sender().tell(result, self());
      saveRootOrgInfoToElastic(req);
    } else {
      result.getResult().put(JsonKey.ORGANISATION_ID, rootOrgDetails.get(JsonKey.ID));
      sender().tell(result, self());
      if (!isRootOrgIndexExists()) {
        saveRootOrgInfoToElastic(rootOrgDetails);
      }
    }
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
              doElasticSearchComplexSearch(
              filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
      if (isNotNull(esResult)
          && esResult.containsKey(JsonKey.CONTENT)
          && isNotNull(esResult.get(JsonKey.CONTENT))) {
        return (((List) esResult.get(JsonKey.CONTENT)).isEmpty());
      }
    }
    return false;
  }

/**
 * This method sets the 'isRootOrgInitialised' flag in system settings
 * 
 * @param status value of initialisation status 'started' or 'completed'
 */
  private void setRootOrgInitialisationStatus(String status) {
    ProjectLogger.log("SystemInitActor: setRootOrgInitialisationStatus called", LoggerEnum.DEBUG.name());
    for (int i = 0; i <= WRITE_SETTINGS_RETRIES_ALLOWED; i++) {
      try {
        this.systemSetting =
                new SystemSetting(
                        JsonKey.IS_ROOT_ORG_INITIALISED, JsonKey.IS_ROOT_ORG_INITIALISED, status);
        Response response = systemSettingService.setSetting(systemSetting);
        ProjectLogger.log(
                "Insert operation result for initialised status =  "
                        + response.getResult().get(JsonKey.RESPONSE),
                LoggerEnum.DEBUG.name());
        break;
      } catch (Exception e) {
        ProjectLogger.log(
                "Exception while updating rootOrg init status. "+e.getMessage(),
                LoggerEnum.DEBUG.name());
        if (i == WRITE_SETTINGS_RETRIES_ALLOWED) {
          ProjectLogger.log(
                  "Max retries reached while updating rootOrg init status.",
                  LoggerEnum.DEBUG.name());
          ProjectCommonException.throwServerErrorException(
                  ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
        }
      }
    }
  }

  /**
   * This method gets the isRootOrgInitalised System setting if it already exists
   *
   *  @return SystemSetting instance of SystemSetting class with id,field,value
   */
  private SystemSetting getRootOrgInitialisationStatus() {
    SystemSetting initSetting = null;
    ProjectLogger.log("SystemInitActor: getRootOrgInitialisationStatus called", LoggerEnum.DEBUG.name());
    try {
      initSetting = systemSettingService.readSetting(JsonKey.IS_ROOT_ORG_INITIALISED);
    } catch (Exception e) {
      ProjectCommonException.throwServerErrorException(
          ResponseCode.SERVER_ERROR, ResponseCode.SERVER_ERROR.getErrorMessage());
    }
    return initSetting;
  }

  /**
   * This method will do the channel uniqueness validation
   *
   * @param req request map contains the request data of organisation
   */
  private void validateChannelForUniqueness(Map<String, Object> req) {
    String channel = (String) req.get(JsonKey.CHANNEL);
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.CHANNEL, channel);
    filters.put(JsonKey.IS_ROOT_ORG, true);
    Map<String, Object> esResult =
            doElasticSearchComplexSearch(
                    filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
    if (isNotNull(esResult)
            && esResult.containsKey(JsonKey.CONTENT)
            && isNotNull(esResult.get(JsonKey.CONTENT))
            && ((List) esResult.get(JsonKey.CONTENT)).size() > 0) {
      ProjectLogger.log("Channel validation failed");
      ProjectCommonException.throwClientErrorException(
              ResponseCode.channelUniquenessInvalid,
              ResponseCode.channelUniquenessInvalid.getErrorMessage());
    }
  }


  private Map<String, Object> doElasticSearchComplexSearch(
      Map<String, Object> filters, String index, String type) {
    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    return ElasticSearchUtil.complexSearch(searchDTO, index, type);
  }
}
