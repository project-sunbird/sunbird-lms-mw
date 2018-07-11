package org.sunbird.learner.actors;

import static org.sunbird.learner.util.Util.isNotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
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
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.Slug;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.models.organization.Organization;
import org.sunbird.telemetry.util.TelemetryUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This actor will handle organisation related operation .
 *
 * @author Amit Kumar
 * @author Arvind
 */
@ActorConfig(
  tasks = {
    "createFirstRootOrg"
  },
  asyncTasks = {}
)
public class InitialisationActor extends BaseActor {
  private ObjectMapper mapper = new ObjectMapper();
  private final CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private final EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.ORGANISATION);
    // set request id fto thread loacl...
    ExecutionContext.setRequestId(request.getRequestId());
    if (request.getOperation().equalsIgnoreCase(ActorOperations.CREATE_FIRST_ROOTORG.getValue())) {
      createFirstRootOrg(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }
  /** Method to create an first root organization. */
  @SuppressWarnings("unchecked")
  private void createFirstRootOrg(Request actorMessage) {
    ProjectLogger.log("createFirstRootOrg method call started");
    checkIfAlreadyInitialised();   
    try {
      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
     
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
	  
      if (req.containsKey(JsonKey.CHANNEL)) {
        validateChannel(req);
      }    

      String updatedBy = "INITIALIZER";
      req.put(JsonKey.CREATED_BY, updatedBy);
      String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
      req.put(JsonKey.ID, uniqueId);
      req.put(JsonKey.ROOT_ORG_ID, uniqueId);
      req.put(JsonKey.HASHTAGID, uniqueId);
      req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      req.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
      req.put(JsonKey.IS_ROOT_ORG, true);
      req.put(JsonKey.IS_DEFAULT,true);
 
      String slug = Slug.makeSlug((String) req.getOrDefault(JsonKey.CHANNEL, ""), true);        
      boolean bool = isSlugUnique(slug);
          if (bool) {
            req.put(JsonKey.SLUG, slug);
          } else {
            ProjectCommonException exception =
                new ProjectCommonException(
                    ResponseCode.slugIsNotUnique.getErrorCode(),
                    ResponseCode.slugIsNotUnique.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          }     
    
        boolean channelRegBool = Util.registerChannel(req);
        if (!channelRegBool) {
          ProjectCommonException exception =
              new ProjectCommonException(
                  ResponseCode.channelRegFailed.getErrorCode(),
                  ResponseCode.channelRegFailed.getErrorMessage(),
                  ResponseCode.SERVER_ERROR.getResponseCode());
          sender().tell(exception, self());
          return;
        }
      
      // This will remove all extra unnecessary parameter from request
      Organization org = mapper.convertValue(req, Organization.class);
      req = mapper.convertValue(org, Map.class);
      Response result =
          cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), req);
      // save initialisation status to system settings file    
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
    } catch (ProjectCommonException e) {
      ProjectLogger.log("Some error occurs" + e.getMessage());
      sender().tell(e, self());
      return;
    }
  }      
  
  /**
   * This method will do the channel uniqueness validation
   *
   * @param req
   */
  private void validateChannel(Map<String, Object> req) {
    if (!validateChannelForUniqueness((String) req.get(JsonKey.CHANNEL))) {
      ProjectLogger.log("Channel validation failed");
      throw new ProjectCommonException(
          ResponseCode.channelUniquenessInvalid.getErrorCode(),
          ResponseCode.channelUniquenessInvalid.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

   /**
   * validates if channel is already present in the organisation
   *
   * @param channel
   * @return boolean
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

private boolean isSlugUnique(String slug) {
  if (!StringUtils.isBlank(slug)) {
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


private String validateHashTagId(String hashTagId, String opType, String orgId) {
  Map<String, Object> filters = new HashMap<>();
  filters.put(JsonKey.HASHTAGID, hashTagId);
  SearchDTO searchDto = new SearchDTO();
  searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filters);
  Map<String, Object> result =
      ElasticSearchUtil.complexSearch(
          searchDto,
          ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.organisation.getTypeName());
  List<Map<String, Object>> dataMapList = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
  if (opType.equalsIgnoreCase(JsonKey.CREATE)) {
    if (!dataMapList.isEmpty()) {
      throw new ProjectCommonException(
          ResponseCode.invalidHashTagId.getErrorCode(),
          ResponseCode.invalidHashTagId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  } else if (opType.equalsIgnoreCase(JsonKey.UPDATE) && !dataMapList.isEmpty()) {
    Map<String, Object> orgMap = dataMapList.get(0);
    if (!(((String) orgMap.get(JsonKey.ID)).equalsIgnoreCase(orgId))) {
      throw new ProjectCommonException(
          ResponseCode.invalidHashTagId.getErrorCode(),
          ResponseCode.invalidHashTagId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }
  return hashTagId;
}


private void addInitialisationFlagToSystemSettings() {
  Map<String, Object> map = new HashMap<>();
  map.put(JsonKey.ID, JsonKey.IS_INITIALISED);
  map.put(JsonKey.FIELD,  JsonKey.IS_INITIALISED);
  map.put(JsonKey.VALUE, String.valueOf(true));
  Response response = cassandraOperation.insertRecord(JsonKey.SUNBIRD, JsonKey.SYSTEM_SETTINGS_DB, map);
  ProjectLogger.log(
      "Insert operation result for initialised status =  "
          + response.getResult().get(JsonKey.RESPONSE),LoggerEnum.INFO.name());
}

private void checkIfAlreadyInitialised(){
  ProjectLogger.log("get initSetting record started" , LoggerEnum.INFO.name());
  Response response =
        cassandraOperation.getRecordById(JsonKey.SUNBIRD, JsonKey.SYSTEM_SETTINGS_DB, JsonKey.IS_INITIALISED);
     List<Map<String, Object>> initSetting = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE); 
     ProjectLogger.log("initSetting record "+initSetting , LoggerEnum.INFO.name());   
    if (initSetting != null && !initSetting.isEmpty()) {      
        throw new ProjectCommonException(
          ResponseCode.systemAlreadyInitialised.getErrorCode(),
          ResponseCode.systemAlreadyInitialised.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());      
    }
}


}
