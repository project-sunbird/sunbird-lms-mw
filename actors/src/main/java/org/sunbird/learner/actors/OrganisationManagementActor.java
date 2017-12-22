package org.sunbird.learner.actors;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

import akka.actor.UntypedAbstractActor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
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
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;

/**
 * This actor will handle organisation related operation .
 *
 * @author Amit Kumar
 * @author Arvind
 */
public class OrganisationManagementActor extends UntypedAbstractActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("OrganisationManagementActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_ORG.getValue())) {
          createOrg(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.UPDATE_ORG_STATUS.getValue())) {
          updateOrgStatus(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.UPDATE_ORG.getValue())) {
          updateOrgData(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.APPROVE_ORG.getValue())) {
          approveOrg(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_ORG_DETAILS.getValue())) {
          getOrgDetails(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.JOIN_USER_ORGANISATION.getValue())) {
          joinUserOrganisation(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.APPROVE_USER_ORGANISATION.getValue())) {
          approveUserOrg(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.REJECT_USER_ORGANISATION.getValue())) {
          rejectUserOrg(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ADD_MEMBER_ORGANISATION.getValue())) {
          addMemberOrganisation(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue())) {
          removeMemberOrganisation(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_ORG_TYPE_LIST.getValue())) {
          getOrgTypeList(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.CREATE_ORG_TYPE.getValue())) {
          createOrgType(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.UPDATE_ORG_TYPE.getValue())) {
          updateOrgType(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION", LoggerEnum.INFO.name());
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                  ResponseCode.invalidOperationName.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    } else {
      // Throw exception as message body
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }


  private void updateOrgType(Request actorMessage) {
    ProjectLogger.log("updateOrgType method call start");
    try {
      Util.DbInfo orgTypeDbInfo = Util.dbInfoMap.get(JsonKey.ORG_TYPE_DB);
      Map<String, Object> request = actorMessage.getRequest();
      Response result = cassandraOperation.getRecordsByProperty(orgTypeDbInfo.getKeySpace(),
          orgTypeDbInfo.getTableName(), JsonKey.NAME, request.get(JsonKey.NAME));
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        Map<String, Object> map = list.get(0);
        if (!(((String) map.get(JsonKey.ID)).equals((String) request.get(JsonKey.ID)))) {
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.orgTypeAlreadyExist.getErrorCode(),
                  ResponseCode.orgTypeAlreadyExist.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
          return;
        }
      }

      request.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      Response response = cassandraOperation.updateRecord(orgTypeDbInfo.getKeySpace(),
          orgTypeDbInfo.getTableName(), request);
      sender().tell(response, self());
   // update DataCacheHandler orgType map with new data
      new Thread() {
        @Override
        public void run() {
          if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
            DataCacheHandler.getOrgTypeMap().put(((String)request.get(JsonKey.NAME)).toLowerCase(),(String)request.get(JsonKey.ID));
          }
        }
      }.start();
    } catch (Exception e) {
      ProjectLogger.log("Exception Occurred while updating data to orgType table :: ", e);
      sender().tell(e, self());
    }
  }

  private void createOrgType(Request actorMessage) {
    ProjectLogger.log("createOrgType method call start");
    try {
      Util.DbInfo orgTypeDbInfo = Util.dbInfoMap.get(JsonKey.ORG_TYPE_DB);
      Map<String, Object> request = actorMessage.getRequest();
      Response result = cassandraOperation.getAllRecords(orgTypeDbInfo.getKeySpace(),
          orgTypeDbInfo.getTableName());
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        for (Map<String, Object> map : list) {
          if (((String) map.get(JsonKey.NAME))
              .equalsIgnoreCase((String) request.get(JsonKey.NAME))) {
            ProjectCommonException exception =
                new ProjectCommonException(ResponseCode.orgTypeAlreadyExist.getErrorCode(),
                    ResponseCode.orgTypeAlreadyExist.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          }
        }
      }
      request.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      request.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv()));
      Response response = cassandraOperation.insertRecord(orgTypeDbInfo.getKeySpace(),
          orgTypeDbInfo.getTableName(), request);
      sender().tell(response, self());
   // update DataCacheHandler orgType map with new data
      new Thread() {
        @Override
        public void run() {
          if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
            DataCacheHandler.getOrgTypeMap().put(((String)request.get(JsonKey.NAME)).toLowerCase(),(String)request.get(JsonKey.ID));
          }
        }
      }.start();
    } catch (Exception e) {
      ProjectLogger.log("Exception Occurred while inserting data to orgType table :: ", e);
      sender().tell(e, self());
    }
  }

  private void getOrgTypeList(Request actorMessage) {
    ProjectLogger.log("getOrgTypeList method call start");
    try {
      Util.DbInfo orgTypeDbInfo = Util.dbInfoMap.get(JsonKey.ORG_TYPE_DB);
      Response response = cassandraOperation.getAllRecords(orgTypeDbInfo.getKeySpace(),
          orgTypeDbInfo.getTableName());
      List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        for (Map<String, Object> map : list) {
          map.remove(JsonKey.CREATED_BY);
          map.remove(JsonKey.CREATED_DATE);
          map.remove(JsonKey.UPDATED_BY);
          map.remove(JsonKey.UPDATED_DATE);
        }
      }
      sender().tell(response, self());
    } catch (Exception e) {
      ProjectLogger.log("Exception Occurred while fetching orgType List :: ", e);
      sender().tell(e, self());
    }
  }

  /**
   * Method to create an organization .
   */
  @SuppressWarnings("unchecked")
  private void createOrg(Request actorMessage) {
    ProjectLogger.log("Create org method call start");
    try {
      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
      if (req.containsKey(JsonKey.ORG_TYPE)
          && !ProjectUtil.isStringNullOREmpty((String) req.get(JsonKey.ORG_TYPE))) {
        req.put(JsonKey.ORG_TYPE_ID, validateOrgType((String) req.get(JsonKey.ORG_TYPE)));
      }
      
      if (req.containsKey(JsonKey.LOC_ID)
          && !ProjectUtil.isStringNullOREmpty((String) req.get(JsonKey.LOC_ID))) {
        req.put(JsonKey.LOC_ID, validateLocationId((String) req.get(JsonKey.LOC_ID)));
      }
      
      Map<String, Object> addressReq = null;
      if (null != actorMessage.getRequest().get(JsonKey.ADDRESS)) {
        addressReq = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ADDRESS);
      }
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
      boolean isChannelVerified = false;
      // combination of source and external id should be unique ...
      if (req.containsKey(JsonKey.PROVIDER) || req.containsKey(JsonKey.EXTERNAL_ID)) {
        validateExternalIdAndProvider((String) req.get(JsonKey.PROVIDER),
            (String) req.get(JsonKey.EXTERNAL_ID));
        validateChannelIdForRootOrg(req);
        if (req.containsKey(JsonKey.CHANNEL)) {
          isChannelVerified = true;
          validateChannel(req);
        } else {
          // if Channel value is not coming then check for provider
          // now if any org has same channel value as provider and that
          // org is rootOrg then set that root orgid with current orgId.
          if (req.containsKey(JsonKey.PROVIDER)) {
            String rootOrgId = getRootOrgIdFromChannel((String) req.get(JsonKey.PROVIDER));
            if (!ProjectUtil.isStringNullOREmpty(rootOrgId)) {
              req.put(JsonKey.ROOT_ORG_ID, rootOrgId);
            }
          }
        }

        Map<String, Object> dbMap = new HashMap<>();
        dbMap.put(JsonKey.PROVIDER, req.get(JsonKey.PROVIDER));
        dbMap.put(JsonKey.EXTERNAL_ID, req.get(JsonKey.EXTERNAL_ID));
        Response result = cassandraOperation.getRecordsByProperties(orgDbInfo.getKeySpace(),
            orgDbInfo.getTableName(), dbMap);
        List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (!(list.isEmpty())) {
          ProjectLogger.log("Org exist with Source " + req.get(JsonKey.PROVIDER) + " , External Id "
              + req.get(JsonKey.EXTERNAL_ID));
          ProjectCommonException exception = new ProjectCommonException(
              ResponseCode.sourceAndExternalIdAlreadyExist.getErrorCode(),
              ResponseCode.sourceAndExternalIdAlreadyExist.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
          return;
        }
      }
      if (req.containsKey(JsonKey.CHANNEL) && !isChannelVerified) {
        validateChannel(req);
      }

      String relation = (String) req.get(JsonKey.RELATION);
      req.remove(JsonKey.RELATION);

      String parentOrg = (String) req.get(JsonKey.PARENT_ORG_ID);

      Boolean isValidParent = false;
      if (!ProjectUtil.isStringNullOREmpty(parentOrg) && isValidParent(parentOrg)) {
        validateRootOrg(req);
        isValidParent = true;
      }
      String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        req.put(JsonKey.CREATED_BY, updatedBy);
      }
      String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
      req.put(JsonKey.ID, uniqueId);
      req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      req.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
      // removing default from request, not allowing user to create default org.
      req.remove(JsonKey.IS_DEFAULT);
      // allow lower case values for source and externalId to the database
      if (req.get(JsonKey.PROVIDER) != null) {
        req.put(JsonKey.PROVIDER, ((String) req.get(JsonKey.PROVIDER)).toLowerCase());
      }
      if (req.get(JsonKey.EXTERNAL_ID) != null) {
        req.put(JsonKey.EXTERNAL_ID, ((String) req.get(JsonKey.EXTERNAL_ID)).toLowerCase());
      }
      // update address if present in request
      if (null != addressReq && addressReq.size() > 0) {
        String addressId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        addressReq.put(JsonKey.ID, addressId);
        addressReq.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());

        if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
          addressReq.put(JsonKey.CREATED_BY, updatedBy);
        }
        upsertAddress(addressReq);
        req.put(JsonKey.ADDRESS_ID, addressId);
      }
      if (ProjectUtil.isStringNullOREmpty((String) req.get(JsonKey.ROOT_ORG_ID))) {
        req.put(JsonKey.ROOT_ORG_ID, JsonKey.DEFAULT_ROOT_ORG_ID);
      }
      // adding one extra filed for tag.
      if(!ProjectUtil.isStringNullOREmpty(((String)req.get(JsonKey.HASHTAGID)))){
        req.put(JsonKey.HASHTAGID,validateHashTagId(((String)req.get(JsonKey.HASHTAGID)),JsonKey.CREATE,""));
      }else{
       req.put(JsonKey.HASHTAGID, uniqueId);
      }
      // if channel is available then make slug for channel.
      // remove the slug key if coming from user input
      req.remove(JsonKey.SLUG);
      if (req.containsKey(JsonKey.CHANNEL)) {
        req.put(JsonKey.SLUG, Slug.makeSlug((String) req.getOrDefault(JsonKey.CHANNEL, ""), true));
      }
      // check contactDetail filed is coming or not. it will always come as list of map
      List<Map<String, Object>> listOfMap = null;
      if (req.containsKey(JsonKey.CONTACT_DETAILS)) {
        listOfMap = (List<Map<String, Object>>) req.get(JsonKey.CONTACT_DETAILS);
        if (listOfMap != null && !listOfMap.isEmpty()) {
          ObjectMapper mapper = new ObjectMapper();
          try {
            req.put(JsonKey.CONTACT_DETAILS, mapper.writeValueAsString(listOfMap));
          } catch (IOException e) {
            ProjectLogger.log(e.getMessage(), e);
          }
        }
      }
      if (ProjectUtil.isNull(req.get(JsonKey.IS_ROOT_ORG))) {
        req.put(JsonKey.IS_ROOT_ORG, false);
      }
      Response result =
          cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), req);
      ProjectLogger.log("Org data saved into cassandra.");
      // create org_map if parentOrgId is present in request
      if (isValidParent) {
        upsertOrgMap(uniqueId, parentOrg, (String) req.get(JsonKey.ROOT_ORG_ID),
            actorMessage.getEnv(), relation);
      }

      ProjectLogger.log("Created org id is ----." + uniqueId);
      result.getResult().put(JsonKey.ORGANISATION_ID, uniqueId);
      sender().tell(result, self());

      if (null != addressReq) {
        req.put(JsonKey.ADDRESS, addressReq);
      }
      if (listOfMap != null) {
        req.put(JsonKey.CONTACT_DETAILS, listOfMap);
      } else {
        listOfMap = new ArrayList<Map<String, Object>>();
        req.put(JsonKey.CONTACT_DETAILS, listOfMap);
      }
      Request orgReq = new Request();
      orgReq.getRequest().put(JsonKey.ORGANISATION, req);
      orgReq.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
      ProjectLogger.log("Calling background job to save org data into ES" + uniqueId);
      ActorUtil.tell(orgReq);
    } catch (ProjectCommonException e) {
      ProjectLogger.log("Some error occurs" + e.getMessage());
      sender().tell(e, self());
      return;
    }
  }

  private String validateHashTagId(String hashTagId,String opType,String orgId) {
    Map<String,Object> filters =  new HashMap<>();
    filters.put(JsonKey.HASHTAGID, hashTagId);
    SearchDTO searchDto = new SearchDTO();
    searchDto.getAdditionalProperties().put(JsonKey.FILTERS,filters);
    Map<String, Object> result = ElasticSearchUtil.complexSearch(searchDto,
        ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.organisation.getTypeName());
    List<Map<String, Object>> dataMapList =
        (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
    if(opType.equalsIgnoreCase(JsonKey.CREATE)){
      if(!dataMapList.isEmpty()){
       throw new ProjectCommonException(ResponseCode.invalidHashTagId.getErrorCode(),
          ResponseCode.invalidHashTagId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }else if(opType.equalsIgnoreCase(JsonKey.UPDATE) && !dataMapList.isEmpty()){
      Map<String, Object> orgMap = dataMapList.get(0);
      if(!(((String)orgMap.get(JsonKey.ID)).equalsIgnoreCase(orgId))){
        throw new ProjectCommonException(ResponseCode.invalidHashTagId.getErrorCode(),
            ResponseCode.invalidHashTagId.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    return hashTagId;
  }


  private String validateLocationId(String locId) {
    Util.DbInfo geoLocDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
    Response response = cassandraOperation.getRecordById(geoLocDbInfo.getKeySpace(), geoLocDbInfo.getTableName(), locId);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (!list.isEmpty()) {
      Map<String, Object> map = list.get(0);
      return ((String)map.get(JsonKey.ID));
    }else{
      throw new ProjectCommonException(ResponseCode.invalidLocationId.getErrorCode(),
          ResponseCode.invalidLocationId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }


  private String validateOrgType(String orgType) {
    String orgTypeId = null;
    if (!ProjectUtil
        .isStringNullOREmpty((String) DataCacheHandler.getOrgTypeMap().get(orgType.toLowerCase()))) {
      orgTypeId = DataCacheHandler.getOrgTypeMap().get(orgType.toLowerCase());
    } else {
      Util.DbInfo orgTypeDbInfo = Util.dbInfoMap.get(JsonKey.ORG_TYPE_DB);
      Response response = cassandraOperation.getAllRecords(orgTypeDbInfo.getKeySpace(),
          orgTypeDbInfo.getTableName());
      List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (!list.isEmpty()) {
        for (Map<String, Object> map : list) {
          if((((String)map.get(JsonKey.NAME)).toLowerCase()).equalsIgnoreCase(orgType.toLowerCase())){
            orgTypeId = (String)map.get(JsonKey.ID);
            DataCacheHandler.getOrgTypeMap().put(((String)map.get(JsonKey.NAME)).toLowerCase(), (String)map.get(JsonKey.ID));
          }
        }
      }
      if(null == orgTypeId){
        throw new ProjectCommonException(ResponseCode.invalidOrgType.getErrorCode(),
            ResponseCode.invalidOrgType.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    return orgTypeId;
  }


  /**
   * Method to approve the organisation only if the org in inactive state
   */
  @SuppressWarnings("unchecked")
  private void approveOrg(Request actorMessage) {

    try {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
      if (!(validateOrgRequest(req))) {
        ProjectLogger.log("REQUESTED DATA IS NOT VALID");
        return;
      }

      Map<String, Object> orgDBO;
      Map<String, Object> updateOrgDBO = new HashMap<String, Object>();
      String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

      String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
      Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), orgId);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        orgDBO = (Map<String, Object>) list.get(0);
      } else {
        ProjectLogger.log("Invalid Org Id");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      updateOrgDBO.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        updateOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
        updateOrgDBO.put(JsonKey.APPROVED_BY, updatedBy);
      }
      updateOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      updateOrgDBO.put(JsonKey.ID, (String) orgDBO.get(JsonKey.ID));
      updateOrgDBO.put(JsonKey.IS_APPROVED, true);
      updateOrgDBO.put(JsonKey.APPROVED_DATE, ProjectUtil.getFormattedDate());

      Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), updateOrgDBO);
      response.getResult().put(JsonKey.ORGANISATION_ID, orgDBO.get(JsonKey.ID));
      sender().tell(response, self());
      // update the ES --
      Request request = new Request();
      request.getRequest().put(JsonKey.ORGANISATION, updateOrgDBO);
      request.setOperation(ActorOperations.UPDATE_ORG_INFO_ELASTIC.getValue());
      ActorUtil.tell(request);
      return;
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }
  }

  /**
   * Updates the status of the organization
   */
  @SuppressWarnings("unchecked")
  private void updateOrgStatus(Request actorMessage) {

    try {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);

      if (!(validateOrgRequest(req))) {
        ProjectLogger.log("REQUESTED DATA IS NOT VALID");
        return;
      }
      Map<String, Object> orgDBO;
      Map<String, Object> updateOrgDBO = new HashMap<>();
      String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

      String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
      Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), orgId);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        orgDBO = list.get(0);
      } else {
        ProjectLogger.log("Invalid Org Id");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      Integer currentStatus = (Integer) orgDBO.get(JsonKey.STATUS);
      Integer nextStatus = ((BigInteger) req.get(JsonKey.STATUS)).intValue();
      if (!(Util.checkOrgStatusTransition(currentStatus, nextStatus))) {

        ProjectLogger.log("Invalid Org State transation");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        updateOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      }
      updateOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      updateOrgDBO.put(JsonKey.ID, (String) orgDBO.get(JsonKey.ID));
      updateOrgDBO.put(JsonKey.STATUS, nextStatus);

      Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), updateOrgDBO);
      response.getResult().put(JsonKey.ORGANISATION_ID, orgDBO.get(JsonKey.ID));
      sender().tell(response, self());
      // update the ES --
      Request request = new Request();
      request.getRequest().put(JsonKey.ORGANISATION, updateOrgDBO);
      request.setOperation(ActorOperations.UPDATE_ORG_INFO_ELASTIC.getValue());
      ActorUtil.tell(request);

      return;
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }
  }

  /**
   * Update the organization data
   */
  @SuppressWarnings("unchecked")
  private void updateOrgData(Request actorMessage) throws ProjectCommonException {

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    try {
      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
      if (req.containsKey(JsonKey.ORG_TYPE)
          && !ProjectUtil.isStringNullOREmpty((String) req.get(JsonKey.ORG_TYPE))) {
        req.put(JsonKey.ORG_TYPE_ID, validateOrgType((String) req.get(JsonKey.ORG_TYPE)));
      }
      if (req.containsKey(JsonKey.LOC_ID)
          && !ProjectUtil.isStringNullOREmpty((String) req.get(JsonKey.LOC_ID))) {
        req.put(JsonKey.LOC_ID, validateLocationId((String) req.get(JsonKey.LOC_ID)));
      }
      if (!(validateOrgRequest(req))) {
        ProjectLogger.log("REQUESTED DATA IS NOT VALID");
        return;
      }
      // validate if Source and external id is in request , it should not already exist in DataBase
      // .....
      if (req.containsKey(JsonKey.PROVIDER) || req.containsKey(JsonKey.EXTERNAL_ID)) {
        validateExternalIdAndProvider((String) req.get(JsonKey.EXTERNAL_ID),
            (String) req.get(JsonKey.PROVIDER));
        Map<String, Object> dbMap = new HashMap<>();
        dbMap.put(JsonKey.PROVIDER, req.get(JsonKey.PROVIDER));
        dbMap.put(JsonKey.EXTERNAL_ID, req.get(JsonKey.EXTERNAL_ID));
        Response result = cassandraOperation.getRecordsByProperties(orgDbInfo.getKeySpace(),
            orgDbInfo.getTableName(), dbMap);
        List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (!(list.isEmpty())) {
          String organisationId = (String) list.get(0).get(JsonKey.ID);

          if (!(req.get(JsonKey.ORGANISATION_ID).equals(organisationId))) {
            ProjectLogger.log("Org exist with Source " + req.get(JsonKey.PROVIDER)
                + " , External Id " + req.get(JsonKey.EXTERNAL_ID));
            ProjectCommonException exception = new ProjectCommonException(
                ResponseCode.sourceAndExternalIdAlreadyExist.getErrorCode(),
                ResponseCode.sourceAndExternalIdAlreadyExist.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          }
        }
      }
      validateChannelIdForRootOrg(req);
      //
      boolean channelAdded = false;
      if ((!req.containsKey(JsonKey.CHANNEL)) && req.containsKey(JsonKey.PROVIDER)) {
          // then make provider as channel to fetch root org id.
          req.put(JsonKey.CHANNEL, req.get(JsonKey.PROVIDER));
          channelAdded = true;
      }
      if (req.containsKey(JsonKey.CHANNEL)) {
        if (!req.containsKey(JsonKey.IS_ROOT_ORG) || !(Boolean) req.get(JsonKey.IS_ROOT_ORG)) {
          String rootOrgId = getRootOrgIdFromChannel((String) req.get(JsonKey.CHANNEL));
          if (!ProjectUtil.isStringNullOREmpty(rootOrgId) || channelAdded) {
            req.put(JsonKey.ROOT_ORG_ID,
                rootOrgId.equals("") ? JsonKey.DEFAULT_ROOT_ORG_ID : rootOrgId);
          } else {
            ProjectLogger.log("Invalid channel id.");
            ProjectCommonException exception =
                new ProjectCommonException(ResponseCode.invalidChannel.getErrorCode(),
                    ResponseCode.invalidChannel.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
          }
        } else if (!channelAdded
            && !validateChannelForUniquenessForUpdate((String) req.get(JsonKey.CHANNEL),
                (String) req.get(JsonKey.ORGANISATION_ID))) {
          ProjectLogger.log("Channel validation failed");
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.channelUniquenessInvalid.getErrorCode(),
                  ResponseCode.channelUniquenessInvalid.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
          return;
        }
      }
      // if channel is not coming and we added it from provider to collect the rootOrgId then
      // remove channel
      if (channelAdded) {
        req.remove(JsonKey.CHANNEL);
      }
      Map<String, Object> addressReq = null;
      if (null != actorMessage.getRequest().get(JsonKey.ADDRESS)) {
        addressReq = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ADDRESS);
      }
      String relation = (String) req.get(JsonKey.RELATION);
      // removing default from request, not allowing user to create default org.
      req.remove(JsonKey.IS_DEFAULT);
      req.remove(JsonKey.RELATION);
      // allow lower case values for source and externalId to the database
      if (req.get(JsonKey.PROVIDER) != null) {
        req.put(JsonKey.PROVIDER, ((String) req.get(JsonKey.PROVIDER)).toLowerCase());
      }
      if (req.get(JsonKey.EXTERNAL_ID) != null) {
        req.put(JsonKey.EXTERNAL_ID, ((String) req.get(JsonKey.EXTERNAL_ID)).toLowerCase());
      }
      String parentOrg = (String) req.get(JsonKey.PARENT_ORG_ID);
      Boolean isValidParent = false;
      ProjectLogger.log("Parent Org Id:" + parentOrg);
      if (!ProjectUtil.isStringNullOREmpty(parentOrg) && isValidParent(parentOrg)) {
        validateCyclicRelationForOrganisation(req);
        validateRootOrg(req);
        isValidParent = true;
      }
      Map<String, Object> orgDBO;
      Map<String, Object> updateOrgDBO = new HashMap<String, Object>();
      updateOrgDBO.putAll(req);
      updateOrgDBO.remove(JsonKey.ORGANISATION_ID);
      updateOrgDBO.remove(JsonKey.IS_APPROVED);
      updateOrgDBO.remove(JsonKey.APPROVED_BY);
      updateOrgDBO.remove(JsonKey.APPROVED_DATE);
      updateOrgDBO.remove(JsonKey.STATUS);
      String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

      String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
      Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), orgId);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        orgDBO = list.get(0);
      } else {
        ProjectLogger.log("Invalid Org Id");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      // update address if present in request
      if (null != addressReq && addressReq.size() > 0) {
        if (orgDBO.get(JsonKey.ADDRESS_ID) != null) {
          String addressId = (String) orgDBO.get(JsonKey.ADDRESS_ID);
          addressReq.put(JsonKey.ID, addressId);
        }
        // add new address record
        else {
          String addressId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
          addressReq.put(JsonKey.ID, addressId);
        }
        if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
          addressReq.put(JsonKey.UPDATED_BY, updatedBy);
          upsertAddress(addressReq);
        }
      }
      if (!ProjectUtil.isStringNullOREmpty(((String) req.get(JsonKey.HASHTAGID)))) {
        req.put(JsonKey.HASHTAGID,
            validateHashTagId(((String) req.get(JsonKey.HASHTAGID)), JsonKey.UPDATE, orgId));
      }
      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        updateOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      }
      updateOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      updateOrgDBO.put(JsonKey.ID, (String) orgDBO.get(JsonKey.ID));

      if (isValidParent) {
        upsertOrgMap((String) orgDBO.get(JsonKey.ID), parentOrg,
            (String) req.get(JsonKey.ROOT_ORG_ID), actorMessage.getEnv(), relation);
      }

      // if channel is available then make slug for channel.
      // remove the slug key if coming from user input
      updateOrgDBO.remove(JsonKey.SLUG);
      if (updateOrgDBO.containsKey(JsonKey.CHANNEL)) {
        updateOrgDBO.put(JsonKey.SLUG,
            Slug.makeSlug((String) updateOrgDBO.getOrDefault(JsonKey.CHANNEL, ""), true));
      }

      // check contactDetail filed is coming or not. it will always come as list of map
      List<Map<String, Object>> listOfMap = null;
      if (updateOrgDBO.containsKey(JsonKey.CONTACT_DETAILS)) {
        listOfMap = (List<Map<String, Object>>) updateOrgDBO.get(JsonKey.CONTACT_DETAILS);
        if (listOfMap != null && !listOfMap.isEmpty()) {
          ObjectMapper mapper = new ObjectMapper();
          try {
            updateOrgDBO.put(JsonKey.CONTACT_DETAILS, mapper.writeValueAsString(listOfMap));
          } catch (IOException e) {
            ProjectLogger.log(e.getMessage(), e);
          }
        }
      }
      Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), updateOrgDBO);
      response.getResult().put(JsonKey.ORGANISATION_ID, orgDBO.get(JsonKey.ID));
      sender().tell(response, self());

      if (null != addressReq) {
        updateOrgDBO.put(JsonKey.ADDRESS, addressReq);
      }
      if (listOfMap != null) {
        updateOrgDBO.put(JsonKey.CONTACT_DETAILS, listOfMap);
      } else {
        listOfMap = new ArrayList<Map<String, Object>>();
        updateOrgDBO.put(JsonKey.CONTACT_DETAILS, listOfMap);
      }
      Request request = new Request();
      request.getRequest().put(JsonKey.ORGANISATION, updateOrgDBO);
      request.setOperation(ActorOperations.UPDATE_ORG_INFO_ELASTIC.getValue());
      ActorUtil.tell(request);
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }
  }

  /**
   * Method to add member to the organisation
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void addMemberOrganisation(Request actorMessage) {

    Response response = new Response();

    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Util.DbInfo organisationDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);

    Map<String, Object> req = actorMessage.getRequest();

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if (!(validateOrgRequestForMembers(usrOrgData))) {
      ProjectLogger.log("REQUESTED DATA IS NOT VALID");
      return;
    }
    if (!(validateUsrRequest(usrOrgData))) {
      ProjectLogger.log("REQUESTED DATA IS NOT VALID");
      return;
    }
    // remove source and external id
    usrOrgData.remove(JsonKey.EXTERNAL_ID);
    usrOrgData.remove(JsonKey.SOURCE);
    usrOrgData.remove(JsonKey.PROVIDER);
    usrOrgData.remove(JsonKey.USERNAME);
    usrOrgData.remove(JsonKey.USER_NAME);
    usrOrgData.put(JsonKey.IS_DELETED, false);

    String updatedBy = null;
    String orgId = null;
    String userId = null;
    List<String> roles = new ArrayList<>();
    if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
      orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
    }
    if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
      userId = (String) usrOrgData.get(JsonKey.USER_ID);
    }
    if (isNotNull(req.get(JsonKey.REQUESTED_BY))) {
      updatedBy = (String) req.get(JsonKey.REQUESTED_BY);
    }
    if (isNotNull(usrOrgData.get(JsonKey.ROLES))) {
      roles.addAll((List<String>) usrOrgData.get(JsonKey.ROLES));
      if (!((List<String>) usrOrgData.get(JsonKey.ROLES)).isEmpty()) {
        String msg = Util.validateRoles((List<String>) usrOrgData.get(JsonKey.ROLES));
        if (!msg.equalsIgnoreCase(JsonKey.SUCCESS)) {
          throw new ProjectCommonException(ResponseCode.invalidRole.getErrorCode(),
              ResponseCode.invalidRole.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
    if (isNotNull(usrOrgData.get(JsonKey.ROLE))) {
      String role = usrOrgData.get(JsonKey.ROLE).toString();
      // partners are going to send these role values
      if (StringUtils.equalsIgnoreCase(role, "content-creator"))
        role = ProjectUtil.UserRole.CONTENT_CREATOR.getValue();
      else if (StringUtils.equalsIgnoreCase(role, "member"))
        role = ProjectUtil.UserRole.ORG_MEMBER.getValue();
      else if (StringUtils.equalsIgnoreCase(role, "admin"))
        role = ProjectUtil.UserRole.ORG_ADMIN.getValue();
      else if (StringUtils.equalsIgnoreCase(role, "content-reviewer"))
        role = ProjectUtil.UserRole.CONTENT_REVIEWER.getValue();
      roles.add(role);
    }
    usrOrgData.remove(JsonKey.ROLE);

    if ((isNull(orgId) || isNull(userId)) || isNull(roles)) {
      // create exception here invalid request data and tell the exception , then return
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    if (!roles.isEmpty())
      usrOrgData.put(JsonKey.ROLES, roles);
    // check org exist or not
    Response orgResult = cassandraOperation.getRecordById(organisationDbInfo.getKeySpace(),
        organisationDbInfo.getTableName(), orgId);

    List orgList = (List) orgResult.get(JsonKey.RESPONSE);
    if (orgList.isEmpty()) {
      // user already enrolled for the organisation
      ProjectLogger.log("Org does not exist");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user exist or not
    Response userResult = cassandraOperation.getRecordById(userDbInfo.getKeySpace(),
        userDbInfo.getTableName(), userId);

    List userList = (List) userResult.get(JsonKey.RESPONSE);
    if (userList.isEmpty()) {
      ProjectLogger.log("User does not exist");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidUserId.getErrorCode(), ResponseCode.invalidUserId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), requestData);

    List list = (List) result.get(JsonKey.RESPONSE);
    if (!list.isEmpty()) {
      // user already enrolled for the organisation
      response = new Response();
      response.getResult().put(JsonKey.RESPONSE, ResponseMessage.Message.EXISTING_ORG_MEMBER);
      sender().tell(response, self());
      return;
    }

    String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    usrOrgData.put(JsonKey.ID, id);
    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = Util.getUserNamebyUserId(updatedBy);
      usrOrgData.put(JsonKey.ADDED_BY, updatedBy);
      usrOrgData.put(JsonKey.APPROVED_BY, updatedBy);
      if (!ProjectUtil.isStringNullOREmpty(updatedByName)) {
        usrOrgData.put(JsonKey.ADDED_BY_NAME, updatedByName);
      }
    }
    usrOrgData.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
    usrOrgData.put(JsonKey.APPROOVE_DATE, ProjectUtil.getFormattedDate());
    usrOrgData.put(JsonKey.IS_REJECTED, false);
    usrOrgData.put(JsonKey.IS_APPROVED, true);

    response = cassandraOperation.insertRecord(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), usrOrgData);

    Map<String, Object> newOrgMap = new HashMap<>();
    if (!orgList.isEmpty()) {
      Integer count = 0;
      Map<String, Object> orgMap = (Map<String, Object>) orgList.get(0);
      if (isNotNull(orgMap.get(JsonKey.NO_OF_MEMBERS.toLowerCase()))) {
        count = Integer.valueOf((String) orgMap.get(JsonKey.NO_OF_MEMBERS.toLowerCase()));
      }
      newOrgMap.put(JsonKey.ID, orgId);
      newOrgMap.put(JsonKey.NO_OF_MEMBERS, count + 1);
      cassandraOperation.updateRecord(organisationDbInfo.getKeySpace(),
          organisationDbInfo.getTableName(), newOrgMap);
    }

    sender().tell(response, self());

    // update ES with latest data through background job manager
    if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      ProjectLogger.log("method call going to satrt for ES--.....");
      Request request = new Request();
      request.setOperation(ActorOperations.UPDATE_USER_ORG_ES.getValue());
      request.getRequest().put(JsonKey.USER, usrOrgData);
      ProjectLogger.log("making a call to save user data to ES");
      try {
        ActorUtil.tell(request);
      } catch (Exception ex) {
        ProjectLogger
            .log("Exception Occured during saving user to Es while addMemberOrganisation : ", ex);
      }
    } else {
      ProjectLogger.log("no call for ES to save user");
    }
    return;

  }

  /**
   * Method to remove member from the organisation
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void removeMemberOrganisation(Request actorMessage) {

    Response response = new Response();

    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Util.DbInfo organisationDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

    Map<String, Object> req = actorMessage.getRequest();

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if (!(validateUsrRequest(usrOrgData))) {
      ProjectLogger.log("REQUESTED DATA IS NOT VALID");
      return;
    }
    if (!(validateOrgRequestForMembers(usrOrgData))) {
      ProjectLogger.log("REQUESTED DATA IS NOT VALID");
      return;
    }

    // remove source and external id
    usrOrgData.remove(JsonKey.EXTERNAL_ID);
    usrOrgData.remove(JsonKey.SOURCE);
    usrOrgData.remove(JsonKey.USERNAME);
    usrOrgData.remove(JsonKey.USER_NAME);

    String updatedBy = null;
    String orgId = null;
    String userId = null;
    if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
      orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
    }

    if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
      userId = (String) usrOrgData.get(JsonKey.USER_ID);
    }
    if (isNotNull(req.get(JsonKey.REQUESTED_BY))) {
      updatedBy = (String) req.get(JsonKey.REQUESTED_BY);
    }

    if (isNull(orgId) || isNull(userId)) {
      // create exception here invalid request data and tell the exception , ther return
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), requestData);

    List list = (List) result.get(JsonKey.RESPONSE);
    if (list.isEmpty()) {
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    } else {
      Map<String, Object> dataMap = (Map<String, Object>) list.get(0);
      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        dataMap.put(JsonKey.UPDATED_BY, updatedBy);
      }
      dataMap.put(JsonKey.ORG_LEFT_DATE, ProjectUtil.getFormattedDate());
      dataMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      dataMap.put(JsonKey.IS_DELETED, true);
      response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace(),
          userOrgDbInfo.getTableName(), dataMap);
      Map<String, Object> newOrgMap = new HashMap<>();

      Response orgresult = cassandraOperation.getRecordById(organisationDbInfo.getKeySpace(),
          organisationDbInfo.getTableName(), orgId);
      List orgList = (List) orgresult.get(JsonKey.RESPONSE);
      if (!orgList.isEmpty()) {
        Map<String, Object> orgMap = (Map<String, Object>) orgList.get(0);
        if (isNotNull(orgMap.get(JsonKey.NO_OF_MEMBERS.toLowerCase()))) {
          Integer count = Integer.valueOf((String) orgMap.get(JsonKey.NO_OF_MEMBERS.toLowerCase()));
          newOrgMap.put(JsonKey.ID, orgId);
          newOrgMap.put(JsonKey.NO_OF_MEMBERS, count == 0 ? 0 : (count - 1));
          cassandraOperation.updateRecord(organisationDbInfo.getKeySpace(),
              organisationDbInfo.getTableName(), newOrgMap);
        }
      }
      sender().tell(response, self());

      // update ES with latest data through background job manager
      if (((String) response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
        ProjectLogger.log("method call going to satrt for ES--.....");
        Request request = new Request();
        request.setOperation(ActorOperations.REMOVE_USER_ORG_ES.getValue());
        request.getRequest().put(JsonKey.USER, dataMap);
        ProjectLogger.log("making a call to save user data to ES");
        try {
          ActorUtil.tell(request);
        } catch (Exception ex) {
          ProjectLogger.log(
              "Exception Occured during saving user to Es while removing memeber from Organisation : ",
              ex);
        }
      } else {
        ProjectLogger.log("no call for ES to save user");
      }
      return;
    }
  }

  /**
   * Provides the details of the organization
   */
  @SuppressWarnings("unchecked")
  private void getOrgDetails(Request actorMessage) {

    Map<String, Object> req =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
    if (!(validateOrgRequest(req))) {
      ProjectLogger.log("REQUESTED DATA IS NOT VALID");
      return;
    }
    String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
    Map<String, Object> result =
        ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.organisation.getTypeName(), orgId);
    Response response = new Response();
    if (result != null) {
      response.put(JsonKey.RESPONSE, result);
    } else {
      result = new HashMap<>();
      response.put(JsonKey.RESPONSE, result);
    }
    sender().tell(response, self());
  }

 
  /**
   * Method to join the user with organisation ...
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void joinUserOrganisation(Request actorMessage) {

    Response response = new Response();

    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Util.DbInfo organisationDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

    Map<String, Object> req = actorMessage.getRequest();

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if (!(validateOrgRequest(usrOrgData))) {
      ProjectLogger.log("REQUESTED DATA IS NOT VALID");
      return;
    }
    if (isNull(usrOrgData)) {
      // create exception here and sender.tell the exception and return
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // remove source and external id
    usrOrgData.remove(JsonKey.EXTERNAL_ID);
    usrOrgData.remove(JsonKey.SOURCE);
    usrOrgData.put(JsonKey.IS_DELETED, false);

    String updatedBy = null;
    String orgId = null;
    String userId = null;
    List<String> roles = new ArrayList<String>();
    if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
      orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
    }

    if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
      userId = (String) usrOrgData.get(JsonKey.USER_ID);
    }
    if (isNotNull(req.get(JsonKey.REQUESTED_BY))) {
      updatedBy = (String) req.get(JsonKey.REQUESTED_BY);
    }
    if (isNotNull(usrOrgData.get(JsonKey.ROLES))) {
      roles.addAll((List<String>) usrOrgData.get(JsonKey.ROLES));
    }

    if (isNull(orgId) || isNull(userId) || isNull(roles)) {
      // create exception here invalid request data and tell the exception , ther return
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    if (!roles.contains(ProjectUtil.UserRole.CONTENT_CREATOR.getValue())) {
      roles.add(ProjectUtil.UserRole.CONTENT_CREATOR.getValue());
      usrOrgData.put(JsonKey.ROLES, roles);
    }

    // check org exist or not
    Response orgResult = cassandraOperation.getRecordById(organisationDbInfo.getKeySpace(),
        organisationDbInfo.getTableName(), orgId);

    List orgList = (List) orgResult.get(JsonKey.RESPONSE);
    if (orgList.size() == 0) {
      // user already enrolled for the organisation
      ProjectLogger.log("Org does not exist");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<String, Object>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), requestData);

    List list = (List) result.get(JsonKey.RESPONSE);
    if (list.size() > 0) {
      // user already enrolled for the organisation
      response = new Response();
      response.getResult().put(JsonKey.RESPONSE, ResponseMessage.Message.EXISTING_ORG_MEMBER);
      sender().tell(response, self());
      return;
    }

    String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    usrOrgData.put(JsonKey.ID, id);
    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = getUserNamebyUserId(updatedBy);
      usrOrgData.put(JsonKey.ADDED_BY_NAME, updatedByName);
      usrOrgData.put(JsonKey.ADDED_BY, updatedBy);
    }
    usrOrgData.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
    usrOrgData.put(JsonKey.IS_REJECTED, false);
    usrOrgData.put(JsonKey.IS_APPROVED, false);

    response = cassandraOperation.insertRecord(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), usrOrgData);
    Map<String, Object> newOrgMap = new HashMap<String, Object>();
    if (orgList.size() > 0) {
      Integer count = 0;
      Map<String, Object> orgMap = (Map<String, Object>) orgList.get(0);
      if (isNotNull(orgMap.get(JsonKey.NO_OF_MEMBERS.toLowerCase()))) {
        count = Integer.valueOf((String) orgMap.get(JsonKey.NO_OF_MEMBERS.toLowerCase()));
      }
      newOrgMap.put(JsonKey.ID, orgId);
      newOrgMap.put(JsonKey.NO_OF_MEMBERS, count + 1);
      cassandraOperation.updateRecord(organisationDbInfo.getKeySpace(),
          organisationDbInfo.getTableName(), newOrgMap);
    }

    sender().tell(response, self());
    return;

  }

  /**
   * Method to approve the user organisation .
   */
  @SuppressWarnings("unchecked")
  private void approveUserOrg(Request actorMessage) {

    Response response = new Response();
    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

    Map<String, Object> updateUserOrgDBO = new HashMap<>();
    Map<String, Object> req = actorMessage.getRequest();
    String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if (!(validateOrgRequest(usrOrgData))) {
      ProjectLogger.log("REQUESTED DATA IS NOT VALID");
      return;
    }

    // remove source and external id
    usrOrgData.remove(JsonKey.EXTERNAL_ID);
    usrOrgData.remove(JsonKey.SOURCE);

    String orgId = null;
    String userId = null;
    List<String> roles = null;
    if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
      orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
    }

    if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
      userId = (String) usrOrgData.get(JsonKey.USER_ID);
    }
    if (isNotNull(usrOrgData.get(JsonKey.ROLES))) {
      roles = (List<String>) usrOrgData.get(JsonKey.ROLES);
    }

    if (isNull(orgId) || isNull(userId) || isNull(roles)) {
      // creeate exception here invalid request data and tell the exception , ther return
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<String, Object>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), requestData);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (list.size() == 0) {
      // user already enrolled for the organisation
      ProjectLogger.log("User does not belong to org");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String, Object> userOrgDBO = list.get(0);

    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = getUserNamebyUserId(updatedBy);
      updateUserOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      updateUserOrgDBO.put(JsonKey.APPROVED_BY, updatedByName);
    }
    updateUserOrgDBO.put(JsonKey.ID, (String) userOrgDBO.get(JsonKey.ID));
    updateUserOrgDBO.put(JsonKey.APPROOVE_DATE, ProjectUtil.getFormattedDate());
    updateUserOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());

    updateUserOrgDBO.put(JsonKey.IS_APPROVED, true);
    updateUserOrgDBO.put(JsonKey.IS_REJECTED, false);
    updateUserOrgDBO.put(JsonKey.IS_DELETED, false);
    updateUserOrgDBO.put(JsonKey.ROLES, roles);

    response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), updateUserOrgDBO);
    sender().tell(response, self());
    return;

  }

  /**
   * Method to reject the user organisation .
   */
  @SuppressWarnings("unchecked")
  private void rejectUserOrg(Request actorMessage) {

    Response response = new Response();
    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

    Map<String, Object> updateUserOrgDBO = new HashMap<>();
    Map<String, Object> req = actorMessage.getRequest();
    String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if (!(validateOrgRequest(usrOrgData))) {
      ProjectLogger.log("REQUESTED DATA IS NOT VALID");
      return;
    }

    // remove source and external id
    usrOrgData.remove(JsonKey.EXTERNAL_ID);
    usrOrgData.remove(JsonKey.SOURCE);

    String orgId = null;
    String userId = null;

    if (isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))) {
      orgId = (String) usrOrgData.get(JsonKey.ORGANISATION_ID);
    }

    if (isNotNull(usrOrgData.get(JsonKey.USER_ID))) {
      userId = (String) usrOrgData.get(JsonKey.USER_ID);
    }

    if (isNull(orgId) || isNull(userId)) {
      // creeate exception here invalid request data and tell the exception , ther return
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<String, Object>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), requestData);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (list.size() == 0) {
      // user already enrolled for the organisation
      ProjectLogger.log("User does not belong to org");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    Map<String, Object> userOrgDBO = list.get(0);

    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = getUserNamebyUserId(updatedBy);
      updateUserOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      updateUserOrgDBO.put(JsonKey.APPROVED_BY, updatedByName);
    }
    updateUserOrgDBO.put(JsonKey.ID, (String) userOrgDBO.get(JsonKey.ID));
    updateUserOrgDBO.put(JsonKey.APPROOVE_DATE, ProjectUtil.getFormattedDate());
    updateUserOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());

    updateUserOrgDBO.put(JsonKey.IS_APPROVED, false);
    updateUserOrgDBO.put(JsonKey.IS_REJECTED, true);

    response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), updateUserOrgDBO);
    sender().tell(response, self());
    return;

  }

  /**
   * Inserts an address if not present, else updates the existing address
   */
  private void upsertAddress(Map<String, Object> addressReq) {

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    cassandraOperation.upsertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), addressReq);
  }

  /**
   * Inserts the organization to organization relation is not exists, else Updates the organization
   * to organization relation
   */
  public void upsertOrgMap(String orgId, String parentOrgId, String rootOrgId, int env,
      String relation) {
    Util.DbInfo orgMapDbInfo = Util.dbInfoMap.get(JsonKey.ORG_MAP_DB);
    String orgMapId = ProjectUtil.getUniqueIdFromTimestamp(env);
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, orgMapId);
    orgMap.put(JsonKey.ORG_ID_ONE, parentOrgId);
    orgMap.put(JsonKey.RELATION, relation);
    orgMap.put(JsonKey.ORG_ID_TWO, orgId);
    orgMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
    cassandraOperation.insertRecord(orgMapDbInfo.getKeySpace(), orgMapDbInfo.getTableName(),
        orgMap);
  }

  /**
   * Checks whether the parent Organization exists
   */
  @SuppressWarnings("unchecked")
  public Boolean isValidParent(String parentId) {
    Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Response result = cassandraOperation.getRecordById(userdbInfo.getKeySpace(),
        userdbInfo.getTableName(), parentId);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      return true;
    }
    throw new ProjectCommonException(ResponseCode.invalidParentId.getErrorCode(),
        ResponseCode.invalidParentId.getErrorMessage(),
        ResponseCode.CLIENT_ERROR.getResponseCode());
  }

  /**
   * Checks whether parentId has a parent relation with the childId
   */
  @SuppressWarnings("unchecked")
  public Boolean isChildOf(String parentId, String childId) {
    Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.ORG_MAP_DB);
    Map<String, Object> properties = new HashMap<>();
    properties.put(JsonKey.ORG_ID_ONE, parentId);
    properties.put(JsonKey.ORG_ID_TWO, childId);
    Response result = cassandraOperation.getRecordsByProperties(userdbInfo.getKeySpace(),
        userdbInfo.getTableName(), properties);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      return true;
    }
    return false;
  }

  /**
   * validates if the organization and parent organization has the same root organization else
   * throws error
   */
  @SuppressWarnings("unchecked")
  public void validateRootOrg(Map<String, Object> request) throws ProjectCommonException {
    ProjectLogger.log("Validating Root org started---");
    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    if (!ProjectUtil.isStringNullOREmpty((String) request.get(JsonKey.PARENT_ORG_ID))) {
      Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), (String) request.get(JsonKey.PARENT_ORG_ID));
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      Map<String, Object> parentOrgDBO = new HashMap<>();
      if (!(list.isEmpty())) {
        parentOrgDBO = list.get(0);
      }
      if (!ProjectUtil.isStringNullOREmpty((String) parentOrgDBO.get(JsonKey.ROOT_ORG_ID))) {
        String parentRootOrg = (String) parentOrgDBO.get(JsonKey.ROOT_ORG_ID);
        if (null != request.get(JsonKey.ROOT_ORG_ID)
            && !parentRootOrg.equalsIgnoreCase(request.get(JsonKey.ROOT_ORG).toString())) {
          throw new ProjectCommonException(ResponseCode.invalidRootOrganisationId.getErrorCode(),
              ResponseCode.invalidRootOrganisationId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        } else {
          // set the parent root org to this organisation.
          request.put(JsonKey.ROOT_ORG_ID, parentRootOrg);
        }
      }
    }
    ProjectLogger.log("Validating Root org ended successfully---");
  }

  /**
   * validates whether the channelId is present for the parent organization
   */
  public boolean validateChannelIdForRootOrg(Map<String, Object> request) {
    ProjectLogger.log("Doing relation check");
    if (null != request.get(JsonKey.IS_ROOT_ORG) && (Boolean) request.get(JsonKey.IS_ROOT_ORG)) {
      if (ProjectUtil.isStringNullOREmpty((String) request.get(JsonKey.CHANNEL))) {
        throw new ProjectCommonException(ResponseCode.channelIdRequiredForRootOrg.getErrorCode(),
            ResponseCode.channelIdRequiredForRootOrg.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      } else if (null != request.get(JsonKey.CHANNEL)
          || !ProjectUtil.isStringNullOREmpty((String) request.get(JsonKey.CHANNEL))) {
        return true;
      }
    }
    ProjectLogger.log("Relation check end successfully!");
    return false;
  }

  /**
   * validates if the relation is cyclic
   */
  public void validateCyclicRelationForOrganisation(Map<String, Object> request) {
    if (!ProjectUtil.isStringNullOREmpty((String) request.get(JsonKey.PARENT_ORG_ID))) {
      if (isChildOf((String) request.get(JsonKey.ORGANISATION_ID),
          (String) request.get(JsonKey.PARENT_ORG_ID))) {
        throw new ProjectCommonException(ResponseCode.cyclicValidationError.getErrorCode(),
            ResponseCode.cyclicValidationError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
  }

  /**
   * This method will provide user name based on user id if user not found then it will return null.
   *
   * @param userId String
   * @return String
   */
  @SuppressWarnings("unchecked")
  private String getUserNamebyUserId(String userId) {
    Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Response result = cassandraOperation.getRecordById(userdbInfo.getKeySpace(),
        userdbInfo.getTableName(), userId);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      return (String) (list.get(0).get(JsonKey.USERNAME));
    }
    return null;
  }

  /**
   * Validates whether the organisation or source with externalId exists in DB
   * 
   * @param req Request from the user
   * @return boolean
   */
  @SuppressWarnings("unchecked")
  private boolean validateOrgRequest(Map<String, Object> req) {

    if (isNull(req)) {
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return false;
    }

    if (isNull(req.get(JsonKey.ORGANISATION_ID))) {

      if (isNull(req.get(JsonKey.PROVIDER)) || isNull(req.get(JsonKey.EXTERNAL_ID))) {
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return false;
      }

      // fetch orgid from database on basis of source and external id and put orgid into request .
      Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

      Map<String, Object> requestDbMap = new HashMap<String, Object>();
      requestDbMap.put(JsonKey.PROVIDER, req.get(JsonKey.PROVIDER));
      requestDbMap.put(JsonKey.EXTERNAL_ID, req.get(JsonKey.EXTERNAL_ID));

      Response result = cassandraOperation.getRecordsByProperties(userdbInfo.getKeySpace(),
          userdbInfo.getTableName(), requestDbMap);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);

      if (list.isEmpty()) {
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return false;
      }

      req.put(JsonKey.ORGANISATION_ID, list.get(0).get(JsonKey.ID));

    }
    return true;
  }

  /**
   * Validates whether the organisation or source with externalId exists in DB
   * 
   * @param req
   * @return boolean
   */
  @SuppressWarnings("unchecked")
  private boolean validateOrgRequestForMembers(Map<String, Object> req) {
    if (isNull(req)) {
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return false;
    }
    if (isNull(req.get(JsonKey.ORGANISATION_ID))) {
      if (isNull(req.get(JsonKey.PROVIDER)) || isNull(req.get(JsonKey.EXTERNAL_ID))) {
        ProjectCommonException exception = new ProjectCommonException(
            ResponseCode.sourceAndExternalIdValidationError.getErrorCode(),
            ResponseCode.sourceAndExternalIdValidationError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return false;
      }
    }
    // fetch orgid from database on basis of source and external id and put orgid into request .
    Util.DbInfo orgDBInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

    Map<String, Object> requestDbMap = new HashMap<>();
    if (!ProjectUtil.isStringNullOREmpty((String) req.get(JsonKey.ORGANISATION_ID))) {
      requestDbMap.put(JsonKey.ID, req.get(JsonKey.ORGANISATION_ID));
    } else {
      requestDbMap.put(JsonKey.PROVIDER, req.get(JsonKey.PROVIDER));
      requestDbMap.put(JsonKey.EXTERNAL_ID, req.get(JsonKey.EXTERNAL_ID));
    } 

    Response result = cassandraOperation.getRecordsByProperties(orgDBInfo.getKeySpace(),
        orgDBInfo.getTableName(), requestDbMap);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);

    if (list.isEmpty()) {
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidOrgData.getErrorCode(), ResponseCode.invalidOrgData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return false;
    }
    req.put(JsonKey.ORGANISATION_ID, list.get(0).get(JsonKey.ID));
    if (req.containsKey(JsonKey.PROVIDER) || req.containsKey(JsonKey.SOURCE)) {
      req.put(JsonKey.PROVIDER, req.get(JsonKey.PROVIDER));
    } else {
      req.put(JsonKey.PROVIDER, list.get(0).get(JsonKey.PROVIDER));
    }
    return true;
  }

  /**
   * Validates where the userId or provider with userName is in database and is valid
   * 
   * @param req
   * @return boolean
   */
  @SuppressWarnings("unchecked")
  private boolean validateUsrRequest(Map<String, Object> req) {
    if (isNull(req)) {
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return false;
    }
    Map<String, Object> data = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    data.putAll(req);
    if (isNull(req.get(JsonKey.USER_ID))) {
      if (isNull(data.get(JsonKey.USERNAME)) || isNull(data.get(JsonKey.PROVIDER))) {
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.usrValidationError.getErrorCode(),
                ResponseCode.usrValidationError.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return false;
      }
    }
    // fetch orgid from database on basis of source and external id and put orgid into request .
    Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);

    Map<String, Object> requestDbMap = new HashMap<>();
    if (!ProjectUtil.isStringNullOREmpty((String) req.get(JsonKey.USER_ID))) {
      requestDbMap.put(JsonKey.ID, req.get(JsonKey.USER_ID));
    } else {
      requestDbMap.put(JsonKey.PROVIDER, data.get(JsonKey.PROVIDER));
      requestDbMap.put(JsonKey.USERNAME, data.get(JsonKey.USERNAME));
    } 

    Response result = cassandraOperation.getRecordsByProperties(userdbInfo.getKeySpace(),
        userdbInfo.getTableName(), requestDbMap);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);

    if (list.isEmpty()) {
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidUsrData.getErrorCode(), ResponseCode.invalidUsrData.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return false;
    }
    req.put(JsonKey.USER_ID, list.get(0).get(JsonKey.ID));

    return true;
  }

  /**
   * validates if channel is already present in the organisation
   * 
   * @param channel
   * @return boolean
   */
  @SuppressWarnings("unchecked")
  private boolean validateChannelForUniqueness(String channel) {
    if (!ProjectUtil.isStringNullOREmpty(channel)) {
      Map<String, Object> filters = new HashMap<>();
      filters.put(JsonKey.CHANNEL, channel);
      filters.put(JsonKey.IS_ROOT_ORG, true);
      Map<String, Object> esResult = elasticSearchComplexSearch(filters,
          EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
      if (isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT)
          && isNotNull(esResult.get(JsonKey.CONTENT))
          && ((List) esResult.get(JsonKey.CONTENT)).size() > 0) {
        return false;
      }
    }
    return true;
  }


  private String getRootOrgIdFromChannel(String channel) {
    if (!ProjectUtil.isStringNullOREmpty(channel)) {
      Map<String, Object> filters = new HashMap<>();
      filters.put(JsonKey.CHANNEL, channel);
      filters.put(JsonKey.IS_ROOT_ORG, true);
      Map<String, Object> esResult = elasticSearchComplexSearch(filters,
          EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
      if (isNotNull(esResult) && esResult.containsKey(JsonKey.CONTENT)
          && isNotNull(esResult.get(JsonKey.CONTENT))
          && ((List) esResult.get(JsonKey.CONTENT)).size() > 0) {
        Map<String, Object> esContent =
            ((List<Map<String, Object>>) esResult.get(JsonKey.CONTENT)).get(0);
        return (String) esContent.getOrDefault(JsonKey.ID, "");
      }
    }
    return "";
  }


  private Map<String, Object> elasticSearchComplexSearch(Map<String, Object> filters, String index,
      String type) {

    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);

    return ElasticSearchUtil.complexSearch(searchDTO, index, type);

  }


  /**
   * validates if channel is already present in the organisation while Updating
   * 
   * @param channel
   * @return boolean
   */
  @SuppressWarnings("unchecked")
  private boolean validateChannelForUniquenessForUpdate(String channel, String orgId) {
    if (!ProjectUtil.isStringNullOREmpty(channel)) {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
      Response result = cassandraOperation.getRecordsByProperty(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), JsonKey.CHANNEL, channel);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if ((list.isEmpty())) {
        return true;
      } else {
        Map<String, Object> data = list.get(0);
        String id = (String) data.get(JsonKey.ID);
        if (id.equalsIgnoreCase(orgId)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * @param actorMessage
   */
  private Map<String, Object> getOrgsData(Request actorMessage) {
    Map<String, Object> requestMap = actorMessage.getRequest();
    SearchDTO dto = new SearchDTO();
    Map<String, Object> map = new HashMap<String, Object>();
    // TODO need to check with request body what data we will get
    // map.put(JsonKey.REGISTERED_ORG_ID, "some value");
    // map.put(JsonKey.ROOT_ORG_ID, "");
    Map<String, Object> additionalProperty = new HashMap<>();
    additionalProperty.put(JsonKey.FILTERS, map);
    dto.setAdditionalProperties(additionalProperty);
    Map<String, Object> responseMap = ElasticSearchUtil.complexSearch(dto,
        ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.organisation.getTypeName());
    if (requestMap != null) {
      return responseMap;
    }
    return null;

  }

  /**
   * 
   * @param externalId
   * @param provider
   */
  private void validateExternalIdAndProvider(String externalId, String provider) {
    if (ProjectUtil.isStringNullOREmpty(externalId) || ProjectUtil.isStringNullOREmpty(provider)) {
      ProjectLogger.log("Source and external ids should exist.");
      throw new ProjectCommonException(
          ResponseCode.sourceAndExternalIdValidationError.getErrorCode(),
          ResponseCode.sourceAndExternalIdValidationError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

  }

  /**
   * This method will do the channel uniqueness validation
   * 
   * @param req
   */
  private void validateChannel(Map<String, Object> req) {
    if (!req.containsKey(JsonKey.IS_ROOT_ORG) || !(Boolean) req.get(JsonKey.IS_ROOT_ORG)) {
      String rootOrgId = getRootOrgIdFromChannel((String) req.get(JsonKey.CHANNEL));
      if (!ProjectUtil.isStringNullOREmpty(rootOrgId)) {
        req.put(JsonKey.ROOT_ORG_ID, rootOrgId);
      } else {
        ProjectLogger.log("Invalid channel id.");
        throw new ProjectCommonException(ResponseCode.invalidChannel.getErrorCode(),
            ResponseCode.invalidChannel.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else if (!validateChannelForUniqueness((String) req.get(JsonKey.CHANNEL))) {
      ProjectLogger.log("Channel validation failed");
      throw new ProjectCommonException(ResponseCode.channelUniquenessInvalid.getErrorCode(),
          ResponseCode.channelUniquenessInvalid.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

  }


}
