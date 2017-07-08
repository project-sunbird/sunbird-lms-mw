package org.sunbird.learner.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;

import akka.actor.UntypedAbstractActor;

/**
 * This actor will handle organisation related operation .
 *
 * @author Amit Kumar
 */
public class OrganisationManagementActor extends UntypedAbstractActor {
  private LogHelper logger = LogHelper.getInstance(OrganisationManagementActor.class.getName());

  private CassandraOperation cassandraOperation = new CassandraOperationImpl();
  private static final String PARENT_ORG_ID = "parentOrgId";
  private static final String IS_ROOT_ORG = "isRootOrg";
  private static final String CHANNEL = "channel";
  private static final String ORG_ID_ONE = "orgIdOne";
  private static final String RELATION = "relation";
  private static final String ORG_ID_TWO = "orgIdTwo";
  private static final String ROOT_ORG_ID = "rootOrgId";
  private static final String PARENT_OF = "parentOf";
  private static final String ORG_TYPE = "orgType";
  private static final String CHILD_OF = "childOf";
  private static final String ADDRESS_ID = "addId";
  private static final String ROOT_ORG = "rootOrg";
  private static final String APPROVED_BY = "approvedBy";
  private static final String APPROVED_DATE = "approvedDate";
  private static final String APPROVED_BY_NAME = "approvedByName";

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      logger.info("OrganisationManagementActor  onReceive called");
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
      } else {
        logger.info("UNSUPPORTED OPERATION");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                ResponseCode.invalidOperationName.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
      }
    } else {
      // Throw exception as message body
      logger.info("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  /**
   * * Method to create the organisation .
   * 
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void createOrg(Request actorMessage) {
    try {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
      String parentOrg = (String) req.get(PARENT_ORG_ID);
      Boolean isValidParent = false;
      if (!ProjectUtil.isStringNullOREmpty(parentOrg) && isValidParent(parentOrg)) {
        validateRootOrg(req);
        validateChannelIdForRootOrg(req);
        isValidParent = true;
      }
      Map<String, Object> addressReq = null;
      if (null != actorMessage.getRequest().get(JsonKey.ADDRESS)) {
        addressReq = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ADDRESS);
      }
      String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        String updatedByName = getUserNamebyUserId(updatedBy);
        // req.put(JsonKey.CREATED_BY_NAME, updatedByName);
        req.put(JsonKey.CREATED_BY, updatedBy);
      }
      String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
      req.put(JsonKey.ID, uniqueId);
      req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());

      // update address if present in request
      if (null != addressReq && addressReq.size() > 0) {
        String addressId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        addressReq.put(JsonKey.ID, addressId);
        addressReq.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());

        if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
          String updatedByName = getUserNamebyUserId(updatedBy);
          // addressReq.put(JsonKey.CREATED_BY_NAME, updatedByName);
          addressReq.put(JsonKey.CREATED_BY, updatedBy);
        }
        upsertAddress(addressReq);
        req.put(ADDRESS_ID, addressId);
      }

      Response result =
          cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), req);

      // create org_map if parentOrgId is present in request
      if (isValidParent) {
        upsertOrgMap(uniqueId, parentOrg, (String) req.get(ROOT_ORG_ID), actorMessage.getEnv());
      }
      // create record in org_type if present in request
      String orgType = (String) req.get(ORG_TYPE);
      if (!ProjectUtil.isStringNullOREmpty(orgType)) {
        upsertOrgType(orgType, actorMessage.getEnv());
      }

      result.getResult().put(JsonKey.ORGANISATION_ID, uniqueId);
      sender().tell(result, self());
      return;
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }

  }

  /**
   * Method to approve the organisation only if the org in inactive state
   * 
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void approveOrg(Request actorMessage) {
    try {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);

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
        logger.info("Invalid Org Id");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      boolean isApprove = (boolean) req.get(JsonKey.IS_APPROVED);

      if (isApprove) {

        String orgStatus = (String) orgDBO.get(JsonKey.STATUS);
        if ((ProjectUtil.OrgStatus.INACTIVE.name().equalsIgnoreCase(orgStatus))) {
          logger.info("Can not approve org");
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                  ResponseCode.invalidRequestData.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
          return;
        }

        updateOrgDBO.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
      }
      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        String updatedByName = getUserNamebyUserId(updatedBy);
        // updateOrgDBO.put(JsonKey.UPDATED_BY_NAME,updatedByName);
        updateOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
        // updateOrgDBO.put(APPROVED_BY_NAME, updatedByName);
        updateOrgDBO.put(APPROVED_BY, updatedBy);
      }
      updateOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      updateOrgDBO.put(JsonKey.ID, (String) orgDBO.get(JsonKey.ID));
      updateOrgDBO.put(JsonKey.IS_APPROVED, req.get(JsonKey.IS_APPROVED));
      updateOrgDBO.put(APPROVED_DATE, ProjectUtil.getFormattedDate());

      Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), updateOrgDBO);
      response.getResult().put(JsonKey.ORGANISATION_ID, orgDBO.get(JsonKey.ID));
      sender().tell(response, self());
      return;
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }
  }

  /**
   * Updates the status of the organization
   * 
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void updateOrgStatus(Request actorMessage) {
    try {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
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
        logger.info("Invalid Org Id");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      String currentStatus = (String) orgDBO.get(JsonKey.STATUS);
      String nextStatus = (String) req.get(JsonKey.STATUS);
      if (!(Util.checkOrgStatusTransition(currentStatus, nextStatus))) {

        logger.info("Invalid Org State transation");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        String updatedByName = getUserNamebyUserId(updatedBy);
        // updateOrgDBO.put(JsonKey.UPDATED_BY_NAME, updatedByName);
        updateOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      }
      updateOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      updateOrgDBO.put(JsonKey.ID, (String) orgDBO.get(JsonKey.ID));
      updateOrgDBO.put(JsonKey.STATUS, nextStatus);

      Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), updateOrgDBO);
      response.getResult().put(JsonKey.ORGANISATION_ID, orgDBO.get(JsonKey.ID));
      sender().tell(response, self());
      return;
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }
  }

  /**
   * Update the organization data
   * 
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void updateOrgData(Request actorMessage) throws ProjectCommonException {
    try {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
      String parentOrg = (String) req.get(PARENT_ORG_ID);
      Boolean isValidParent = false;
      logger.info(parentOrg);
      if (!ProjectUtil.isStringNullOREmpty(parentOrg) && isValidParent(parentOrg)) {
        validateCyclicRelationForOrganisation(req);
        validateRootOrg(req);
        validateChannelIdForRootOrg(req);
        isValidParent = true;
      }
      Map<String, Object> addressReq = null;
      if (null != actorMessage.getRequest().get(JsonKey.ADDRESS)) {
        addressReq = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ADDRESS);
      }
      Map<String, Object> orgDBO;
      Map<String, Object> updateOrgDBO = new HashMap<String, Object>();
      updateOrgDBO.putAll(req);
      updateOrgDBO.remove(JsonKey.ORGANISATION_ID);
      updateOrgDBO.remove(JsonKey.IS_APPROVED);
      updateOrgDBO.remove(APPROVED_BY);
      // updateOrgDBO.remove(APPROVED_BY_NAME);
      updateOrgDBO.remove(APPROVED_DATE);
      updateOrgDBO.remove(JsonKey.STATUS);
      String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

      String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
      Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), orgId);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        orgDBO = (Map<String, Object>) list.get(0);
      } else {
        logger.info("Invalid Org Id");
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
          String updatedByName = getUserNamebyUserId(updatedBy);
          // addressReq.put(JsonKey.UPDATED_BY_NAME, updatedByName);
          addressReq.put(JsonKey.UPDATED_BY, updatedBy);
          upsertAddress(addressReq);
        }
      }

      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        String updatedByName = getUserNamebyUserId(updatedBy);
        // updateOrgDBO.put(JsonKey.UPDATED_BY_NAME, updatedByName);
        updateOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      }
      updateOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      updateOrgDBO.put(JsonKey.ID, (String) orgDBO.get(JsonKey.ID));

      if (isValidParent) {
        upsertOrgMap((String) orgDBO.get(JsonKey.ID), parentOrg, (String) req.get(ROOT_ORG_ID),
            actorMessage.getEnv());
      }

      // create record in org_type if present in request
      String orgType = (String) req.get(ORG_TYPE);
      if (!ProjectUtil.isStringNullOREmpty(orgType)) {
        upsertOrgType(orgType, actorMessage.getEnv());
      }

      Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), updateOrgDBO);
      response.getResult().put(JsonKey.ORGANISATION_ID, orgDBO.get(JsonKey.ID));
      sender().tell(response, self());
      return;
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }

  }

  /**
   * Provides the details of the organization
   * 
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void getOrgDetails(Request actorMessage) {
    try {
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
      Map<String, Object> orgDBO;
      String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
      Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), orgId);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {
        orgDBO = (Map<String, Object>) list.get(0);
      } else {
        logger.info("Invalid Org Id");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return;
      }

      if (orgDBO.get(JsonKey.ADDRESS_ID.toLowerCase()) != null) {
        String addressId = (String) orgDBO.get(JsonKey.ADDRESS_ID.toLowerCase());
        Map<String, Object> address = getAddress(addressId);
        orgDBO.put(JsonKey.ADDRESS, address);
      }
      orgDBO.put(JsonKey.RELATIONS, getRelations(orgId));
      Response response = new Response();
      response.getResult().putAll(orgDBO);
      sender().tell(response, self());
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }
  }

  /**
   * Get the details of the Organization
   * 
   * @param actorMessage
   */
  @SuppressWarnings({"unchecked", "unused"})
  private void getOrgData(Request actorMessage) {

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

    Map<String, Object> req =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
    Map<String, Object> orgDBO;
    String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
    Response result =
        cassandraOperation.getRecordById(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgId);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      orgDBO = (Map<String, Object>) list.get(0);
    } else {
      logger.info("Invalid Org Id");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    if (orgDBO.get(JsonKey.ADDRESS_ID) != null) {
      String addressId = (String) orgDBO.get(JsonKey.ADDRESS_ID);
      Map<String, Object> address = getAddress(addressId);
      orgDBO.put(JsonKey.ADDRESS, address);
    }
    sender().tell(result, self());

  }

  /**
   * Gets the address
   * 
   * @param addressId
   * @return
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> getAddress(String addressId) {

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
        orgDbInfo.getTableName(), addressId);
    Map<String, Object> addressDBO = new HashMap<String, Object>();

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      addressDBO = (Map<String, Object>) list.get(0);
    }
    return addressDBO;
  }

  /**
   * Gets the relations of an organization
   * 
   * @param orgId
   * @return
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getRelations(String orgId) {

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_MAP_DB);
    List<Map<String, Object>> finalResult = new ArrayList<>();
    Response result = cassandraOperation.getRecordsByProperty(orgDbInfo.getKeySpace(),
        orgDbInfo.getTableName(), ORG_ID_ONE, orgId);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      for (Object data : list) {
        Map<String, Object> relationDBO = new HashMap<String, Object>();
        Map<String, Object> relation = (Map<String, Object>) data;
        relationDBO.put(JsonKey.ID, relation.get(ORG_ID_TWO.toLowerCase()));
        relationDBO.put(RELATION, PARENT_OF);
        relationDBO.putAll(
            getRelationOrgObject(relationDBO, (String) relation.get(ORG_ID_TWO.toLowerCase())));
        finalResult.add(relationDBO);
      }
    }
    Response resultParent = cassandraOperation.getRecordsByProperty(orgDbInfo.getKeySpace(),
        orgDbInfo.getTableName(), ORG_ID_TWO, orgId);
    List<Map<String, Object>> listParent =
        (List<Map<String, Object>>) resultParent.get(JsonKey.RESPONSE);
    if (!(listParent.isEmpty())) {
      Map<String, Object> relationDBO = new HashMap<String, Object>();
      Map<String, Object> relation = (Map<String, Object>) listParent.get(0);
      relationDBO.put(JsonKey.ID, relation.get(ORG_ID_ONE.toLowerCase()));
      relationDBO.put(RELATION, CHILD_OF);
      relationDBO.putAll(
          getRelationOrgObject(relationDBO, (String) relation.get(ORG_ID_ONE.toLowerCase())));
      finalResult.add(relationDBO);
    }
    return finalResult;
  }

  /**
   * Gets the organization object for a particular relation
   * 
   * @param relationDBO
   * @param orgIdParam
   * @return
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> getRelationOrgObject(Map<String, Object> relationDBO,
      String orgIdParam) {
    if (null == orgIdParam) {
      return new HashMap<>();
    }
    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Response resultChild = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
        orgDbInfo.getTableName(), orgIdParam);

    List<Map<String, Object>> listChild =
        (List<Map<String, Object>>) resultChild.get(JsonKey.RESPONSE);
    if (!(listChild.isEmpty())) {
      if (null != listChild.get(0)) {
        relationDBO.put(JsonKey.ORG_NAME, listChild.get(0).get(JsonKey.ORG_NAME));
        relationDBO.put(JsonKey.DESCRIPTION, listChild.get(0).get(JsonKey.DESCRIPTION));
      }
    }
    return relationDBO;
  }

  /**
   * Inserts an address if not present, else updates the existing address
   * 
   * @param addressReq
   */
  private void upsertAddress(Map<String, Object> addressReq) {

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    cassandraOperation.upsertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), addressReq);
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

  @SuppressWarnings({"unused", "unchecked"})
  private void removeUnwantedProperties(Response response, List<String> removableAttributes) {
    List<Map<String, Object>> list =
        (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    for (Map<String, Object> map : list) {
      for (String attr : removableAttributes) {
        map.remove(attr);
      }
    }
  }

  /**
   * Inserts the organization to organization relation is not exists, else Updates the organization
   * to organization relation
   * 
   * @param orgId
   * @param parentOrgId
   * @param rootOrgId
   * @param env
   */
  public void upsertOrgMap(String orgId, String parentOrgId, String rootOrgId, int env) {
    Util.DbInfo orgMapDbInfo = Util.dbInfoMap.get(JsonKey.ORG_MAP_DB);
    String orgMapId = ProjectUtil.getUniqueIdFromTimestamp(env);
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, orgMapId);
    orgMap.put(ORG_ID_ONE, parentOrgId);
    orgMap.put(RELATION, PARENT_OF);
    orgMap.put(ORG_ID_TWO, orgId);
    orgMap.put(ROOT_ORG_ID, rootOrgId);
    cassandraOperation.insertRecord(orgMapDbInfo.getKeySpace(), orgMapDbInfo.getTableName(),
        orgMap);
  }

  /**
   * Inserts orgType if not exists, else updates the orgType
   * 
   * @param orgType
   * @param env
   */
  public void upsertOrgType(String orgType, int env) {
    Util.DbInfo orgMapDbInfo = Util.dbInfoMap.get(JsonKey.ORG_TYPE_DB);
    String orgMapId = ProjectUtil.getUniqueIdFromTimestamp(env);
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, orgMapId);
    orgMap.put(JsonKey.NAME, orgType);
    cassandraOperation.insertRecord(orgMapDbInfo.getKeySpace(), orgMapDbInfo.getTableName(),
        orgMap);
  }

  /**
   * Checks whether the parent Organization exists
   * 
   * @param parentId
   * @return
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
   * 
   * @param parentId
   * @param childId
   * @return
   */
  @SuppressWarnings("unchecked")
  public Boolean isChildOf(String parentId, String childId) {
    Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.ORG_MAP_DB);
    Map<String, Object> properties = new HashMap<>();
    properties.put(ORG_ID_ONE, parentId);
    properties.put(ORG_ID_TWO, childId);
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
   * 
   * @param request
   */
  @SuppressWarnings("unchecked")
  public void validateRootOrg(Map<String, Object> request) throws ProjectCommonException {
    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    if (!ProjectUtil.isStringNullOREmpty((String) request.get(PARENT_ORG_ID))) {
      Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), (String) request.get(PARENT_ORG_ID));
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      Map<String, Object> parentOrgDBO = new HashMap<>();
      if (!(list.isEmpty())) {
        parentOrgDBO = list.get(0);
      }
      if (!ProjectUtil.isStringNullOREmpty((String) parentOrgDBO.get(ROOT_ORG))) {
        String parentRootOrg = parentOrgDBO.get(ROOT_ORG).toString();
        if (null != request.get(ROOT_ORG)) {
          if (null != request.get(ROOT_ORG)
              && !parentRootOrg.equalsIgnoreCase(request.get(ROOT_ORG).toString())) {
            throw new ProjectCommonException(ResponseCode.invalidRootOrganisationId.getErrorCode(),
                ResponseCode.invalidRootOrganisationId.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
          }
        }
      }
    }
  }

  /**
   * validates whether the channelId is present for the parent organization
   * 
   * @param request
   */
  public void validateChannelIdForRootOrg(Map<String, Object> request)
      throws ProjectCommonException {
    if (null != request.get(IS_ROOT_ORG) && (Boolean) request.get(IS_ROOT_ORG)) {
      if (null == request.get(CHANNEL)) {
        throw new ProjectCommonException(ResponseCode.channelIdRequiredForRootOrg.getErrorCode(),
            ResponseCode.channelIdRequiredForRootOrg.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
  }

  /**
   * validates if the relation is cyclic
   * 
   * @param request
   */
  public void validateCyclicRelationForOrganisation(Map<String, Object> request)
      throws ProjectCommonException {
    if (!ProjectUtil.isStringNullOREmpty((String) request.get(PARENT_ORG_ID))) {
      if (isChildOf((String) request.get(JsonKey.ORGANISATION_ID), (String) request.get(PARENT_ORG_ID))) {
        throw new ProjectCommonException(ResponseCode.cyclicValidationError.getErrorCode(),
            ResponseCode.cyclicValidationError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
  }

}