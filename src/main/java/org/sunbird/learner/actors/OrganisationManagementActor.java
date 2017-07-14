package org.sunbird.learner.actors;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import akka.actor.UntypedAbstractActor;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import scala.concurrent.duration.Duration;

/**
 * This actor will handle organisation related operation .
 *
 * @author Amit Kumar
 */
public class OrganisationManagementActor extends UntypedAbstractActor {
  private LogHelper logger = LogHelper.getInstance(OrganisationManagementActor.class.getName());

  private CassandraOperation cassandraOperation = new CassandraOperationImpl();

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        logger.info("OrganisationManagementActor  onReceive called");
        ProjectLogger.log("OrganisationManagementActor  onReceive called");
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
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.JOIN_USER_ORGANISATION.getValue())) {
          joinUserOrganisation(actorMessage);
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.APPROVE_USER_ORGANISATION.getValue())) {
          approveUserOrg(actorMessage);
        } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.REJECT_USER_ORGANISATION.getValue())) {
          rejectUserOrg(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.ADD_MEMBER_ORGANISATION.getValue())) {
          addMemberOrganisation(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue())) {
          removeMemberOrganisation(actorMessage);
        } else {
          logger.info("UNSUPPORTED OPERATION");
          ProjectLogger.log("UNSUPPORTED OPERATION");
          ProjectCommonException exception =
                  new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                          ResponseCode.invalidOperationName.getErrorMessage(),
                          ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        logger.error(ex);
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    } else {
      // Throw exception as message body
      logger.info("UNSUPPORTED MESSAGE");
      ProjectLogger.log("UNSUPPORTED MESSAGE");
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
      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
      Map<String, Object> addressReq = null;
      if (null != actorMessage.getRequest().get(JsonKey.ADDRESS)) {
        addressReq = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ADDRESS);
      }
      String relation = (String) req.get(JsonKey.RELATION);
      req.remove(JsonKey.RELATION);
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
      String parentOrg = (String) req.get(JsonKey.PARENT_ORG_ID);
      Boolean isValidParent = false;
      if (!ProjectUtil.isStringNullOREmpty(parentOrg) && isValidParent(parentOrg)) {
        validateRootOrg(req);
        validateChannelIdForRootOrg(req);
        isValidParent = true;
      }
      String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        req.put(JsonKey.CREATED_BY, updatedBy);
      }
      String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
      req.put(JsonKey.ID, uniqueId);
      req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      req.put(JsonKey.STATUS , ProjectUtil.OrgStatus.ACTIVE.name());

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

      Response result =
          cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), req);

      // create org_map if parentOrgId is present in request
      if (isValidParent) {
        upsertOrgMap(uniqueId, parentOrg, (String) req.get(JsonKey.ROOT_ORG_ID), actorMessage.getEnv(),relation);
      }
      // create record in org_type if present in request
      String orgType = (String) req.get(JsonKey.ORG_TYPE);
      if (!ProjectUtil.isStringNullOREmpty(orgType)) {
        upsertOrgType(orgType, actorMessage.getEnv());
      }

      result.getResult().put(JsonKey.ORGANISATION_ID, uniqueId);
      sender().tell(result, self());


      Response orgResponse =  new Response();
      Timeout timeout = new Timeout(Duration.create(ProjectUtil.BACKGROUND_ACTOR_WAIT_TIME, TimeUnit.SECONDS));
      if(null != addressReq){
        req.put(JsonKey.ADDRESS, addressReq);
      }
      orgResponse.put(JsonKey.ORGANISATION, req);
      orgResponse.put(JsonKey.OPERATION, ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
      Patterns.ask(RequestRouterActor.backgroundJobManager, orgResponse, timeout);
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
      if(!(validateOrgRequest(req))){
        logger.info("REQUESTED DATA IS NOT VALID");
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
        logger.info("Invalid Org Id");
        ProjectLogger.log("Invalid Org Id");
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
          ProjectLogger.log("Can not approve org");
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
        updateOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
        updateOrgDBO.put(JsonKey.APPROVED_BY, updatedBy);
      }
      updateOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      updateOrgDBO.put(JsonKey.ID, (String) orgDBO.get(JsonKey.ID));
      updateOrgDBO.put(JsonKey.IS_APPROVED, req.get(JsonKey.IS_APPROVED));
      updateOrgDBO.put(JsonKey.APPROVED_DATE, ProjectUtil.getFormattedDate());

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

      if(!(validateOrgRequest(req))){
        logger.info("REQUESTED DATA IS NOT VALID");
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
        logger.info("Invalid Org Id");
        ProjectLogger.log("Invalid Org Id");
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
      Map<String, Object> req =
          (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
      if(!(validateOrgRequest(req))){
        logger.info("REQUESTED DATA IS NOT VALID");
        return;
      }
      Map<String, Object> addressReq = null;
      if (null != actorMessage.getRequest().get(JsonKey.ADDRESS)) {
        addressReq = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ADDRESS);
      }
      String relation = (String) req.get(JsonKey.RELATION);
      req.remove(JsonKey.RELATION);
      Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
      String parentOrg = (String) req.get(JsonKey.PARENT_ORG_ID);
      Boolean isValidParent = false;
      logger.info(parentOrg);
      ProjectLogger.log("Parent Org Id:"+ parentOrg);
      if (!ProjectUtil.isStringNullOREmpty(parentOrg) && isValidParent(parentOrg)) {
        validateCyclicRelationForOrganisation(req);
        validateRootOrg(req);
        validateChannelIdForRootOrg(req);
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
        orgDBO = (Map<String, Object>) list.get(0);
      } else {
        logger.info("Invalid Org Id");
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

      if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
        updateOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
      }
      updateOrgDBO.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      updateOrgDBO.put(JsonKey.ID, (String) orgDBO.get(JsonKey.ID));

      if (isValidParent) {
        upsertOrgMap((String) orgDBO.get(JsonKey.ID), parentOrg, (String) req.get(JsonKey.ROOT_ORG_ID),
            actorMessage.getEnv(),relation);
      }

      // create record in org_type if present in request
      String orgType = (String) req.get(JsonKey.ORG_TYPE);
      if (!ProjectUtil.isStringNullOREmpty(orgType)) {
        upsertOrgType(orgType, actorMessage.getEnv());
      }

      Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), updateOrgDBO);
      response.getResult().put(JsonKey.ORGANISATION_ID, orgDBO.get(JsonKey.ID));
      sender().tell(response, self());
      
      Response orgResponse =  new Response();
      Timeout timeout = new Timeout(Duration.create(ProjectUtil.BACKGROUND_ACTOR_WAIT_TIME, TimeUnit.SECONDS));
      if(null != addressReq){
        updateOrgDBO.put(JsonKey.ADDRESS, addressReq);
      }
      orgResponse.put(JsonKey.ORGANISATION, updateOrgDBO);
      orgResponse.put(JsonKey.OPERATION, ActorOperations.UPDATE_ORG_INFO_ELASTIC.getValue());
      Patterns.ask(RequestRouterActor.backgroundJobManager, orgResponse, timeout);
    } catch (ProjectCommonException e) {
      sender().tell(e, self());
      return;
    }
  }

  /**
   * Method to add member to the organisation
   *
   * @param actorMessage
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void addMemberOrganisation(Request actorMessage) {

    Response response = new Response();

    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Util.DbInfo organisationDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);

    Map<String, Object> req = actorMessage.getRequest();

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if(!(validateOrgRequest(usrOrgData))){
      logger.info("REQUESTED DATA IS NOT VALID");
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

    //remove source and external id
    usrOrgData.remove(JsonKey.EXTERNAL_ID);
    usrOrgData.remove(JsonKey.SOURCE);


    String updatedBy = null;
    String orgId = null;
    String userId = null;
    List<String> roles = null;
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
      roles = (List<String>) usrOrgData.get(JsonKey.ROLES);
    }

    if ((isNull(orgId) || isNull(userId)) || isNull(roles)) {
      // create exception here invalid request data and tell the exception , ther return
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check org exist or not
    Response orgResult = cassandraOperation.getRecordById(organisationDbInfo.getKeySpace(),
        organisationDbInfo.getTableName(), orgId);

    List orgList = (List) orgResult.get(JsonKey.RESPONSE);
    if (orgList.size() == 0) {
      // user already enrolled for the organisation
      logger.info("Org does not exist");
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
    if (userList.size() == 0) {
      logger.info("User does not exist");
      ProjectLogger.log("User does not exist");
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidUserId.getErrorCode(), ResponseCode.invalidUserId.getErrorMessage(),
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
      response.getResult().put(JsonKey.RESPONSE, "User already a member of the organisation");
      sender().tell(response, self());
      return;
    }



    String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    usrOrgData.put(JsonKey.ID, id);
    if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
      String updatedByName = Util.getUserNamebyUserId(updatedBy);
      usrOrgData.put(JsonKey.ADDED_BY, updatedBy);
      usrOrgData.put(JsonKey.APPROVED_BY, updatedBy);
      if(!ProjectUtil.isStringNullOREmpty(updatedByName)){
         usrOrgData.put(JsonKey.ADDED_BY_NAME, Util.getUserNamebyUserId(updatedBy));
      }
    }
    usrOrgData.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
    usrOrgData.put(JsonKey.APPROOVE_DATE, ProjectUtil.getFormattedDate());
    usrOrgData.put(JsonKey.IS_REJECTED, false);
    usrOrgData.put(JsonKey.IS_APPROVED, true);

    response = cassandraOperation.insertRecord(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), usrOrgData);
    sender().tell(response, self());
    return;

  }

  /**
   * Method to remove member from the organisation
   *
   * @param actorMessage
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void removeMemberOrganisation(Request actorMessage) {

    Response response = new Response();

    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

    Map<String, Object> req = actorMessage.getRequest();

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if(!(validateOrgRequest(usrOrgData))){
      logger.info("REQUESTED DATA IS NOT VALID");
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

    //remove source and external id
    usrOrgData.remove(JsonKey.EXTERNAL_ID);
    usrOrgData.remove(JsonKey.SOURCE);

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
    Map<String, Object> requestData = new HashMap<String, Object>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(),
        userOrgDbInfo.getTableName(), requestData);

    List list = (List) result.get(JsonKey.RESPONSE);
    if (list.size() < 0) {
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
        sender().tell(response, self());
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

    Map<String, Object> req =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ORGANISATION);
    if(!(validateOrgRequest(req))){
      logger.info("REQUESTED DATA IS NOT VALID");
      return;
    }
    Map<String, Object> orgDBO;
    String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
    Map<String, Object> result = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.organisation.getTypeName(), orgId);
    Response response = new Response();
    if(result !=null) {
    response.put(JsonKey.RESPONSE, result);
    } else {
         result = new HashMap<String, Object>();
         response.put(JsonKey.RESPONSE, result);    
    }
    sender().tell(response, self());
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
    if(!(validateOrgRequest(req))){
      logger.info("REQUESTED DATA IS NOT VALID");
      return;
    }
    Map<String, Object> orgDBO;
    String orgId = (String) req.get(JsonKey.ORGANISATION_ID);
    Response result =
        cassandraOperation.getRecordById(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgId);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      orgDBO = (Map<String, Object>) list.get(0);
    } else {
      logger.info("Invalid Org Id");
      ProjectLogger.log("Invalid Org Id");
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
   * Method to join the user with organisation ...
   *
   * @param actorMessage
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void joinUserOrganisation(Request actorMessage) {

     Response response = new Response();

    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Util.DbInfo organisationDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

    Map<String, Object> req = actorMessage.getRequest();

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if(!(validateOrgRequest(usrOrgData))){
      logger.info("REQUESTED DATA IS NOT VALID");
      return;
    }
    if (isNull(usrOrgData)) {
      //create exception here and sender.tell the exception and return
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    //remove source and external id
    usrOrgData.remove(JsonKey.EXTERNAL_ID);
    usrOrgData.remove(JsonKey.SOURCE);

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
      //create exception here invalid request data and tell the exception , ther return
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    //check org exist or not
    Response orgResult = cassandraOperation.getRecordById(organisationDbInfo.getKeySpace(), organisationDbInfo.getTableName(), orgId);

    List orgList = (List) orgResult.get(JsonKey.RESPONSE);
    if (orgList.size() == 0) {
      // user already enrolled for the organisation
      logger.info("Org does not exist");
      ProjectLogger.log("Org does not exist");
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<String, Object>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), requestData);

    List list = (List) result.get(JsonKey.RESPONSE);
    if (list.size() > 0) {
      // user already enrolled for the organisation
      response = new Response();
      response.getResult().put(JsonKey.RESPONSE, "User already joined the organisation");
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

    response = cassandraOperation.insertRecord(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), usrOrgData);
    sender().tell(response, self());
    return;

  }

  /**
   * Method to approve the user organisation .
   *
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void approveUserOrg(Request actorMessage) {

    Response response = new Response();
    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

    Map<String, Object> updateUserOrgDBO = new HashMap<String, Object>();
    Map<String, Object> req = actorMessage.getRequest();
    String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if(!(validateOrgRequest(usrOrgData))){
      logger.info("REQUESTED DATA IS NOT VALID");
      return;
    }
    if (isNull(usrOrgData)) {
      //create exception here and sender.tell the exception and return
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    //remove source and external id
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
      //creeate exception here invalid request data and tell the exception , ther return
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<String, Object>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), requestData);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (list.size() == 0) {
      // user already enrolled for the organisation
      logger.info("User does not belong to org");
      ProjectLogger.log("User does not belong to org");
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
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
    updateUserOrgDBO.put(JsonKey.ROLES, roles);


    response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), updateUserOrgDBO);
    sender().tell(response, self());
    return;

  }

  /**
   * Method to reject the user organisation .
   *
   * @param actorMessage
   */
  private void rejectUserOrg(Request actorMessage) {

    Response response = new Response();
    Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

    Map<String, Object> updateUserOrgDBO = new HashMap<String, Object>();
    Map<String, Object> req = actorMessage.getRequest();
    String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

    Map<String, Object> usrOrgData = (Map<String, Object>) req.get(JsonKey.USER_ORG);
    if(!(validateOrgRequest(usrOrgData))){
      logger.info("REQUESTED DATA IS NOT VALID");
      return;
    }
    if (isNull(usrOrgData)) {
      //create exception here and sender.tell the exception and return
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    //remove source and external id
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
      //creeate exception here invalid request data and tell the exception , ther return
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }

    // check user already exist for the org or not
    Map<String, Object> requestData = new HashMap<String, Object>();
    requestData.put(JsonKey.USER_ID, userId);
    requestData.put(JsonKey.ORGANISATION_ID, orgId);

    Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), requestData);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (list.size() == 0) {
      // user already enrolled for the organisation
      logger.info("User does not belong to org");
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
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

    response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName(), updateUserOrgDBO);
    sender().tell(response, self());
    return;

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
        orgDbInfo.getTableName(), JsonKey.ORG_ID_ONE, orgId);

    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      for (Object data : list) {
        Map<String, Object> relationDBO = new HashMap<String, Object>();
        Map<String, Object> relation = (Map<String, Object>) data;
        relationDBO.put(JsonKey.ID, relation.get(JsonKey.ORG_ID_TWO.toLowerCase()));
        relationDBO.put(JsonKey.RELATION, JsonKey.PARENT_OF);
        relationDBO.putAll(
            getRelationOrgObject(relationDBO, (String) relation.get(JsonKey.ORG_ID_TWO.toLowerCase())));
        finalResult.add(relationDBO);
      }
    }
    Response resultParent = cassandraOperation.getRecordsByProperty(orgDbInfo.getKeySpace(),
        orgDbInfo.getTableName(), JsonKey.ORG_ID_TWO, orgId);
    List<Map<String, Object>> listParent =
        (List<Map<String, Object>>) resultParent.get(JsonKey.RESPONSE);
    if (!(listParent.isEmpty())) {
      Map<String, Object> relationDBO = new HashMap<String, Object>();
      Map<String, Object> relation = (Map<String, Object>) listParent.get(0);
      relationDBO.put(JsonKey.ID, relation.get(JsonKey.ORG_ID_ONE.toLowerCase()));
      relationDBO.put(JsonKey.RELATION, JsonKey.CHILD_OF);
      relationDBO.putAll(
          getRelationOrgObject(relationDBO, (String) relation.get(JsonKey.ORG_ID_ONE.toLowerCase())));
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
   * @param relation 
   */
  public void upsertOrgMap(String orgId, String parentOrgId, String rootOrgId, int env, String relation) {
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
   * 
   * @param request
   */
  @SuppressWarnings("unchecked")
  public void validateRootOrg(Map<String, Object> request) throws ProjectCommonException {
    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    if (!ProjectUtil.isStringNullOREmpty((String) request.get(JsonKey.PARENT_ORG_ID))) {
      Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace(),
          orgDbInfo.getTableName(), (String) request.get(JsonKey.PARENT_ORG_ID));
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      Map<String, Object> parentOrgDBO = new HashMap<>();
      if (!(list.isEmpty())) {
        parentOrgDBO = list.get(0);
      }
      if (!ProjectUtil.isStringNullOREmpty((String) parentOrgDBO.get(JsonKey.ROOT_ORG))) {
        String parentRootOrg = parentOrgDBO.get(JsonKey.ROOT_ORG).toString();
        if (null != request.get(JsonKey.ROOT_ORG)) {
          if (null != request.get(JsonKey.ROOT_ORG)
              && !parentRootOrg.equalsIgnoreCase(request.get(JsonKey.ROOT_ORG).toString())) {
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
  public void validateChannelIdForRootOrg(Map<String, Object> request){
    if (null != request.get(JsonKey.IS_ROOT_ORG) && (Boolean) request.get(JsonKey.IS_ROOT_ORG)) {
      if (null == request.get(JsonKey.CHANNEL)) {
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
  public void validateCyclicRelationForOrganisation(Map<String, Object> request){
    if (!ProjectUtil.isStringNullOREmpty((String) request.get(JsonKey.PARENT_ORG_ID))) {
      if (isChildOf((String) request.get(JsonKey.ORGANISATION_ID), (String) request.get(JsonKey.PARENT_ORG_ID))) {
        throw new ProjectCommonException(ResponseCode.cyclicValidationError.getErrorCode(),
            ResponseCode.cyclicValidationError.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
  }

  /**
   * This method will provide user name based on user id if user not found
   * then it will return null.
   *
   * @param userId String
   * @return String
   */
  @SuppressWarnings("unchecked")
  private String getUserNamebyUserId(String userId) {
    Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Response result = cassandraOperation.getRecordById(userdbInfo.getKeySpace(), userdbInfo.getTableName(), userId);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      return (String) (list.get(0).get(JsonKey.USERNAME));
    }
    return null;
  }

  private boolean validateOrgRequest(Map<String, Object> req) {

    if(isNull(req)){
      ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return false;
    }

    if (isNull(req.get(JsonKey.ORGANISATION_ID))) {

      if (isNull(req.get(JsonKey.SOURCE)) || isNull(req.get(JsonKey.EXTERNAL_ID))) {
        ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return false;
      }

      // fetch orgid from database on basis of source and external id and put orgid into request .
      Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

      Map<String, Object> requestDbMap = new HashMap<String, Object>();
      requestDbMap.put(JsonKey.SOURCE, req.get(JsonKey.SOURCE));
      requestDbMap.put(JsonKey.EXTERNAL_ID, req.get(JsonKey.EXTERNAL_ID));


      Response result = cassandraOperation.getRecordsByProperties(userdbInfo.getKeySpace(),
              userdbInfo.getTableName(), requestDbMap);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);

      if (list.isEmpty()) {
        ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
        return false;
      }

      req.put(JsonKey.ORGANISATION_ID, list.get(0).get(JsonKey.ID));

    }
    return true;
  }

}
