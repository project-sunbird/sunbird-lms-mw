package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	            logger.info("OrganisationManagementActor  onReceive called");
	            Request actorMessage = (Request) message;
	            if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_ORG.getValue())) {
	            	createOrg(actorMessage);
	            }else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_ORG.getValue())) {
	            	updateOrg(actorMessage);
	            }else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_ORG_DETAILS.getValue())) {
	            	getOrgDetails(actorMessage);
	            }else {
	                logger.info("UNSUPPORTED OPERATION");
	                ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
	                sender().tell(exception, self());
	            }
	        } else {
	            // Throw exception as message body
	            logger.info("UNSUPPORTED MESSAGE");
	            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
	            sender().tell(exception, self());
	        }
	    }

	@SuppressWarnings("unchecked")
	private void getOrgDetails(Request actorMessage) {
		
		Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
		Map<String , Object> orgMap=(Map<String, Object>)actorMessage.getRequest().get(JsonKey.ORGANISATION);
        Response response = cassandraOperation.getRecordById(userDbInfo.getKeySpace(),userDbInfo.getTableName(),(String)orgMap.get(JsonKey.ORGANISATION_ID));
        sender().tell(response, getSelf());
		
	}

	private void updateOrg(Request actorMessage) {
		// TODO Auto-generated method stub
		
	}

	/** * Method to create the organisation .
     * @param actorMessage
     */
    @SuppressWarnings("unchecked")
	private void createOrg(Request actorMessage) {
        Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
        
        Map<String , Object> req = (Map<String , Object>)actorMessage.getRequest().get(JsonKey.ORGANISATION);
		Map<String , Object> addressReq = null;
		if(null != actorMessage.getRequest().get(JsonKey.ADDRESS)) {
			addressReq = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ADDRESS);
		}
        String updatedBy = (String)actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
        if(!(ProjectUtil.isStringNullOREmpty(updatedBy))){
            String updatedByName = getUserNamebyUserId(updatedBy);
            req.put(JsonKey.ADDED_BY_NAME ,updatedByName);
            req.put(JsonKey.ADDED_BY , updatedBy);
        }
        String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        req.put(JsonKey.ID , uniqueId);
        req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());

		//update address if present in request
		if(null != addressReq && addressReq.size()>0){
			String addressId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
			addressReq.put(JsonKey.ID, addressId);
			addressReq.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());

			if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
				String updatedByName = getUserNamebyUserId(updatedBy);
				addressReq.put(JsonKey.ADDED_BY_NAME, updatedByName);
				addressReq.put(JsonKey.ADDED_BY, updatedBy);
			}
			upsertAddress(addressReq);
		}


        Response result = cassandraOperation.insertRecord(orgDbInfo.getKeySpace(),orgDbInfo.getTableName(),req);
        sender().tell(result , self());
        
    }

	/**
	 * Method to approve the organisation  only if the org in inactive state
	 * @param actorMessage
	 */
	private void approveOrg(Request actorMessage){

		Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

		Map<String , Object> req = (Map<String , Object>)actorMessage.getRequest().get(JsonKey.ORGANISATION);

		Map<String , Object> orgDBO ;
		Map<String , Object> updateOrgDBO = new HashMap<String , Object>();
		String updatedBy = (String)actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

		String orgId = (String)req.get(JsonKey.ORGANISATION_ID);
		Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace() , orgDbInfo.getTableName(), orgId);
		List<Map<String ,Object>> list = (List<Map<String ,Object>>)result.get(JsonKey.RESPONSE);
		if(!(list.isEmpty())){
			orgDBO = (Map<String , Object>)list.get(0);
		}else{
			logger.info("Invalid Org Id");
			ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
			return;
		}

		boolean isApprove = (boolean)req.get(JsonKey.IS_APPROVED);

		if(isApprove){

			String orgStatus = (String) orgDBO.get(JsonKey.STATUS);
			if(!(ProjectUtil.OrgStatus.INACTIVE.getValue().equalsIgnoreCase(orgStatus))){
				logger.info("Can not approve org");
				ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, self());
				return;
			}
			if(!(ProjectUtil.isStringNullOREmpty(updatedBy))){
				String updatedByName = getUserNamebyUserId(updatedBy);
				updateOrgDBO.put(JsonKey.UPDATED_BY_NAME ,updatedByName);
				updateOrgDBO.put(JsonKey.UPDATED_BY , updatedBy);
			}
			updateOrgDBO.put(JsonKey.UPDATED_DATE , ProjectUtil.getFormattedDate());
			updateOrgDBO.put(JsonKey.ID , (String)orgDBO.get(JsonKey.ID));
			updateOrgDBO.put(JsonKey.IS_APPROVED , req.get(JsonKey.IS_APPROVED));
			updateOrgDBO.put(JsonKey.IS_REJECTED , false);
			updateOrgDBO.put(JsonKey.STATUS , ProjectUtil.OrgStatus.ACTIVE.getValue());

			Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace() , orgDbInfo.getTableName() , updateOrgDBO);
			sender().tell(response , self());
			return;
		}else{

			if(!(ProjectUtil.isStringNullOREmpty(updatedBy))){
				String updatedByName = getUserNamebyUserId(updatedBy);
				updateOrgDBO.put(JsonKey.UPDATED_BY_NAME ,updatedByName);
				updateOrgDBO.put(JsonKey.UPDATED_BY , updatedBy);
			}
			updateOrgDBO.put(JsonKey.UPDATED_DATE , ProjectUtil.getFormattedDate());
			updateOrgDBO.put(JsonKey.ID , (String)orgDBO.get(JsonKey.ID));
			updateOrgDBO.put(JsonKey.IS_APPROVED , false);
			updateOrgDBO.put(JsonKey.IS_REJECTED , true);

			Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace() , orgDbInfo.getTableName() , updateOrgDBO);
			sender().tell(response , self());
			return;
		}
	}

	private void updateOrgStatus(Request actorMessage){

		Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

		Map<String , Object> req = (Map<String , Object>)actorMessage.getRequest().get(JsonKey.ORGANISATION);
		Map<String , Object> orgDBO ;
		Map<String , Object> updateOrgDBO = new HashMap<String , Object>();
		String updatedBy = (String)actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

		String orgId = (String)req.get(JsonKey.ORGANISATION_ID);
		Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace() , orgDbInfo.getTableName(), orgId);
		List<Map<String ,Object>> list = (List<Map<String ,Object>>)result.get(JsonKey.RESPONSE);
		if(!(list.isEmpty())){
			orgDBO = (Map<String , Object>)list.get(0);
		}else{
			logger.info("Invalid Org Id");
			ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
			return;
		}

		String currentStatus = (String) orgDBO.get(JsonKey.STATUS);
		String nextStatus = (String) req.get(JsonKey.STATUS);
		 if(!(Util.checkOrgStatusTransition(currentStatus , nextStatus))){

			 logger.info("Invalid Org State transation");
			 ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			 sender().tell(exception, self());
			 return;
		 }

		if(!(ProjectUtil.isStringNullOREmpty(updatedBy))){
			String updatedByName = getUserNamebyUserId(updatedBy);
			updateOrgDBO.put(JsonKey.UPDATED_BY_NAME ,updatedByName);
			updateOrgDBO.put(JsonKey.UPDATED_BY , updatedBy);
		}
		updateOrgDBO.put(JsonKey.UPDATED_DATE , ProjectUtil.getFormattedDate());
		updateOrgDBO.put(JsonKey.ID , (String)orgDBO.get(JsonKey.ID));
		updateOrgDBO.put(JsonKey.STATUS , nextStatus);

		Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace() , orgDbInfo.getTableName() , updateOrgDBO);
		sender().tell(response , self());
		return;

	}

	private void updateOrgData(Request actorMessage){

		Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

		Map<String , Object> req = (Map<String , Object>)actorMessage.getRequest().get(JsonKey.ORGANISATION);
		Map<String , Object> addressReq = null;
		if(null != actorMessage.getRequest().get(JsonKey.ADDRESS)) {
			addressReq = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.ADDRESS);
		}
		Map<String , Object> orgDBO ;
		Map<String , Object> updateOrgDBO = new HashMap<String , Object>();
		String updatedBy = (String)actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

		String orgId = (String)req.get(JsonKey.ORGANISATION_ID);
		Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace() , orgDbInfo.getTableName(), orgId);
		List<Map<String ,Object>> list = (List<Map<String ,Object>>)result.get(JsonKey.RESPONSE);
		if(!(list.isEmpty())){
			orgDBO = (Map<String , Object>)list.get(0);
		}else{
			logger.info("Invalid Org Id");
			ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
			return;
		}

		//update address if present in request
		if(null != addressReq && addressReq.size()>0) {
			if (orgDBO.get(JsonKey.ADDRESS_ID) != null) {
				String addressId = (String) orgDBO.get(JsonKey.ADDRESS_ID);
				addressReq.put(JsonKey.ID, addressId);
			}
			//add new address record
			else {
				String addressId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
				addressReq.put(JsonKey.ID, addressId);
			}
			if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
				String updatedByName = getUserNamebyUserId(updatedBy);
				addressReq.put(JsonKey.UPDATED_BY_NAME, updatedByName);
				addressReq.put(JsonKey.UPDATED_BY, updatedBy);
				upsertAddress(addressReq);
			}
		}

		if(!(ProjectUtil.isStringNullOREmpty(updatedBy))){
			String updatedByName = getUserNamebyUserId(updatedBy);
			updateOrgDBO.put(JsonKey.UPDATED_BY_NAME ,updatedByName);
			updateOrgDBO.put(JsonKey.UPDATED_BY , updatedBy);
		}
		updateOrgDBO.put(JsonKey.UPDATED_DATE , ProjectUtil.getFormattedDate());
		updateOrgDBO.put(JsonKey.ID , (String)orgDBO.get(JsonKey.ID));

		Response response = cassandraOperation.updateRecord(orgDbInfo.getKeySpace() , orgDbInfo.getTableName() , updateOrgDBO);
		sender().tell(response , self());
		return;

	}

	private void getOrgData(Request actorMessage){

		Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

		Map<String , Object> req = (Map<String , Object>)actorMessage.getRequest().get(JsonKey.ORGANISATION);
		Map<String , Object> orgDBO ;
		Map<String , Object> updateOrgDBO = new HashMap<String , Object>();
		Map<String , Object> updateAddressDBO = new HashMap<String , Object>();
		String updatedBy = (String)actorMessage.getRequest().get(JsonKey.REQUESTED_BY);

		String orgId = (String)req.get(JsonKey.ORGANISATION_ID);
		Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace() , orgDbInfo.getTableName(), orgId);
		List<Map<String ,Object>> list = (List<Map<String ,Object>>)result.get(JsonKey.RESPONSE);
		if(!(list.isEmpty())){
			orgDBO = (Map<String , Object>)list.get(0);
		}else{
			logger.info("Invalid Org Id");
			ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
			return;
		}

		if(orgDBO.get(JsonKey.ADDRESS_ID)!= null){
			String addressId = (String)orgDBO.get(JsonKey.ADDRESS_ID);
			Map<String ,Object> address = getAddress(addressId);
			orgDBO.put(JsonKey.ADDRESS , address);
		}
		sender().tell(result , self());

	}

	private Map<String ,Object> getAddress(String addressId){

		Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
		Response result = cassandraOperation.getRecordById(orgDbInfo.getKeySpace() , orgDbInfo.getTableName(), addressId);
		Map<String, Object> addressDBO = new HashMap<String , Object>();

		List<Map<String ,Object>> list = (List<Map<String ,Object>>)result.get(JsonKey.RESPONSE);
		if(!(list.isEmpty())){
			addressDBO = (Map<String, Object>) list.get(0);
		}
		return addressDBO;
	}

	private void upsertAddress(Map<String, Object> addressReq) {

		Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
		Response result = cassandraOperation.upsertRecord(orgDbInfo.getKeySpace() , orgDbInfo.getTableName(), addressReq);

	}



    /**
     * This method will provide user name based on user id if user not found
     * then it will return null.
     * @param userId String
     * @return String
     */
    @SuppressWarnings("unchecked")
	private String getUserNamebyUserId(String userId) {
        Util.DbInfo  userdbInfo=Util.dbInfoMap.get(JsonKey.USER_DB);
        Response result = cassandraOperation.getRecordById(userdbInfo.getKeySpace() , userdbInfo.getTableName(), userId);
        List<Map<String ,Object>> list = (List<Map<String ,Object>>)result.get(JsonKey.RESPONSE);
        if(!(list.isEmpty())){
            return (String)(list.get(0).get(JsonKey.USERNAME));
        }
        return null;
    }

	private void removeUnwantedProperties(Response response , List<String> removableAttributes){
		List<Map<String, Object>> list = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
		for(Map<String, Object> map : list){
			for(String attr : removableAttributes){
				map.remove(attr);
			}
		}
	}

}
