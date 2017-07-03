package org.sunbird.learner.actors;

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
        String updatedBy = (String)actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
        if(!(ProjectUtil.isStringNullOREmpty(updatedBy))){
            String updatedByName = getUserNamebyUserId(updatedBy);
            req.put(JsonKey.ADDED_BY_NAME ,updatedByName);
            req.put(JsonKey.ADDED_BY , updatedBy);
        }
        String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        req.put(JsonKey.ID , uniqueId);
        req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
        Response result = cassandraOperation.insertRecord(orgDbInfo.getKeySpace(),orgDbInfo.getTableName(),req);
        sender().tell(result , self());
        
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

}
