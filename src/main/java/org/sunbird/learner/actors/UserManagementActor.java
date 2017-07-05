/**
 *
 */
package org.sunbird.learner.actors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.Constants;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;

import akka.actor.UntypedAbstractActor;

/**
 * This actor will handle course enrollment operation .
 *
 * @author Manzarul
 * @author Amit Kumar
 */
public class UserManagementActor extends UntypedAbstractActor {
    private LogHelper logger = LogHelper.getInstance(UserManagementActor.class.getName());

    private CassandraOperation cassandraOperation = new CassandraOperationImpl();

    /**
     * Receives the actor message and perform the course enrollment operation .
     *
     * @param message
     * @throws Throwable
     */
    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Request) {
            logger.info("UserManagementActor  onReceive called");
            Request actorMessage = (Request) message;
            if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_USER.getValue())) {
            	createUser(actorMessage);
            }else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_USER.getValue())) {
            	updateUser(actorMessage);
            }else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.LOGIN.getValue())) {
            	login(actorMessage);
            }else if(actorMessage.getOperation().equalsIgnoreCase(ActorOperations.LOGOUT.getValue())){
            	logout(actorMessage);
            }else if(actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CHANGE_PASSWORD.getValue())){
            	changePassword(actorMessage);
            }else if(actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_PROFILE.getValue())){
            	getUserProfile(actorMessage);
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

    /**
     * Method to get the user profile .
     * @param actorMessage Request
     */
    @SuppressWarnings("unchecked")
	private void getUserProfile(Request actorMessage) {
        Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
		Map<String , Object> userMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        Response response = cassandraOperation.getRecordById(userDbInfo.getKeySpace(),userDbInfo.getTableName(),(String)userMap.get(JsonKey.USER_ID));
        List<Map<String,Object>> list = (List<Map<String,Object>>)response.getResult().get(JsonKey.RESPONSE);
        if(!(list.isEmpty())) {
            Map<String, Object> map = list.get(0);
            Util.removeAttributes(map, Arrays.asList(JsonKey.PASSWORD, JsonKey.UPDATED_BY, JsonKey.ID));
        }
        sender().tell(response, self());
	}

    /**
     * Method to change the user password .
     * @param actorMessage Request
     */
	@SuppressWarnings("unchecked")
	private void changePassword(Request actorMessage) {
        Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String , Object> userMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        String currentPassword = (String)userMap.get(JsonKey.PASSWORD);
        String newPassword = (String)userMap.get(JsonKey.NEW_PASSWORD);
        Response result = cassandraOperation.getRecordById(userDbInfo.getKeySpace(),userDbInfo.getTableName(),(String)userMap.get(JsonKey.USER_ID));
        List<Map<String,Object>> list = (List<Map<String,Object>>)result.get(JsonKey.RESPONSE);
        if(!(list.isEmpty())) {
            Map<String, Object> resultMap = list.get(0);
            boolean passwordMatched = ((String) resultMap.get(JsonKey.PASSWORD)).equals(OneWayHashing.encryptVal(currentPassword));
            if (passwordMatched) {
                // update the new password
                String newHashedPassword = OneWayHashing.encryptVal(newPassword);
                Map<String, Object> queryMap = new LinkedHashMap<String, Object>();
                queryMap.put(JsonKey.ID, userMap.get(JsonKey.USER_ID));
                queryMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
                queryMap.put(JsonKey.UPDATED_BY, actorMessage.getRequest().get(JsonKey.REQUESTED_BY));
                queryMap.put(JsonKey.PASSWORD, newHashedPassword);
                result = cassandraOperation.updateRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), queryMap);
                sender().tell(result, self());
            } else {
                ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidCredentials.getErrorCode(), ResponseCode.invalidCredentials.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
            }
        }
	}

    /**
     * Method to Logout the user , once logout successfully it will delete the AuthToken from DB.
     * @param actorMessage Request
     */
    private void logout(Request actorMessage) {
        Util.DbInfo userAuthDbInfo = Util.dbInfoMap.get(JsonKey.USER_AUTH_DB);
        String authToken = (String) actorMessage.getRequest().get(JsonKey.AUTH_TOKEN);

        Response result = cassandraOperation.deleteRecord(userAuthDbInfo.getKeySpace(), userAuthDbInfo.getTableName(), authToken);

        result.put(Constants.RESPONSE, JsonKey.SUCCESS);
        sender().tell(result, self());
    }

    /**
     * Method to Login the user by taking Username and password , once login successful it will create Authtoken and return the token.
     * user can login from multiple source at a time for example web, app,etc
     * but for a same source user cann't be login from different machine,
     * for example if user is trying to login from a source from two different machine we are invalidating the auth token of previous machine 
     * and creating a new auth token for 
     * new machine and sending that auth token with response  
     * @param actorMessage Request
     */
	@SuppressWarnings("unchecked")
	private void login(Request actorMessage) {
        Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String , Object> reqMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        Response result = cassandraOperation.getRecordById(userDbInfo.getKeySpace(),userDbInfo.getTableName(),OneWayHashing.encryptVal((String)reqMap.get(JsonKey.USERNAME)));
        List<Map<String, Object>> list = ((List<Map<String,Object>>)result.get(JsonKey.RESPONSE));
        if(null != list && list.size() == 1){
            Map<String , Object> resultMap = list.get(0);
	            if(null != resultMap.get(JsonKey.STATUS) && (ProjectUtil.Status.ACTIVE.getValue()) == (int)resultMap.get(JsonKey.STATUS)){
		            if(ProjectUtil.isStringNullOREmpty(((String)reqMap.get(JsonKey.LOGIN_TYPE)))){
		            	//here login type is general
		                boolean password = ((String)resultMap.get(JsonKey.PASSWORD)).equals(OneWayHashing.encryptVal((String)reqMap.get(JsonKey.PASSWORD)));
		                if(password){
		                    Map<String,Object> userAuthMap =  new HashMap<>();
		                    userAuthMap.put(JsonKey.SOURCE, reqMap.get(JsonKey.SOURCE));
		                    userAuthMap.put(JsonKey.USER_ID, resultMap.get(JsonKey.ID));
		                    userAuthMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
		                    
		                    String userAuth = ProjectUtil.createUserAuthToken((String)resultMap.get(JsonKey.ID), (String)reqMap.get(JsonKey.SOURCE));
		                    userAuthMap.put(JsonKey.ID,userAuth );
		                    checkForDuplicateUserAuthToken(userAuthMap,resultMap,reqMap);
		                    
		                    Map<String,Object> user = new HashMap<>();
		                    user.put(JsonKey.ID, OneWayHashing.encryptVal((String)reqMap.get(JsonKey.USERNAME)));
		                    user.put(JsonKey.LAST_LOGIN_TIME, ProjectUtil.getFormattedDate());
		                    
		                    cassandraOperation.updateRecord(userDbInfo.getKeySpace(),userDbInfo.getTableName(),user);
		
		                    reqMap.remove(JsonKey.PASSWORD);
		                    reqMap.remove(JsonKey.USERNAME);
		                    reqMap.put(JsonKey.FIRST_NAME, resultMap.get(JsonKey.FIRST_NAME));
		                    reqMap.put(JsonKey.TOKEN,userAuthMap.get(JsonKey.ID));
		                    reqMap.put(JsonKey.USER_ID, resultMap.get(JsonKey.USER_ID));
		                    Response response = new Response();
		                    response.put(Constants.RESPONSE, reqMap);
		                    sender().tell(response, self());
		                }else{
		                    ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidCredentials.getErrorCode(), ResponseCode.invalidCredentials.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		                    sender().tell(exception, self());
		                }
		            }else{
		            	//for other login type operation 
		            	ProjectCommonException exception = new ProjectCommonException(ResponseCode.loginTypeError.getErrorCode(), ResponseCode.loginTypeError.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		                sender().tell(exception, self());
		            }
	        }else{
	        	ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidCredentials.getErrorCode(), ResponseCode.invalidCredentials.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
	            sender().tell(exception, self());
	        }
        }else{
        	//TODO:need to implement code for other login like fb login, gmail login etc
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidCredentials.getErrorCode(), ResponseCode.invalidCredentials.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
        }
	}

    /**
     *
     * Method to update the user profile.
     * @param actorMessage
     */
	@SuppressWarnings("unchecked")
	private void updateUser(Request actorMessage) {
		 Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
	        Map<String , Object> req = actorMessage.getRequest();
	        Map<String , Object> userMap=(Map<String, Object>) req.get(JsonKey.USER);
		if(userMap.containsKey(JsonKey.USERNAME) && null != userMap.get(JsonKey.USERNAME)){
			ProjectCommonException exception = new ProjectCommonException(ResponseCode.userNameCanntBeUpdated.getErrorCode(), ResponseCode.userNameCanntBeUpdated.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
		}else if(userMap.containsKey(JsonKey.USERNAME) && null == userMap.get(JsonKey.USERNAME)){
			userMap.remove(JsonKey.USERNAME);
		}
		
		if(null != userMap.get(JsonKey.EMAIL)){
        	String email = (String)userMap.get(JsonKey.EMAIL);
	        Response resultFrEmail = cassandraOperation.getRecordsByProperty(dbInfo.getKeySpace(),dbInfo.getTableName(),JsonKey.EMAIL,email);
	        if(((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).size() != 0){
	        	Map<String,Object> dbusrMap = ((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).get(0);
	        	String usrId = (String) dbusrMap.get(JsonKey.USER_ID);
	        	if(!(usrId.equals(userMap.get(JsonKey.ID)))){
	        		ProjectCommonException exception = new ProjectCommonException(ResponseCode.emailAlreadyExistError.getErrorCode(), ResponseCode.emailAlreadyExistError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
		            sender().tell(exception, self());
		            return;
	        	}
	        }
        }
		
       if(userMap.containsKey(JsonKey.STATUS)){
    	   ProjectCommonException exception = new ProjectCommonException(ResponseCode.statusCanntBeUpdated.getErrorCode(), ResponseCode.statusCanntBeUpdated.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
    	   sender().tell(exception, self());
    	   return;
       }
       
       boolean isSSOEnabled = Boolean.valueOf(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
       if(isSSOEnabled){
	    	try{
		    	SSOManager ssoManager = new KeyCloakServiceImpl();
		    	String userId = ssoManager.updateUser(userMap);
		    	if(!(!ProjectUtil.isStringNullOREmpty(userId) && userId.equalsIgnoreCase((String) JsonKey.SUCCESS))){
		    		ProjectCommonException exception = new ProjectCommonException(ResponseCode.userUpdationUnSuccessfull.getErrorCode(), ResponseCode.userUpdationUnSuccessfull.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
		    		sender().tell(exception, self());
		    		return;
		    	}
	    	}catch(Exception e){
	    		logger.error(e.getMessage(), e);
	    		ProjectCommonException exception = new ProjectCommonException(e.getMessage(), e.getMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
	    		sender().tell(exception, self());
	    		return;
	    	}
	    }
        userMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        userMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
        userMap.put(JsonKey.ID,userMap.get(JsonKey.USER_ID));
        Response result = cassandraOperation.updateRecord(dbInfo.getKeySpace(),dbInfo.getTableName(),userMap);
        sender().tell(result, self());
	}

    /**
     * Method to create the new user , Username should be unique .
     * @param actorMessage Request
     */
	@SuppressWarnings("unchecked")
	private void createUser(Request actorMessage){
        Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String , Object> req = actorMessage.getRequest();
        Map<String , Object> userMap=(Map<String, Object>) req.get(JsonKey.USER);
        
	        boolean isSSOEnabled = Boolean.valueOf(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
		        if(null != userMap.get(JsonKey.EMAIL)){
		        	String email = (String)userMap.get(JsonKey.EMAIL);
			        Response resultFrEmail = cassandraOperation.getRecordsByProperty(dbInfo.getKeySpace(),dbInfo.getTableName(),JsonKey.EMAIL,email);
			        if(((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).size() != 0){
			        	ProjectCommonException exception = new ProjectCommonException(ResponseCode.emailAlreadyExistError.getErrorCode(), ResponseCode.emailAlreadyExistError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			            sender().tell(exception, self());
			            return;
			        }
		        }
		        if(null != userMap.get(JsonKey.USERNAME)){
		        	 String userName = (String)userMap.get(JsonKey.USERNAME);
		        	 Response resultFrUserName = cassandraOperation.getRecordsByProperty(dbInfo.getKeySpace(),dbInfo.getTableName(),JsonKey.USERNAME,userName);
		        	 if(((List<Map<String,Object>>)resultFrUserName.get(JsonKey.RESPONSE)).size() != 0){
		             	ProjectCommonException exception = new ProjectCommonException(ResponseCode.userNameAlreadyExistError.getErrorCode(), ResponseCode.userNameAlreadyExistError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
		                 sender().tell(exception, self());
		                 return;
		             }
		        }
		        
		    if(isSSOEnabled){
		    	try{
			    	SSOManager ssoManager = new KeyCloakServiceImpl();
			    	String userId = ssoManager.createUser(userMap);
			    	if(!ProjectUtil.isStringNullOREmpty(userId)){
				    	userMap.put(JsonKey.USER_ID,userId);
				    	userMap.put(JsonKey.ID,userId);
			    	}else{
			    		ProjectCommonException exception = new ProjectCommonException(ResponseCode.userRegUnSuccessfull.getErrorCode(), ResponseCode.userRegUnSuccessfull.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			    		sender().tell(exception, self());
			    		return;
			    	}
		    	}catch(Exception exception){
		    		logger.error(exception.getMessage(), exception);
		    		sender().tell(exception, self());
		    		return;
		    	}
		    }else{
		    	userMap.put(JsonKey.USER_ID,OneWayHashing.encryptVal((String)userMap.get(JsonKey.USERNAME)));
		    	userMap.put(JsonKey.ID,OneWayHashing.encryptVal((String)userMap.get(JsonKey.USERNAME)));
		    }
            userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
            userMap.put(JsonKey.PASSWORD,OneWayHashing.encryptVal((String)userMap.get(JsonKey.PASSWORD)));
            //userMap.put(JsonKey.ID,OneWayHashing.encryptVal((String)userMap.get(JsonKey.USERNAME)));
           // userMap.put(JsonKey.USER_ID,OneWayHashing.encryptVal((String)userMap.get(JsonKey.USERNAME)));
            Response response = cassandraOperation.insertRecord(dbInfo.getKeySpace(),dbInfo.getTableName(),userMap);
            sender().tell(response, self());
    }

    /**
     * Utility method to provide the unique authtoken .
     * @param userAuthMap
     * @param resultMap
     * @param reqMap
     */
    @SuppressWarnings("unchecked")
	private void checkForDuplicateUserAuthToken(Map<String,Object> userAuthMap,Map<String,Object> resultMap,Map<String , Object> reqMap){
        Util.DbInfo userAuthDbInfo = Util.dbInfoMap.get(JsonKey.USER_AUTH_DB);
        String userAuth=null;
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.SOURCE, reqMap.get(JsonKey.SOURCE));
        map.put(JsonKey.USER_ID, resultMap.get(JsonKey.USER_ID));
        Response authResponse = cassandraOperation.getRecordsByProperties(userAuthDbInfo.getKeySpace(),userAuthDbInfo.getTableName(), map);
        List<Map<String, Object>> userAuthList = ((List<Map<String,Object>>)authResponse.get(JsonKey.RESPONSE));
        if(null != userAuthList && userAuthList.size() == 0){
            cassandraOperation.insertRecord(userAuthDbInfo.getKeySpace(),userAuthDbInfo.getTableName(),userAuthMap);
        }else{
            cassandraOperation.deleteRecord(userAuthDbInfo.getKeySpace(),userAuthDbInfo.getTableName(),(String)((Map<String, Object>) userAuthList.get(0)).get(JsonKey.ID));
            userAuth = ProjectUtil.createUserAuthToken((String)resultMap.get(JsonKey.ID), (String)reqMap.get(JsonKey.SOURCE));
            userAuthMap.put(JsonKey.ID, userAuth);
            userAuthMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            cassandraOperation.insertRecord(userAuthDbInfo.getKeySpace(),userAuthDbInfo.getTableName(),userAuthMap);
        }
    }

}