/**
 *
 */
package org.sunbird.learner.actors;

import java.math.BigInteger;
import java.util.ArrayList;
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
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
        Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
        Response response = null;
        List<Map<String,Object>> list = null;
		Map<String , Object> userMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
        response = cassandraOperation.getRecordById(userDbInfo.getKeySpace(),userDbInfo.getTableName(),(String)userMap.get(JsonKey.USER_ID));
        list = (List<Map<String,Object>>)response.getResult().get(JsonKey.RESPONSE);
        
        if(!(list.isEmpty())) {
            Map<String, Object> map = list.get(0);
            Response addrResponse = cassandraOperation.getRecordsByProperty(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(), JsonKey.USER_ID, userMap.get(JsonKey.USER_ID));
            list = (List<Map<String,Object>>)addrResponse.getResult().get(JsonKey.RESPONSE);
            if(list.size() > 0){
            	map.put(JsonKey.ADDRESS, list);
            }
            
            Response eduResponse = cassandraOperation.getRecordsByProperty(eduDbInfo.getKeySpace(),eduDbInfo.getTableName(), JsonKey.USER_ID, userMap.get(JsonKey.USER_ID));
            list = (List<Map<String,Object>>)eduResponse.getResult().get(JsonKey.RESPONSE);
            if(list.size() > 0){
            	for(Map<String,Object> eduMap : list){
            		String addressId = (String)eduMap.get(JsonKey.ADDRESS_ID);
            		if(!ProjectUtil.isStringNullOREmpty(addressId)){
            			Response addrResponseMap = cassandraOperation.getRecordsByProperty(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(), JsonKey.USER_ID, userMap.get(JsonKey.USER_ID));
            			List<Map<String,Object>> addrList = (List<Map<String,Object>>)addrResponseMap.getResult().get(JsonKey.RESPONSE);
                        if(!(addrList.isEmpty())){
                        	eduMap.put(JsonKey.ADDRESS, addrList.get(0));
                        }
            		}
            	}
            	map.put(JsonKey.EDUCATION, list);
            }
            
            Response jobProfileResponse = cassandraOperation.getRecordsByProperty(jobProDbInfo.getKeySpace(),jobProDbInfo.getTableName(), JsonKey.USER_ID, userMap.get(JsonKey.USER_ID));
            list = (List<Map<String,Object>>)jobProfileResponse.getResult().get(JsonKey.RESPONSE);
            if(list.size() > 0){
            	for(Map<String,Object> eduMap : list){
            		String addressId = (String)eduMap.get(JsonKey.ADDRESS_ID);
            		if(!ProjectUtil.isStringNullOREmpty(addressId)){
            			Response addrResponseMap = cassandraOperation.getRecordsByProperty(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(), JsonKey.USER_ID, userMap.get(JsonKey.USER_ID));
            			List<Map<String,Object>> addrList = (List<Map<String,Object>>)addrResponseMap.getResult().get(JsonKey.RESPONSE);
                        if(!(addrList.isEmpty())){
                        	eduMap.put(JsonKey.ADDRESS, addrList.get(0));
                        }
            		}
            	}
            	map.put(JsonKey.JOB_PROFILE, list);
            }
            
            Util.removeAttributes(map, Arrays.asList(JsonKey.PASSWORD, JsonKey.UPDATED_BY, JsonKey.ID));
        }
        //response.put(key, vo);
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
		Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
		Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
        Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
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
	        Response resultFrEmail = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.EMAIL,email);
	        if(((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).size() != 0){
	        	Map<String,Object> dbusrMap = ((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).get(0);
	        	String usrId = (String) dbusrMap.get(JsonKey.ID);
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
        Response result = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),userMap);
            if(userMap.containsKey(JsonKey.ADDRESS)){
            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.ADDRESS);
            	for(int i = 0 ; i < reqList.size() ;i++ ){
            		Map<String,Object> reqMap = reqList.get(i);
            		reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            		reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
            		reqMap.remove(JsonKey.USER_ID);
            		cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),reqMap);
            	}
            }
            if(userMap.containsKey(JsonKey.EDUCATION)){
            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.EDUCATION);
            	for(int i = 0 ; i < reqList.size() ;i++ ){
            		Map<String,Object> reqMap = reqList.get(i);
            		String addrId = null;
            		Response addrResponse = null;
            		if(reqMap.containsKey(JsonKey.ADDRESS)){
            			Map<String,Object> address = (Map<String,Object>)reqMap.get(JsonKey.ADDRESS);
            			if(address.containsKey(JsonKey.ID)){
            				addrId = ProjectUtil.getUniqueIdFromTimestamp(i+1);
            				address.put(JsonKey.ID, addrId);
            				address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                			address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
            			}else{
            				address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            				address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
            			}
            			
            			addrResponse = cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
            		}
            		if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
            			reqMap.put(JsonKey.ADDRESS_ID, addrId);
            			reqMap.remove(JsonKey.ADDRESS);
            		}
	            	reqMap.put(JsonKey.YEAR_OF_PASSING, ((BigInteger)reqMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
	            	reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
	            	reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
	            	if(null != reqMap.get(JsonKey.PERCENTAGE)){
	            		reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf(reqMap.get(JsonKey.PERCENTAGE))));
	            	}
	            	reqMap.remove(JsonKey.USER_ID);
	            	cassandraOperation.upsertRecord(eduDbInfo.getKeySpace(),eduDbInfo.getTableName(),reqMap);
            	}
            }
            if(userMap.containsKey(JsonKey.JOB_PROFILE)){
            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.JOB_PROFILE);
            	for(int i = 0 ; i < reqList.size() ;i++ ){
            		Map<String,Object> reqMap = reqList.get(i);
            		String addrId = null;
            		Response addrResponse = null;
            		if(reqMap.containsKey(JsonKey.ADDRESS)){
            			Map<String,Object> address = (Map<String,Object>)reqMap.get(JsonKey.ADDRESS);
            			if(address.containsKey(JsonKey.ID)){
            				addrId = ProjectUtil.getUniqueIdFromTimestamp(i+1);
            				address.put(JsonKey.ID, addrId);
            				address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                			address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
            			}else{
            				address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            				address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
            			}
            			addrResponse = cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
            		}
            		if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
            			reqMap.put(JsonKey.ADDRESS_ID, addrId);
            			reqMap.remove(JsonKey.ADDRESS);
            		}
            		reqMap.remove(JsonKey.USER_ID);
            		reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            		reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
            		reqMap.remove(JsonKey.USER_ID);
            		cassandraOperation.upsertRecord(jobProDbInfo.getKeySpace(),jobProDbInfo.getTableName(),reqMap);
            	}
            }
        sender().tell(result, self());
	}

    /**
     * Method to create the new user , Username should be unique .
     * @param actorMessage Request
     */
	@SuppressWarnings("unchecked")
	private void createUser(Request actorMessage){
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
        Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
        Map<String , Object> req = actorMessage.getRequest();
        Map<String,Object> requestMap = null;
        Map<String , Object> userMap=(Map<String, Object>) req.get(JsonKey.USER);
        
	        boolean isSSOEnabled = Boolean.valueOf(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
		        if(null != userMap.get(JsonKey.EMAIL)){
		        	String email = (String)userMap.get(JsonKey.EMAIL);
			        Response resultFrEmail = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.EMAIL,email);
			        if(((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).size() != 0){
			        	ProjectCommonException exception = new ProjectCommonException(ResponseCode.emailAlreadyExistError.getErrorCode(), ResponseCode.emailAlreadyExistError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			            sender().tell(exception, self());
			            return;
			        }
		        }
		        if(null != userMap.get(JsonKey.USERNAME)){
		        	 String userName = (String)userMap.get(JsonKey.USERNAME);
		        	 Response resultFrUserName = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.USERNAME,userName);
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
            requestMap = new HashMap<>();
            requestMap.putAll(userMap);
            removeUnwanted(requestMap);
            Response response = cassandraOperation.insertRecord(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),requestMap);
            if(((String)response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
	            if(userMap.containsKey(JsonKey.ADDRESS)){
	            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.ADDRESS);
	            	for(int i = 0 ; i < reqList.size() ;i++ ){
	            		Map<String,Object> reqMap = reqList.get(i);
	            		reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i+1));
	            		reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
	            		reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
	            		reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
	            		cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),reqMap);
	            	}
	            }
	            if(userMap.containsKey(JsonKey.EDUCATION)){
	            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.EDUCATION);
	            	for(int i = 0 ; i < reqList.size() ;i++ ){
	            		Map<String,Object> reqMap = reqList.get(i);
	            		reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i+1));
	            		String addrId = null;
	            		Response addrResponse = null;
	            		if(reqMap.containsKey(JsonKey.ADDRESS)){
	            			Map<String,Object> address = (Map<String,Object>)reqMap.get(JsonKey.ADDRESS);
	            			addrId = ProjectUtil.getUniqueIdFromTimestamp(i+1);
	            			address.put(JsonKey.ID, addrId);
	            			address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
	            			address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
	            			addrResponse = cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
	            		}
	            		if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
	            			reqMap.put(JsonKey.ADDRESS_ID, addrId);
	            			reqMap.remove(JsonKey.ADDRESS);
	            		}
		            	reqMap.put(JsonKey.YEAR_OF_PASSING, ((BigInteger)reqMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
		            	if(null != reqMap.get(JsonKey.PERCENTAGE)){
		            		reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf(reqMap.get(JsonKey.PERCENTAGE))));
		            	}
		            	reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
		            	reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
		            	reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
		            	cassandraOperation.insertRecord(eduDbInfo.getKeySpace(),eduDbInfo.getTableName(),reqMap);
	            	}
	            }
	            if(userMap.containsKey(JsonKey.JOB_PROFILE)){
	            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.JOB_PROFILE);
	            	for(int i = 0 ; i < reqList.size() ;i++ ){
	            		Map<String,Object> reqMap = reqList.get(i);
	            		reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i+1));
	            		String addrId = null;
	            		Response addrResponse = null;
	            		if(reqMap.containsKey(JsonKey.ADDRESS)){
	            			Map<String,Object> address = (Map<String,Object>)reqMap.get(JsonKey.ADDRESS);
	            			addrId = ProjectUtil.getUniqueIdFromTimestamp(i+1);
	            			address.put(JsonKey.ID, addrId);
	            			address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
	            			address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
	            			addrResponse = cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
	            		}
	            		if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
	            			reqMap.put(JsonKey.ADDRESS_ID, addrId);
	            			reqMap.remove(JsonKey.ADDRESS);
	            		}
	            		reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
	            		reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
	            		reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
	            		cassandraOperation.insertRecord(jobProDbInfo.getKeySpace(),jobProDbInfo.getTableName(),reqMap);
	            	}
	            }
            }
            sender().tell(response, self());
    }

    private void removeUnwanted(Map<String, Object> reqMap) {
    	reqMap.remove(JsonKey.ADDRESS);
    	reqMap.remove(JsonKey.EDUCATION);
    	reqMap.remove(JsonKey.JOB_PROFILE);
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