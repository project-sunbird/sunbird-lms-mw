/**
 *
 */
package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import akka.pattern.Patterns;
import akka.util.Timeout;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.Constants;
import org.sunbird.common.ElasticSearchUtil;
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
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;
import scala.concurrent.duration.Duration;

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
            }else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_ROLES.getValue())){
              getRoles(actorMessage);
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
      Map<String , Object> userMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
      Map<String, Object> result = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), (String)userMap.get(JsonKey.USER_ID));
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
        Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
        Map<String , Object> req = actorMessage.getRequest();
        Map<String,Object> requestMap = null;
        Map<String , Object> userMap=(Map<String, Object>) req.get(JsonKey.USER);
        userMap.put(JsonKey.ID,userMap.get(JsonKey.USER_ID));
		if(null != userMap.get(JsonKey.EMAIL)){
			checkForEmailAndUserNameUniqueness(userMap,usrDbInfo);
        }
		//not allowing user to update the status
		userMap.remove(JsonKey.STATUS);
       boolean isSSOEnabled = Boolean.valueOf(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
       if(isSSOEnabled){
	    	UpdateKeyCloakUserBase(userMap);
	    }
        userMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        userMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
        
        requestMap = new HashMap<>();
        requestMap.putAll(userMap);
        if(!userMap.containsKey(JsonKey.ROLES)){
          List<String> roles = new ArrayList<>();
          roles.add(JsonKey.PUBLIC);
          userMap.put(JsonKey.ROLES, roles);
        }
        removeUnwanted(requestMap);
        Response result = null;
        try{
          result = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),requestMap);
        }catch(Exception ex){
          sender().tell(ex, self());
          return;
        }
            if(userMap.containsKey(JsonKey.ADDRESS)){
            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.ADDRESS);
            	for(int i = 0 ; i < reqList.size() ;i++ ){
            		Map<String,Object> reqMap = reqList.get(i);
            		//reqMap.remove(JsonKey.USER_ID);
            		if(!reqMap.containsKey(JsonKey.ID)){
            		  reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
            		  reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                      reqMap.put(JsonKey.CREATED_BY, req.get(JsonKey.REQUESTED_BY));
            		}else{
            		  reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
                      reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
            		}
            		try{
            		  cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),reqMap);
            		}catch(Exception ex){
                      logger.error(ex);
                    }
            	}
            }
            if(userMap.containsKey(JsonKey.EDUCATION)){
            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.EDUCATION);
            	for(int i = 0 ; i < reqList.size() ;i++ ){
            		Map<String,Object> reqMap = reqList.get(i);
            		processEducationInfo(reqMap,userMap,req,addrDbInfo,eduDbInfo);
            	}
            }
            if(userMap.containsKey(JsonKey.JOB_PROFILE)){
            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.JOB_PROFILE);
            	for(int i = 0 ; i < reqList.size() ;i++ ){
            		Map<String,Object> reqMap = reqList.get(i);
            		processJobProfileInfo(reqMap,userMap,req,addrDbInfo,jobProDbInfo);
            	}
            }
            if(userMap.containsKey(JsonKey.ORGANISATION)){
              List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.ORGANISATION);
              for(int i = 0 ; i < reqList.size() ;i++ ){
                  Map<String,Object> reqMap = reqList.get(i);
                  processOrganisationInfo(reqMap,userMap,req,addrDbInfo,usrOrgDb);
              }
              
              updateUserExtId(requestMap,usrExtIdDb);
          }
        sender().tell(result, self());
        
        Timeout timeout = new Timeout(Duration.create(ProjectUtil.BACKGROUND_ACTOR_WAIT_TIME, TimeUnit.SECONDS));
        if (((String)result.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
                Response UsrResponse = new Response();
                UsrResponse.getResult().put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
                UsrResponse.getResult().put(JsonKey.ID, userMap.get(JsonKey.ID));
                Patterns.ask(RequestRouterActor.backgroundJobManager, UsrResponse, timeout);
        }
	}


	private void processOrganisationInfo(Map<String, Object> reqMap, Map<String, Object> userMap,
        Map<String, Object> req, DbInfo addrDbInfo, DbInfo usrOrgDb) {
	  String addrId = null;
      Response addrResponse = null;
      if(reqMap.containsKey(JsonKey.ADDRESS)){
          @SuppressWarnings("unchecked")
          Map<String,Object> address = (Map<String,Object>)reqMap.get(JsonKey.ADDRESS);
          if(!address.containsKey(JsonKey.ID)){
              addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
              address.put(JsonKey.ID, addrId);
              address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
              address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
          }else{
              addrId = (String) address.get(JsonKey.ID);
              address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
              address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
          }
          try{
            addrResponse = cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
          }catch(Exception ex){
            logger.error(ex);
          }
      }
      if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
          reqMap.put(JsonKey.ADDRESS_ID, addrId);
          reqMap.remove(JsonKey.ADDRESS);
      }
      if(reqMap.containsKey(JsonKey.ID)){
        reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
      }else{
        reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        reqMap.put(JsonKey.USER_ID,userMap.get(JsonKey.ID));
      }
      try{
       cassandraOperation.upsertRecord(usrOrgDb.getKeySpace(),usrOrgDb.getTableName(),reqMap);
      }catch(Exception ex){
        logger.error(ex);
      }
    }

  private void processJobProfileInfo(Map<String, Object> reqMap, Map<String, Object> userMap, Map<String, Object> req, DbInfo addrDbInfo, DbInfo jobProDbInfo) {
		String addrId = null;
		Response addrResponse = null;
		if(reqMap.containsKey(JsonKey.ADDRESS)){
			@SuppressWarnings("unchecked")
			Map<String,Object> address = (Map<String,Object>)reqMap.get(JsonKey.ADDRESS);
			if(!address.containsKey(JsonKey.ID)){
				addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
				address.put(JsonKey.ID, addrId);
				address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    			address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
			}else{
				addrId = (String) address.get(JsonKey.ID);
				address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
				address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
			}
			try{
			 addrResponse = cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
			}catch(Exception ex){
              logger.error(ex);
            }
		}
		if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
			reqMap.put(JsonKey.ADDRESS_ID, addrId);
			reqMap.remove(JsonKey.ADDRESS);
		}
		
		if(reqMap.containsKey(JsonKey.ID)){
		  reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
	      reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
		}else{
		  reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
		  reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
		  reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
		  reqMap.put(JsonKey.USER_ID,userMap.get(JsonKey.ID));
		}
		try{
		 cassandraOperation.upsertRecord(jobProDbInfo.getKeySpace(),jobProDbInfo.getTableName(),reqMap);
		}catch(Exception ex){
          logger.error(ex);
        }
	}

	private void processEducationInfo(Map<String, Object> reqMap, Map<String, Object> userMap, Map<String, Object> req,
			DbInfo addrDbInfo, DbInfo eduDbInfo) {

   		String addrId = null;
		Response addrResponse = null;
		if(reqMap.containsKey(JsonKey.ADDRESS)){
			@SuppressWarnings("unchecked")
			Map<String,Object> address = (Map<String,Object>)reqMap.get(JsonKey.ADDRESS);
			if(!address.containsKey(JsonKey.ID)){
				addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
				address.put(JsonKey.ID, addrId);
				address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    			address.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
			}else{
				addrId = (String) address.get(JsonKey.ID);
				address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
				address.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
			}
			try{
			  addrResponse = cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
			}catch(Exception ex){
              logger.error(ex);
            }
		}
		if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
			reqMap.put(JsonKey.ADDRESS_ID, addrId);
			reqMap.remove(JsonKey.ADDRESS);
		}
    	reqMap.put(JsonKey.YEAR_OF_PASSING, ((BigInteger)reqMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
    	
    	if(null != reqMap.get(JsonKey.PERCENTAGE)){
    		reqMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf(reqMap.get(JsonKey.PERCENTAGE))));
    	}
    	if(reqMap.containsKey(JsonKey.ID)){
    	  reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    	  reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
    	}else{
    	  reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
    	  reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    	  reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
    	  reqMap.put(JsonKey.USER_ID,userMap.get(JsonKey.ID));
    	}
    	try{
    	  cassandraOperation.upsertRecord(eduDbInfo.getKeySpace(),eduDbInfo.getTableName(),reqMap);
    	}catch(Exception ex){
          logger.error(ex);
        }
	
		
	}

	@SuppressWarnings("unchecked")
	private void checkForEmailAndUserNameUniqueness(Map<String, Object> userMap, DbInfo usrDbInfo) {
   		String email = (String)userMap.get(JsonKey.EMAIL);
        Response resultFrEmail = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.EMAIL,email);
        if(((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).size() != 0){
        	Map<String,Object> dbusrMap = ((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).get(0);
        	String usrId = (String) dbusrMap.get(JsonKey.USER_ID);
        	if(!(usrId.equals(userMap.get(JsonKey.ID)))){
        		ProjectCommonException exception = new ProjectCommonException(ResponseCode.emailAlreadyExistError.getErrorCode(), ResponseCode.emailAlreadyExistError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
	            sender().tell(exception, self());
	            return;
        	}
        }
        if(null != userMap.get(JsonKey.USERNAME)){
	        String userName = (String)userMap.get(JsonKey.USERNAME);
	        Response resultFruserName = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.USERNAME,userName);
	        if(((List<Map<String,Object>>)resultFruserName.get(JsonKey.RESPONSE)).size() != 0){
	        	Map<String,Object> dbusrMap = ((List<Map<String,Object>>)resultFruserName.get(JsonKey.RESPONSE)).get(0);
	        	String usrId = (String) dbusrMap.get(JsonKey.USER_ID);
	        	if(!(usrId.equals(userMap.get(JsonKey.ID)))){
	        		ProjectCommonException exception = new ProjectCommonException(ResponseCode.userNameAlreadyExistError.getErrorCode(), ResponseCode.userNameAlreadyExistError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
		            sender().tell(exception, self());
		            return;
	        	}
	        }
        }
	}

	private void UpdateKeyCloakUserBase(Map<String, Object> userMap) {
   		try{
	    	SSOManager ssoManager = new KeyCloakServiceImpl();
	    	String userId = ssoManager.updateUser(userMap);
	    	if(!(!ProjectUtil.isStringNullOREmpty(userId) && userId.equalsIgnoreCase(JsonKey.SUCCESS))){
	    		ProjectCommonException exception = new ProjectCommonException(ResponseCode.userUpdationUnSuccessfull.getErrorCode(), ResponseCode.userUpdationUnSuccessfull.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
	    		sender().tell(exception, self());
	    		return;
	    	}
    	}catch(Exception e){
    		logger.error(e.getMessage(), e);
    		ProjectCommonException exception = new ProjectCommonException(ResponseCode.userUpdationUnSuccessfull.getErrorCode(), ResponseCode.userUpdationUnSuccessfull.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
    		sender().tell(exception, self());
    		return;
    	}
   		
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
        Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
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
            if(!userMap.containsKey(JsonKey.ROLES)){
              List<String> roles = new ArrayList<>();
              roles.add(JsonKey.PUBLIC);
              userMap.put(JsonKey.ROLES, roles);
            }
            requestMap = new HashMap<>();
            requestMap.putAll(userMap);
            removeUnwanted(requestMap);
            Response response = null;
            try{
              response = cassandraOperation.insertRecord(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),requestMap);
            }catch(ProjectCommonException exception){
              sender().tell(exception, self());
              return;
            }
            if(((String)response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
	            if(userMap.containsKey(JsonKey.ADDRESS)){
	            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.ADDRESS);
	            	for(int i = 0 ; i < reqList.size() ;i++ ){
	            		Map<String,Object> reqMap = reqList.get(i);
	            		reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i+1));
	            		reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
	            		reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
	            		reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
	            		try{
	            		  cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),reqMap);
	            		}catch(Exception e){
	                      logger.error(e);
	                    }
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
	            			try{
	            			  addrResponse = cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
	            			}catch(Exception e){
	                          logger.error(e);
	                        }
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
		            	try{
		            	  cassandraOperation.insertRecord(eduDbInfo.getKeySpace(),eduDbInfo.getTableName(),reqMap);
		            	}catch(Exception e){
                          logger.error(e);
                        }
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
	            			try{
	            			  addrResponse = cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
	            			}catch(Exception e){
	                          logger.error(e);
	                        }
	            		}
	            		if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
	            			reqMap.put(JsonKey.ADDRESS_ID, addrId);
	            			reqMap.remove(JsonKey.ADDRESS);
	            		}
	            		reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
	            		reqMap.put(JsonKey.CREATED_BY, userMap.get(JsonKey.ID));
	            		reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
	            		try{
	            		  cassandraOperation.insertRecord(jobProDbInfo.getKeySpace(),jobProDbInfo.getTableName(),reqMap);
	            		}catch(Exception e){
                          logger.error(e);
                        }
	            	}
	            }
	            if(userMap.containsKey(JsonKey.ORGANISATION)){
                  List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.ORGANISATION);
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
                          try{
                            addrResponse = cassandraOperation.insertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),address);
                          }catch(Exception e){
                            logger.error(e);
                          }
                      }
                      if(null!= addrResponse && ((String)addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)){
                          reqMap.put(JsonKey.ADDRESS_ID, addrId);
                          reqMap.remove(JsonKey.ADDRESS);
                      }
                      reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
                      try{
                        cassandraOperation.insertRecord(usrOrgDb.getKeySpace(),usrOrgDb.getTableName(),reqMap);
                      }catch(Exception e){
                        logger.error(e);
                      }
                  }
              }
	            updateUserExtId(requestMap,usrExtIdDb);
            }
            
            
            sender().tell(response, self());
            
            Timeout timeout = new Timeout(Duration.create(ProjectUtil.BACKGROUND_ACTOR_WAIT_TIME, TimeUnit.SECONDS));
            if (((String)response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
                    Response UsrResponse = new Response();
                    UsrResponse.getResult().put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
                    UsrResponse.getResult().put(JsonKey.ID, userMap.get(JsonKey.ID));
                    Patterns.ask(RequestRouterActor.backgroundJobManager, UsrResponse, timeout);
            }
    }

    private void updateUserExtId(Map<String, Object> requestMap, DbInfo usrExtIdDb) {
      Map<String,Object> map = new HashMap<>();
      Map<String,Object> reqMap = new HashMap<>();
      String operation = "";
      reqMap.put(JsonKey.USER_ID, map.get(JsonKey.USER_ID));
      
      
      map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
      map.put(JsonKey.USER_ID, requestMap.get(JsonKey.ID));
      //map.put(JsonKey.SOURCE, "");
      map.put(JsonKey.IS_VERIFIED, false);
      if(requestMap.containsKey(JsonKey.USERNAME) && (ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.USERNAME)))){
        map.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.USERNAME));
        map.put(JsonKey.IS_VERIFIED, true);
        
        reqMap.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.USERNAME));
        String id = checkFrRecordInDb(reqMap,usrExtIdDb);
        if(null != id){
          operation = JsonKey.UPDATE;
        }else{
          operation = JsonKey.INSERT;
        }
        updateUserExtIdentity(map,usrExtIdDb,operation);
      }
      if(requestMap.containsKey(JsonKey.PHONE) && (ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.PHONE)))){
        map.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.PHONE));
        
        reqMap.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.PHONE));
        String id = checkFrRecordInDb(reqMap,usrExtIdDb);
        if(null != id){
          operation = JsonKey.UPDATE;
        }else{
          operation = JsonKey.INSERT;
        }
        updateUserExtIdentity(map,usrExtIdDb,operation);
      }
      if(requestMap.containsKey(JsonKey.EMAIL) && (ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.EMAIL)))){
        map.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.EMAIL));
        
        reqMap.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.EMAIL));
        String id = checkFrRecordInDb(reqMap,usrExtIdDb);
        if(null != id){
          operation = JsonKey.UPDATE;
        }else{
          operation = JsonKey.INSERT;
        }
        updateUserExtIdentity(map,usrExtIdDb,operation);
      }
      if(requestMap.containsKey(JsonKey.AADHAAR_NO) && (ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.AADHAAR_NO)))){
        map.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.AADHAAR_NO));
        
        reqMap.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.AADHAAR_NO));
        String id = checkFrRecordInDb(reqMap,usrExtIdDb);
        if(null != id){
          operation = JsonKey.UPDATE;
        }else{
          operation = JsonKey.INSERT;
        }
        updateUserExtIdentity(map,usrExtIdDb,operation);
      }
  }

    private String checkFrRecordInDb(Map<String, Object> map, DbInfo usrExtIdDb) {
      String id = null;
      try{
        Response response = cassandraOperation.getRecordsByProperties(usrExtIdDb.getKeySpace(),usrExtIdDb.getTableName(), map);
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> list = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
        if(list.size() > 0){
          Map<String,Object> usrMap = list.get(0);
          id = (String) usrMap.get(JsonKey.ID);
        }
      }catch(Exception e){
        logger.error(e);
      }
      return id;
    }

    private void updateUserExtIdentity(Map<String, Object> map, DbInfo usrExtIdDb,String operation) {
      try{
        if(operation.equalsIgnoreCase(JsonKey.INSERT)){
          cassandraOperation.insertRecord(usrExtIdDb.getKeySpace(),usrExtIdDb.getTableName(),map);
        }else{
          cassandraOperation.updateRecord(usrExtIdDb.getKeySpace(),usrExtIdDb.getTableName(),map);
        }
      }catch(Exception e){
        logger.error(e);
      }
      
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
    
    /**
     *This method will provide the complete role structure..
     * @param actorMessage Request
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void getRoles(Request actorMessage) {
        Util.DbInfo roleDbInfo = Util.dbInfoMap.get(JsonKey.ROLE);
        Util.DbInfo roleGroupDbInfo = Util.dbInfoMap.get(JsonKey.ROLE_GROUP);
        Util.DbInfo urlActionDbInfo = Util.dbInfoMap.get(JsonKey.URL_ACTION);
        Response mergeResponse = new Response();
        List<Map<String,Object>> resposnemap = new ArrayList<>();
        List<Map<String,Object>> list = null;
        Response response  = cassandraOperation.getAllRecords(roleDbInfo.getKeySpace(), roleDbInfo.getTableName());
        Response rolegroup = cassandraOperation.getAllRecords(roleGroupDbInfo.getKeySpace(), roleGroupDbInfo.getTableName());
        Response urlAction = cassandraOperation.getAllRecords(urlActionDbInfo.getKeySpace(), urlActionDbInfo.getTableName());
        List<Map<String,Object>> urlActionListMap  = (List<Map<String,Object>>) urlAction.getResult().get(JsonKey.RESPONSE);
        List<Map<String,Object>> roleGroupMap  = (List<Map<String,Object>>) rolegroup.getResult().get(JsonKey.RESPONSE);
        list = (List<Map<String,Object>>)response.getResult().get(JsonKey.RESPONSE);
        if (list != null && list.size()>0) {
          //This map will have all the master roles 
          for (Map<String,Object> map: list) {
            Map<String,Object> roleResponseMap = new HashMap<>();
            roleResponseMap.put(JsonKey.ID, map.get(JsonKey.ID));
            roleResponseMap.put(JsonKey.NAME, map.get(JsonKey.NAME));
             List<String> roleGroup = (List) map.get(JsonKey.ROLE_GROUP_ID);
             List<Map<String,Object>> actionGroupListMap = new ArrayList<>();
             roleResponseMap.put(JsonKey.ACTION_GROUPS, actionGroupListMap);
             Map<String,Object> subRoleResponseMap = new HashMap<>();
             for (String val : roleGroup) {
               Map<String,Object> subRoleMap =  getSubRoleListMap(roleGroupMap, val);
               List<String> subRole  =(List) subRoleMap.get(JsonKey.URL_ACTION_ID);
               List<Map<String,Object>> roleUrlResponList = new ArrayList<>();
               subRoleResponseMap.put(JsonKey.ID, subRoleMap.get(JsonKey.ID));
               subRoleResponseMap.put(JsonKey.NAME, subRoleMap.get(JsonKey.NAME));
               for (String rolemap : subRole) {
                 roleUrlResponList.add(getRoleAction(urlActionListMap, rolemap));
               }
               subRoleResponseMap.put(JsonKey.ACTIONS, roleUrlResponList);
             }
             actionGroupListMap.add(subRoleResponseMap);
             resposnemap.add(roleResponseMap);
          }
        }
        mergeResponse.getResult().put(JsonKey.ROLES, resposnemap);
       sender().tell(mergeResponse, self());
    }
    
    
    /**
     * This method will find the action from role action mapping it will return 
     * action id, action name and list of urls.
     * @param urlActionListMap List<Map<String,Object>>
     * @param actionName String
     * @return Map<String,Object>
     */
  @SuppressWarnings("rawtypes")
  private Map<String, Object> getRoleAction(List<Map<String, Object>> urlActionListMap,
      String actionName) {
    Map<String, Object> response = new HashMap<>();
    if (urlActionListMap != null && urlActionListMap.size() > 0) {
      for (Map<String, Object> map : urlActionListMap) {
        if (map.get(JsonKey.ID).equals(actionName)) {
          response.put(JsonKey.ID, map.get(JsonKey.ID));
          response.put(JsonKey.NAME, map.get(JsonKey.NAME));
          response.put(JsonKey.URLS, (List) (map.get(JsonKey.URL) != null ? map.get(JsonKey.URL)
              : new ArrayList<String>()));
          return response;
        }
      }
    }
    return response;
  }
  
 /**
  * This method will provide sub role mapping details. 
  * @param urlActionListMap List<Map<String, Object>>
  * @param roleName String
  * @return  Map< String, Object>
  */
 @SuppressWarnings("rawtypes")
private Map< String, Object> getSubRoleListMap (List<Map<String, Object>> urlActionListMap,
      String roleName) {
    Map<String, Object> response = new HashMap<>();
    if (urlActionListMap != null && urlActionListMap.size() > 0) {
      for (Map<String, Object> map : urlActionListMap) {
        if (map.get(JsonKey.ID).equals(roleName)) {
          response.put(JsonKey.ID, map.get(JsonKey.ID));
          response.put(JsonKey.NAME, map.get(JsonKey.NAME));
          response.put(JsonKey.URL_ACTION_ID, (List) (map.get(JsonKey.URL_ACTION_ID) !=null ?map.get(JsonKey.URL_ACTION_ID):new ArrayList<>()));
          return response;
        }
      }
    }
    return response;
  }

}