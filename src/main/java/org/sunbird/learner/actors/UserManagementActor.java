/**
 *
 */
package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.Constants;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.*;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;
import scala.concurrent.duration.Duration;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

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
            try {
                logger.info("UserManagementActor  onReceive called");
                ProjectLogger.log("UserManagementActor  onReceive called");
                Request actorMessage = (Request) message;
                if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_USER.getValue())) {
                    createUser(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_USER.getValue())) {
                    updateUser(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.LOGIN.getValue())) {
                    login(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.LOGOUT.getValue())) {
                    logout(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CHANGE_PASSWORD.getValue())) {
                    changePassword(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_PROFILE.getValue())) {
                    getUserProfile(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_ROLES.getValue())) {
                    getRoles(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.JOIN_USER_ORGANISATION.getValue())) {
                    joinUserOrganisation(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.APPROVE_USER_ORGANISATION.getValue())) {
                    approveUserOrg(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue())) {
                    getUserDetailsByLoginId(actorMessage);
                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.REJECT_USER_ORGANISATION.getValue())) {
                    rejectUserOrg(actorMessage);
                } else {
                    logger.info("UNSUPPORTED OPERATION");
                    ProjectLogger.log("UNSUPPORTED OPERATION");
                    ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                    sender().tell(exception, self());
                }
            }catch(Exception ex){
                logger.error(ex);
                ProjectLogger.log(ex.getMessage(), ex);
                sender().tell(ex, self());
            }
        } else {
            // Throw exception as message body
            logger.info("UNSUPPORTED MESSAGE");
            ProjectLogger.log("UNSUPPORTED MESSAGE");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
        }
    }

    @SuppressWarnings("unchecked")
    private void getUserDetailsByLoginId(Request actorMessage) {
      Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
      Map<String , Object> userMap=(Map<String, Object>) actorMessage.getRequest().get(JsonKey.USER);
      if(null != userMap.get(JsonKey.LOGIN_ID)){
        String loginId = (String)userMap.get(JsonKey.LOGIN_ID);
        Response resultFrLoginId = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.LOGIN_ID,loginId);
        if(!((List<Map<String,Object>>)resultFrLoginId.get(JsonKey.RESPONSE)).isEmpty()){
          Map<String,Object> map = ((List<Map<String,Object>>)resultFrLoginId.get(JsonKey.RESPONSE)).get(0);
          Map<String, Object> result = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), (String)map.get(JsonKey.USER_ID));
          Response response = new Response();
          if(result !=null) {
          response.put(JsonKey.RESPONSE, result);
          } else {
               result = new HashMap<String, Object>();
               response.put(JsonKey.RESPONSE, result);    
          }
          sender().tell(response, self());
          return;
        }else{
          ProjectCommonException exception = new ProjectCommonException(ResponseCode.userNotFound.getErrorCode(), ResponseCode.userNotFound.getErrorMessage(), ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
          sender().tell(exception, self());
          return;
        }
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
            		if(reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED) && ((boolean)reqMap.get(JsonKey.IS_DELETED)) 
            		    && !ProjectUtil.isStringNullOREmpty((String)reqMap.get(JsonKey.ID))){
            		    deleteRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),(String)reqMap.get(JsonKey.ID));
            		    continue;
            		}
            		  if(!reqMap.containsKey(JsonKey.ID)){
                        reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
                        reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
                        reqMap.put(JsonKey.CREATED_BY, req.get(JsonKey.REQUESTED_BY));
                        reqMap.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
                      }else{
                        reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
                        reqMap.put(JsonKey.UPDATED_BY, req.get(JsonKey.REQUESTED_BY));
                      }
                      try{
                        cassandraOperation.upsertRecord(addrDbInfo.getKeySpace(),addrDbInfo.getTableName(),reqMap);
                      }catch(Exception ex){
                        logger.error(ex);
                        ProjectLogger.log(ex.getMessage(), ex);
                      }
            		
            	}
            }
            if(userMap.containsKey(JsonKey.EDUCATION)){
            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.EDUCATION);
            	for(int i = 0 ; i < reqList.size() ;i++ ){
            		Map<String,Object> reqMap = reqList.get(i);
            		if(reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED) && ((boolean)reqMap.get(JsonKey.IS_DELETED)) 
            		    && !ProjectUtil.isStringNullOREmpty((String)reqMap.get(JsonKey.ID))){
                        deleteRecord(eduDbInfo.getKeySpace(),eduDbInfo.getTableName(),(String)reqMap.get(JsonKey.ID));
                        continue;
                    }
            		processEducationInfo(reqMap,userMap,req,addrDbInfo,eduDbInfo);
            	}
            }
            if(userMap.containsKey(JsonKey.JOB_PROFILE)){
            	List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.JOB_PROFILE);
            	for(int i = 0 ; i < reqList.size() ;i++ ){
            		Map<String,Object> reqMap = reqList.get(i);
            		if(reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED) && ((boolean)reqMap.get(JsonKey.IS_DELETED)) 
            		    && !ProjectUtil.isStringNullOREmpty((String)reqMap.get(JsonKey.ID))){
                        deleteRecord(jobProDbInfo.getKeySpace(),jobProDbInfo.getTableName(),(String)reqMap.get(JsonKey.ID));
                        continue;
                    }
            		processJobProfileInfo(reqMap,userMap,req,addrDbInfo,jobProDbInfo);
            	}
            }
            if(userMap.containsKey(JsonKey.ORGANISATION)){
              List<Map<String,Object>> reqList = (List<Map<String,Object>>)userMap.get(JsonKey.ORGANISATION);
              for(int i = 0 ; i < reqList.size() ;i++ ){
                  Map<String,Object> reqMap = reqList.get(i);
                  if(reqMap.containsKey(JsonKey.IS_DELETED) && null != reqMap.get(JsonKey.IS_DELETED) && ((boolean)reqMap.get(JsonKey.IS_DELETED)) 
                      && !ProjectUtil.isStringNullOREmpty((String)reqMap.get(JsonKey.ID))){
                      deleteRecord(usrOrgDb.getKeySpace(),usrOrgDb.getTableName(),(String)reqMap.get(JsonKey.ID));
                      continue;
                  }
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


	private void deleteRecord(String keyspaceName, String tableName, String id) {
	  try{
	    cassandraOperation.deleteRecord(keyspaceName, tableName, id);
	  }catch(Exception ex){
	    logger.error(ex);
	    ProjectLogger.log(ex.getMessage(), ex);
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
            ProjectLogger.log(ex.getMessage(), ex);
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
         ProjectLogger.log(ex.getMessage(), ex);
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
              ProjectLogger.log(ex.getMessage(), ex);
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
          ProjectLogger.log(ex.getMessage(), ex);
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
              ProjectLogger.log(ex.getMessage(), ex);;
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
          ProjectLogger.log(ex.getMessage(), ex);
        }
	
		
	}

	@SuppressWarnings("unchecked")
	private void checkForEmailAndUserNameUniqueness(Map<String, Object> userMap, DbInfo usrDbInfo) {
   		/*String email = (String)userMap.get(JsonKey.EMAIL);
        Response resultFrEmail = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.EMAIL,email);
        if(((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).size() != 0){
        	Map<String,Object> dbusrMap = ((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).get(0);
        	String usrId = (String) dbusrMap.get(JsonKey.USER_ID);
        	if(!(usrId.equals(userMap.get(JsonKey.ID)))){
        		ProjectCommonException exception = new ProjectCommonException(ResponseCode.emailAlreadyExistError.getErrorCode(), ResponseCode.emailAlreadyExistError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
	            sender().tell(exception, self());
	            return;
        	}
        }*/
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
    		ProjectLogger.log(e.getMessage(), e);
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
	    logger.info("create user method started..");
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
        Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
        Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
        logger.info("collected all the DB setup..");
        Map<String , Object> req = actorMessage.getRequest();
        Map<String,Object> requestMap = null;
        Map<String , Object> userMap=(Map<String, Object>) req.get(JsonKey.USER);
        
	        boolean isSSOEnabled = Boolean.valueOf(PropertiesCache.getInstance().getProperty(JsonKey.IS_SSO_ENABLED));
	        
	        if(!ProjectUtil.isStringNullOREmpty((String)userMap.get(JsonKey.PROVIDER))){
              userMap.put(JsonKey.LOGIN_ID, ((String)userMap.get(JsonKey.USERNAME)+"@"+(String)userMap.get(JsonKey.PROVIDER)));
            }else{
              userMap.put(JsonKey.LOGIN_ID,userMap.get(JsonKey.USERNAME));
            }
		        /*if(null != userMap.get(JsonKey.EMAIL)){
		        	String email = (String)userMap.get(JsonKey.EMAIL);
			        Response resultFrEmail = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.EMAIL,email);
			        if(((List<Map<String,Object>>)resultFrEmail.get(JsonKey.RESPONSE)).size() != 0){
			        	ProjectCommonException exception = new ProjectCommonException(ResponseCode.emailAlreadyExistError.getErrorCode(), ResponseCode.emailAlreadyExistError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
			            sender().tell(exception, self());
			            return;
			        }
		        }*/
		        if(null != userMap.get(JsonKey.LOGIN_ID)){
		        	 String loginId = (String)userMap.get(JsonKey.LOGIN_ID);
		        	 Response resultFrUserName = cassandraOperation.getRecordsByProperty(usrDbInfo.getKeySpace(),usrDbInfo.getTableName(),JsonKey.LOGIN_ID,loginId);
		        	 if(((List<Map<String,Object>>)resultFrUserName.get(JsonKey.RESPONSE)).size() != 0){
		             	ProjectCommonException exception = new ProjectCommonException(ResponseCode.userAlreadyExist.getErrorCode(), ResponseCode.userAlreadyExist.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
		                 sender().tell(exception, self());
		                 return;
		             }
		        }
		        SSOManager ssoManager = new KeyCloakServiceImpl();
		    if(isSSOEnabled){
		    	try{
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
		    		ProjectLogger.log(exception.getMessage(), exception);
		    		sender().tell(exception, self());
		    		return;
		    	}
		    }else{
		    	userMap.put(JsonKey.USER_ID,OneWayHashing.encryptVal((String)userMap.get(JsonKey.USERNAME)));
		    	userMap.put(JsonKey.ID,OneWayHashing.encryptVal((String)userMap.get(JsonKey.USERNAME)));
		    }
		    
            userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
            userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
            if(!ProjectUtil.isStringNullOREmpty((String)userMap.get(JsonKey.PASSWORD))){
              userMap.put(JsonKey.PASSWORD,OneWayHashing.encryptVal((String)userMap.get(JsonKey.PASSWORD)));
            }
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
            }finally{
              if(null == response && isSSOEnabled){
                ssoManager.removeUser(userMap);
              }
            }
            response.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
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
	                      ProjectLogger.log(e.getMessage(), e);
	                    }
	            	}
	            }
	            logger.info("User insertation on DB started--.....");
	            if(userMap.containsKey(JsonKey.EDUCATION)){
	              insertEducationDetails(userMap,addrDbInfo,eduDbInfo);
	              logger.info("User insertation for Education done--.....");
	            }
	            if(userMap.containsKey(JsonKey.JOB_PROFILE)){
	              insertJobProfileDetails(userMap,addrDbInfo,jobProDbInfo);
	              logger.info("User insertation for Job profile done--.....");
	            }
	            if(userMap.containsKey(JsonKey.ORGANISATION)){
	              insertOrganisationDetails(userMap,addrDbInfo,usrOrgDb);
	            }
	            //update the user external identity data
	            logger.info("User insertation for extrenal identity started--.....");
	            updateUserExtId(requestMap,usrExtIdDb);
	            logger.info("User insertation for extrenal identity completed--.....");
            }
            
            logger.info("User created successfully....."); 
            sender().tell(response, self());
            
            Timeout timeout = new Timeout(Duration.create(ProjectUtil.BACKGROUND_ACTOR_WAIT_TIME, TimeUnit.SECONDS));
            if (((String)response.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
              logger.info("method call going to satrt for ES--.....");
                    Response UsrResponse = new Response();
                    UsrResponse.getResult().put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
                    UsrResponse.getResult().put(JsonKey.ID, userMap.get(JsonKey.ID));
                     logger.info("making a call to save user data to ES");
                    Patterns.ask(RequestRouterActor.backgroundJobManager, UsrResponse, timeout);
            }else {
              logger.info("no call for ES to save user");
            }
    }

    @SuppressWarnings("unchecked")
    private void insertOrganisationDetails(Map<String, Object> userMap, DbInfo addrDbInfo, DbInfo usrOrgDb) {

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
                ProjectLogger.log(e.getMessage(), e);
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
            ProjectLogger.log(e.getMessage(), e);
          }
      }
  
    
  }

    @SuppressWarnings("unchecked")
    private void insertJobProfileDetails(Map<String, Object> userMap, DbInfo addrDbInfo, DbInfo jobProDbInfo) {

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
                ProjectLogger.log(e.getMessage(), e);
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
            ProjectLogger.log(e.getMessage(), e);
          }
      }
  
    
  }

    @SuppressWarnings("unchecked")
    private void insertEducationDetails(Map<String, Object> userMap, DbInfo addrDbInfo, DbInfo eduDbInfo) {

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
                ProjectLogger.log(e.getMessage(), e);
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
            ProjectLogger.log(e.getMessage(), e);
          }
      }
  
    
  }

    private void updateUserExtId(Map<String, Object> requestMap, DbInfo usrExtIdDb) {
      Map<String,Object> map = new HashMap<>();
      Map<String,Object> reqMap = new HashMap<>();
      reqMap.put(JsonKey.USER_ID, requestMap.get(JsonKey.USER_ID));
      /* update table for userName,phone,email,Aadhar No
       * for each of these parameter insert a record into db
       * for username update isVerified as true
       * and for others param this will be false
       * once verified will update this flag to true
       */
      
      map.put(JsonKey.USER_ID, requestMap.get(JsonKey.ID));
      map.put(JsonKey.IS_VERIFIED, false);
      if(requestMap.containsKey(JsonKey.USERNAME) && !(ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.USERNAME)))){
        map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        map.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.USERNAME));
        map.put(JsonKey.EXTERNAL_ID_VALUE, JsonKey.USERNAME);
        map.put(JsonKey.IS_VERIFIED, true);
        
        reqMap.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.USERNAME));
        
        updateUserExtIdentity(map,usrExtIdDb);
      }
      if(requestMap.containsKey(JsonKey.PHONE) && !(ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.PHONE)))){
        map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        map.put(JsonKey.EXTERNAL_ID, JsonKey.PHONE);
        map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.PHONE));
        
        if(!ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.PHONE_NUMBER_VERIFIED)) && (boolean)requestMap.get(JsonKey.PHONE_NUMBER_VERIFIED)){
          map.put(JsonKey.IS_VERIFIED, true);
        }
        reqMap.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.PHONE));
        
        updateUserExtIdentity(map,usrExtIdDb);
      }
      if(requestMap.containsKey(JsonKey.EMAIL) && !(ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.EMAIL)))){
        map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        map.put(JsonKey.EXTERNAL_ID, JsonKey.EMAIL);
        map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.EMAIL));
        
        if(!ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.EMAIL_VERIFIED)) && (boolean)requestMap.get(JsonKey.EMAIL_VERIFIED)){
          map.put(JsonKey.IS_VERIFIED, true);
        }
        reqMap.put(JsonKey.EXTERNAL_ID, requestMap.get(JsonKey.EMAIL));
        
        updateUserExtIdentity(map,usrExtIdDb);
      }
      if(requestMap.containsKey(JsonKey.AADHAAR_NO) && !(ProjectUtil.isStringNullOREmpty((String)requestMap.get(JsonKey.AADHAAR_NO)))){
        map.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        map.put(JsonKey.EXTERNAL_ID, JsonKey.AADHAAR_NO);
        map.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.AADHAAR_NO));
        
        reqMap.put(JsonKey.EXTERNAL_ID_VALUE, requestMap.get(JsonKey.AADHAAR_NO));
        
        updateUserExtIdentity(map,usrExtIdDb);
      }
  }

    private void updateUserExtIdentity(Map<String, Object> map, DbInfo usrExtIdDb) {
      try{
          cassandraOperation.insertRecord(usrExtIdDb.getKeySpace(),usrExtIdDb.getTableName(),map);
      }catch(Exception e){
        logger.error(e);
        ProjectLogger.log(e.getMessage(), e);
      }
      
    }
    
    private void removeUnwanted(Map<String, Object> reqMap) {
    	reqMap.remove(JsonKey.ADDRESS);
    	reqMap.remove(JsonKey.EDUCATION);
    	reqMap.remove(JsonKey.JOB_PROFILE);
    	reqMap.remove(JsonKey.ORGANISATION);
    	reqMap.remove(JsonKey.EMAIL_VERIFIED);
    	reqMap.remove(JsonKey.PHONE_NUMBER_VERIFIED);
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

	/**
	 * Method to join the user with organisation ...
	 * @param actorMessage
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	private void joinUserOrganisation(Request actorMessage){

		Response response = new Response();

		Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
		Util.DbInfo organisationDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

		Map<String , Object> req = actorMessage.getRequest();

		Map<String , Object> usrOrgData = (Map<String , Object>)req.get(JsonKey.USER_ORG);
		if(isNull(usrOrgData)){
			//create exception here and sender.tell the exception and return
			ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
			return;
		}

		String updatedBy = null;
		String orgId = null;
		String userId = null;
		if(isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))){
			orgId = (String)usrOrgData.get(JsonKey.ORGANISATION_ID);
		}

		if(isNotNull(usrOrgData.get(JsonKey.USER_ID))){
			userId = (String)usrOrgData.get(JsonKey.USER_ID);
		}
		if(isNotNull(req.get(JsonKey.REQUESTED_BY))){
			updatedBy = (String) req.get(JsonKey.REQUESTED_BY);
		}

		if(isNull(orgId)||isNull(userId)){
			//create exception here invalid request data and tell the exception , ther return
			ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
			return;
		}

		//check org exist or not
		Response orgResult = cassandraOperation.getRecordById(organisationDbInfo.getKeySpace(), organisationDbInfo.getTableName() , orgId);

		List orgList =(List)orgResult.get(JsonKey.RESPONSE);
		if(orgList.size()==0){
			// user already enrolled for the organisation
			logger.info("Org does not exist");
			ProjectLogger.log("Org does not exist");
			ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
			return;
		}

		// check user already exist for the org or not
		Map<String , Object> requestData = new HashMap<String , Object>();
		requestData.put(JsonKey.USER_ID , userId);
		requestData.put(JsonKey.ORGANISATION_ID , orgId);

		Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName() , requestData);

		List list =(List)result.get(JsonKey.RESPONSE);
		if(list.size()>0){
			// user already enrolled for the organisation
			response = new Response();
			response.getResult().put(JsonKey.RESPONSE, "User already joined the organisation");
			sender().tell(response , self());
			return;
		}

		String id = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
		usrOrgData.put(JsonKey.ID , id);
		if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
			String updatedByName = getUserNamebyUserId(updatedBy);
			usrOrgData.put(JsonKey.ADDED_BY_NAME, updatedByName);
			usrOrgData.put(JsonKey.ADDED_BY, updatedBy);
		}
		usrOrgData.put(JsonKey.ORG_JOIN_DATE , ProjectUtil.getFormattedDate());
		usrOrgData.put(JsonKey.IS_REJECTED , false);
		usrOrgData.put(JsonKey.IS_APPROVED , false);

		response = cassandraOperation.insertRecord(userOrgDbInfo.getKeySpace() , userOrgDbInfo.getTableName() , usrOrgData);
		sender().tell(response , self());
		return;

	}

    /**
     * Method to approve the user organisation .
     * @param actorMessage
     */
	@SuppressWarnings("unchecked")
    private void approveUserOrg(Request actorMessage){

        Response response = new Response();
        Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

        Map<String , Object> updateUserOrgDBO = new HashMap<String , Object>();
        Map<String , Object> req = actorMessage.getRequest();
        String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

        Map<String , Object> usrOrgData = (Map<String , Object>)req.get(JsonKey.USER_ORG);
        if(isNull(usrOrgData)){
            //create exception here and sender.tell the exception and return
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        String orgId = null;
        String userId = null;
        String role = null;
        if(isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))){
            orgId = (String)usrOrgData.get(JsonKey.ORGANISATION_ID);
        }

        if(isNotNull(usrOrgData.get(JsonKey.USER_ID))){
            userId = (String)usrOrgData.get(JsonKey.USER_ID);
        }
        if(isNotNull(usrOrgData.get(JsonKey.ROLE))){
            role = (String)usrOrgData.get(JsonKey.ROLE);
        }


        if(isNull(orgId)||isNull(userId)||isNull(role)){
            //creeate exception here invalid request data and tell the exception , ther return
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        // check user already exist for the org or not
        Map<String , Object> requestData = new HashMap<String , Object>();
        requestData.put(JsonKey.USER_ID , userId);
        requestData.put(JsonKey.ORGANISATION_ID , orgId);

        Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName() , requestData);

        List<Map<String , Object>> list =(List<Map<String , Object>>)result.get(JsonKey.RESPONSE);
        if(list.size()==0){
            // user already enrolled for the organisation
            logger.info("User does not belong to org");
			ProjectLogger.log("User does not belong to org");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        Map<String , Object> userOrgDBO = list.get(0);

        if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
            String updatedByName = getUserNamebyUserId(updatedBy);
            updateUserOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
            updateUserOrgDBO.put(JsonKey.APPROVED_BY , updatedByName);
        }
        updateUserOrgDBO.put(JsonKey.ID , (String)userOrgDBO.get(JsonKey.ID));
        updateUserOrgDBO.put(JsonKey.APPROOVE_DATE , ProjectUtil.getFormattedDate());
        updateUserOrgDBO.put(JsonKey.UPDATED_DATE , ProjectUtil.getFormattedDate());


        updateUserOrgDBO.put(JsonKey.IS_APPROVED, true);
        updateUserOrgDBO.put(JsonKey.IS_REJECTED, false);
        updateUserOrgDBO.put(JsonKey.ROLE , role);


        response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace() , userOrgDbInfo.getTableName() , updateUserOrgDBO);
        sender().tell(response , self());
        return;

    }

    /**
     * Method to reject the user organisation .
     * @param actorMessage
     */
    private void rejectUserOrg(Request actorMessage){

        Response response = new Response();
        Util.DbInfo userOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);

        Map<String , Object> updateUserOrgDBO = new HashMap<String , Object>();
        Map<String , Object> req = actorMessage.getRequest();
        String updatedBy = (String) req.get(JsonKey.REQUESTED_BY);

        Map<String , Object> usrOrgData = (Map<String , Object>)req.get(JsonKey.USER_ORG);
        if(isNull(usrOrgData)){
            //create exception here and sender.tell the exception and return
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        String orgId = null;
        String userId = null;

        if(isNotNull(usrOrgData.get(JsonKey.ORGANISATION_ID))){
            orgId = (String)usrOrgData.get(JsonKey.ORGANISATION_ID);
        }

        if(isNotNull(usrOrgData.get(JsonKey.USER_ID))){
            userId = (String)usrOrgData.get(JsonKey.USER_ID);
        }

        if(isNull(orgId)||isNull(userId)){
            //creeate exception here invalid request data and tell the exception , ther return
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        // check user already exist for the org or not
        Map<String , Object> requestData = new HashMap<String , Object>();
        requestData.put(JsonKey.USER_ID , userId);
        requestData.put(JsonKey.ORGANISATION_ID , orgId);

        Response result = cassandraOperation.getRecordsByProperties(userOrgDbInfo.getKeySpace(), userOrgDbInfo.getTableName() , requestData);

        List<Map<String , Object>> list =(List<Map<String , Object>>)result.get(JsonKey.RESPONSE);
        if(list.size()==0){
            // user already enrolled for the organisation
            logger.info("User does not belong to org");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOrgId.getErrorCode(), ResponseCode.invalidOrgId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }

        Map<String , Object> userOrgDBO = list.get(0);

        if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
            String updatedByName = getUserNamebyUserId(updatedBy);
            updateUserOrgDBO.put(JsonKey.UPDATED_BY, updatedBy);
            updateUserOrgDBO.put(JsonKey.APPROVED_BY , updatedByName);
        }
        updateUserOrgDBO.put(JsonKey.ID , (String)userOrgDBO.get(JsonKey.ID));
        updateUserOrgDBO.put(JsonKey.APPROOVE_DATE , ProjectUtil.getFormattedDate());
        updateUserOrgDBO.put(JsonKey.UPDATED_DATE , ProjectUtil.getFormattedDate());

        updateUserOrgDBO.put(JsonKey.IS_APPROVED, false);
        updateUserOrgDBO.put(JsonKey.IS_REJECTED, true);

        response = cassandraOperation.updateRecord(userOrgDbInfo.getKeySpace() , userOrgDbInfo.getTableName() , updateUserOrgDBO);
        sender().tell(response , self());
        return;

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


}