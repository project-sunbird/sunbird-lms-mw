package org.sunbird.user.actors;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;

@ActorConfig(
		  tasks = {"resetPassword"},
		  asyncTasks = {}
		)
public class ResetPasswordActor extends BaseActor {
	
    private SSOManager ssoManager = SSOServiceFactory.getInstance();
    
	@Override
	public void onReceive(Request request) throws Throwable {
		String userId = (String)request.get(JsonKey.USER_ID);
		String password = (String)request.get(JsonKey.PASSWORD);
		resetPassword(userId,password);	
	}
	
	private void resetPassword(String userId, String password) {
		ProjectLogger.log("ResetPasswordActor:resetPassword: method called.",LoggerEnum.INFO.name());
		if(ssoManager.updatePassword(userId, password)){
			Response response = new Response();
			response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
		} else {
			ProjectCommonException.throwServerErrorException(ResponseCode.invalidUserCredentials);
		}
	}
	

}
