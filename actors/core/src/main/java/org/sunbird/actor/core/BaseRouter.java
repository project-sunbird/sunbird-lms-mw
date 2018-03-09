package org.sunbird.actor.core;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.responsecode.ResponseCode;

import akka.actor.ActorRef;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public abstract class BaseRouter extends BaseActor {

	@Override
	protected void unSupportedMessage() {
		ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
				ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		sender().tell(exception, ActorRef.noSender());
	}

	@Override
	protected void onReceiveException(String callerName, Exception e) {
		ProjectLogger.log(callerName + ": exception in message processing = " + e.getMessage(), e);
		sender().tell(e, ActorRef.noSender());
	}

}
