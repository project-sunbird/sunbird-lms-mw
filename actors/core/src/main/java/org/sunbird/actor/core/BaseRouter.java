package org.sunbird.actor.core;

import java.util.HashMap;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import akka.actor.ActorRef;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public abstract class BaseRouter extends BaseActor {

	protected static Map<String, ActorRef> routingMap = new HashMap<>();

	@Override
	public void onReceive(Request request) throws Throwable {
		org.sunbird.common.request.ExecutionContext.setRequestId(request.getRequestId());
		ActorRef ref = routingMap.get(request.getOperation());
		if (null != ref) {
			ref.tell(request, ActorRef.noSender());
		} else {
			onReceiveUnsupportedOperation(request.getOperation());
		}
	}

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
