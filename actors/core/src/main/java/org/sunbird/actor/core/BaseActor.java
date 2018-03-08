package org.sunbird.actor.core;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import akka.actor.UntypedAbstractActor;

/**
 * 
 * @author Vinaya & Mahesh Kumar Gangula
 *
 */

public abstract class BaseActor extends UntypedAbstractActor {

	public abstract void onReceive(Request request) throws Throwable;

	@Override
	public void onReceive(Object message) throws Throwable {
		if (message instanceof Request) {
			Request request = (Request) message;
			String callerName = request.getOperation();
			try {
				onReceive(request);
			} catch (Exception e) {
				onReceiveException(callerName, e);
			}
		} else {
			unSupportedMessage();
		}
	}

	protected void unSupportedMessage() {
		ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
				ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		sender().tell(exception, self());
	}

	protected void onReceiveUnsupportedOperation(String callerName) {
		ProjectLogger.log(callerName + ": unsupported message");
		unSupportedMessage();
	}

	protected void onReceiveUnsupportedMessage(String callerName) {
		ProjectLogger.log(callerName + ": unsupported operation");
		ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
				ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		sender().tell(exception, self());
	}

	protected void onReceiveException(String callerName, Exception e) {
		ProjectLogger.log(callerName + ": exception in message processing = " + e.getMessage(), e);
		sender().tell(e, self());
	}
}
