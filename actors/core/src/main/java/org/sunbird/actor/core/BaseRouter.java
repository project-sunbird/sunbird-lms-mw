package org.sunbird.actor.core;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import akka.actor.ActorRef;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public abstract class BaseRouter extends BaseActor {

	public abstract String getRouterMode();
	
	public abstract void route(Request request) throws Throwable;

	@Override
	public void onReceive(Request request) throws Throwable {
		String senderPath = sender().path().toString();
		if (RouterMode.LOCAL.name().equalsIgnoreCase(getRouterMode())
				&& !StringUtils.startsWith(senderPath, "akka://")) {
			throw new RouterException("Invalid invocation of the router. Processing not possible from: " + senderPath);
		}
		route(request);
	}

	protected static String getPropertyValue(String key) {
		String mode = System.getenv(key);
		if (StringUtils.isBlank(mode)) {
			mode = PropertiesCache.getInstance().getProperty(key);
		}
		return mode;
	}

	@Override
	public void unSupportedMessage() {
		ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
				ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		sender().tell(exception, ActorRef.noSender());
	}

	@Override
	public void onReceiveException(String callerName, Exception e) {
		ProjectLogger.log(callerName + ": exception in message processing = " + e.getMessage(), e);
		sender().tell(e, ActorRef.noSender());
	}

}
