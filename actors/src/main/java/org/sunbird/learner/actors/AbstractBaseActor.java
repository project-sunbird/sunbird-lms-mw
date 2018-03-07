package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.responsecode.ResponseCode;

public abstract class AbstractBaseActor extends UntypedAbstractActor {
    protected void onReceiveUnsupportedOperation(String callerName) {
        ProjectLogger.log(callerName + ": unsupported message");
        ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
    }

    protected void onReceiveUnsupportedMessage(String callerName) {
        ProjectLogger.log(callerName + ": unsupported operation");
        ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                ResponseCode.invalidOperationName.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
    }

    protected void onReceiveException(String callerName, Exception e) {
        ProjectLogger.log(callerName + ": exception in message processing = " + e.getMessage(), e);
        sender().tell(e, self());
    }
}
