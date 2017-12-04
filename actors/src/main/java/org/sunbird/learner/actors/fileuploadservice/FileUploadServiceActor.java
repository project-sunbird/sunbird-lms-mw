package org.sunbird.learner.actors.fileuploadservice;

import akka.actor.UntypedAbstractActor;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.azure.CloudService;
import org.sunbird.common.models.util.azure.CloudServiceFactory;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

/**
 * Class to upload the file on cloud storage. Created by arvind on 28/8/17.
 */
public class FileUploadServiceActor extends UntypedAbstractActor {

  @Override
  public void onReceive(Object message) throws Throwable {

    if (message instanceof Request) {
      try {
        ProjectLogger.log("FileUploadServiceActor onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.FILE_STORAGE_SERVICE.getValue())) {
          processFileUpload(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                  ResponseCode.invalidOperationName.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    } else {
      // Throw exception as message body
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void processFileUpload(Request actorMessage) throws IOException {
    String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.DATA);

    Response response = new Response();
    String fileExtension = "";
    String fileName = (String) req.get(JsonKey.FILE_NAME);
    if(!ProjectUtil.isStringNullOREmpty(fileName)){
      String[] split = fileName.split("\\.");
      if(split.length>1) {
        fileExtension = split[split.length - 1];
      }
    }
    String fName = "File-" + processId;
    if(!ProjectUtil.isStringNullOREmpty(fileExtension)){
      fName = fName+"."+fileExtension;
    }

    File file = new File(fName);
    FileOutputStream fos = null;
    String avatarUrl = null;
    try {
      fos = new FileOutputStream(file);
      fos.write((byte[]) req.get(JsonKey.FILE));

      CloudService service = (CloudService) CloudServiceFactory.get("Azure");
      if (null == service) {
        ProjectLogger.log("The cloud service is not available");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
      }
      String container = (String) req.get(JsonKey.CONTAINER);
      avatarUrl = service.uploadFile(container, file);
    } catch (IOException e) {
      ProjectLogger.log("Exception Occurred while reading file in FileUploadServiceActor", e);
      throw e;
    } finally {
      try {
        if (ProjectUtil.isNotNull(fos)) {
          fos.close();
        }
        if (ProjectUtil.isNotNull(file)) {
          file.delete();
        }
      } catch (IOException e) {
        ProjectLogger
            .log("Exception Occurred while closing fileInputStream in FileUploadServiceActor", e);
      }
    }
    response.put(JsonKey.URL, avatarUrl);
    sender().tell(response, self());
  }
}
