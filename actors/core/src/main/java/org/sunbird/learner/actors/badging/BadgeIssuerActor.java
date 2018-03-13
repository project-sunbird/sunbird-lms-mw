package org.sunbird.learner.actors.badging;

import akka.actor.ActorRef;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.BadgingActorOperations;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.AbstractBaseActor;
import org.sunbird.learner.util.Util;

/**
 * Created by arvind on 5/3/18.
 */
public class BadgeIssuerActor extends AbstractBaseActor {

  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("AssessmentItemActor onReceive called");
        Request actorMessage = (Request) message;
        Util.initializeContext(actorMessage, JsonKey.USER);
        // set request id fto thread loacl...
        ExecutionContext.setRequestId(actorMessage.getRequestId());
        if(actorMessage.getOperation().equalsIgnoreCase(BadgingActorOperations.CREATE_BADGE_ISSUER.getValue())){
          createBadgeIssuer(actorMessage);
        }else if(actorMessage.getOperation().equalsIgnoreCase(BadgingActorOperations.GET_BADGE_ISSUER.getValue())){
          getBadgeIssuer(actorMessage);
        } else if(actorMessage.getOperation().equalsIgnoreCase(BadgingActorOperations.GET_ALL_ISSUER.getValue())){
          getAllIssuer(actorMessage);
        } else {
          onReceiveUnsupportedOperation(null);
        }
      } catch (Exception ex) {
        onReceiveException(null, ex);
      }
    } else {
      onReceiveUnsupportedMessage(null);
    }

  }

  /**
   *Actor mathod to create the issuer of badge .
   * @param actorMessage
   *
   **/
  private void createBadgeIssuer(Request actorMessage) throws IOException {

    Map<String, Object> req = actorMessage.getRequest();
    Map<String, Object> targetObject = null;
    // correlated object of telemetry event...
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    byte[] image = null;
    Map<String , byte[]> fileData = new HashMap<>();
    if(req.containsKey(JsonKey.IMAGE) && null != (Object)req.get(JsonKey.IMAGE)){
       image = (byte[]) req.get(JsonKey.IMAGE) ;
    }

    Map<String, String> requestData = new HashMap<>();
    requestData.put(JsonKey.NAME , (String)req.get(JsonKey.NAME));
    requestData.put(JsonKey.DESCRIPTION, (String)req.get(JsonKey.DESCRIPTION));
    requestData.put(JsonKey.URL , (String)req.get(JsonKey.URL));
    requestData.put(JsonKey.EMAIL , (String)req.get(JsonKey.EMAIL));

    String url = "/v1/issuer/issuers";
    Map<String , String> headers = BadgingUtil.getBadgrHeaders();

    String httpResponseString= null;
    if(null != image) {
      fileData.put(JsonKey.IMAGE, image);
    }
    httpResponseString = makeBadgerPostRequest(requestData , headers , BadgingUtil.getBadgrBaseUrl()+url , fileData);
    Response response = new Response();
    BadgingUtil.prepareBadgeIssuerResponse(httpResponseString, response.getResult());
    sender().tell(response , ActorRef.noSender());
  }

  private void getBadgeIssuer(Request actorMessage) throws IOException {

    Map<String, Object> req = actorMessage.getRequest();
    String slug = (String) req.get(JsonKey.SLUG);
    String url = "/v1/issuer/issuers"+"/"+slug;

    Map<String, String> headers = BadgingUtil.getBadgrHeaders();

    String result = HttpUtil.sendGetRequest(BadgingUtil.getBadgrBaseUrl()+url , headers);

    if(ProjectUtil.isStringNullOREmpty(result)){
      throw new ProjectCommonException(ResponseCode.invalidIssuerSlug.getErrorCode(),
          ResponseCode.invalidIssuerSlug.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    Response response = new Response();
    BadgingUtil.prepareBadgeIssuerResponse(result, response.getResult());
    sender().tell(response , ActorRef.noSender());
    sender().tell(response , ActorRef.noSender());
  }

  private void getAllIssuer(Request actorMessage) throws IOException {

    String url = "/v1/issuer/issuers";
    Map<String, String> headers = BadgingUtil.getBadgrHeaders();
    String result = HttpUtil.sendGetRequest(BadgingUtil.getBadgrBaseUrl()+url , headers);
    // todo: what should be the message in case of the
    if(ProjectUtil.isStringNullOREmpty(result)){
      throw new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
          ResponseCode.internalError.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    List<Map<String, Object>> issuers = new ArrayList<>();
    Response response = new Response();
    List<Map<String, Object>> data = mapper.readValue(result, new TypeReference<List<Map<String, Object>>>(){});
    for (Object badge : data) {
      Map<String, Object> mappedBadge = new HashMap<>();
      BadgingUtil.prepareBadgeIssuerResponse((Map<String, Object>) badge, mappedBadge);
      issuers.add(mappedBadge);
    }
    Map<String , Object> res = new HashMap<>();
    res.put(BadgingJsonKey.ISSUERS , issuers);
    response.getResult().putAll(res);
    sender().tell(response , ActorRef.noSender());
  }

  /**
   * If file data is not empty means make call to post file as well otherwise simple post call
   * @param requestData
   * @param headers
   * @param url
   * @param fileData
   * @return
   * @throws IOException
   */
  private String makeBadgerPostRequest(Map<String, String> requestData, Map<String, String> headers,
      String url, Map<String, byte[]> fileData) throws IOException {

    String httpResponseString;
    httpResponseString = HttpUtil.postFormData(requestData, fileData, headers, url);
    return httpResponseString;
  }

}
