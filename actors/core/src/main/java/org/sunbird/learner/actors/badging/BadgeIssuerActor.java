package org.sunbird.learner.actors.badging;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    if(req.containsKey(BadgingJsonKey.IMAGE) && null != (Object)req.get(BadgingJsonKey.IMAGE)){
       image = (byte[]) req.get(BadgingJsonKey.IMAGE) ;
    }

    Map<String, String> requestData = new HashMap<>();
    requestData.put(JsonKey.NAME , (String)req.get(JsonKey.NAME));
    requestData.put(JsonKey.DESCRIPTION, (String)req.get(JsonKey.DESCRIPTION));
    requestData.put(JsonKey.URL , (String)req.get(JsonKey.URL));
    requestData.put(JsonKey.EMAIL , (String)req.get(JsonKey.EMAIL));

    String url = "/v1/issuer/issuers";

    String badgerBaseUrl = System.getenv(BadgingJsonKey.BADGER_BASE_URL);
    if (ProjectUtil.isStringNullOREmpty(badgerBaseUrl)) {
      badgerBaseUrl = PropertiesCache.getInstance().getProperty(BadgingJsonKey.BADGER_BASE_URL);
    }
    String authKey = System.getenv(BadgingJsonKey.BADGING_AUTHORIZATION_KEY);
    if (ProjectUtil.isStringNullOREmpty(authKey)) {
      authKey = BadgingJsonKey.BADGING_TOKEN +PropertiesCache.getInstance().getProperty(BadgingJsonKey.BADGING_AUTHORIZATION_KEY);
    } else {
      authKey = BadgingJsonKey.BADGING_TOKEN + authKey;
    }

    Map<String , String> headers = new HashMap<>();
    headers.put(JsonKey.AUTHORIZATION, authKey);

    String httpResponseString= null;
    if(null != image) {
      fileData.put(BadgingJsonKey.IMAGE, image);
    }
    httpResponseString = makeBadgerPostRequest(requestData , headers , badgerBaseUrl+url , fileData);

    Response response = new Response();
    Map<String , Object> res  = mapper.readValue(httpResponseString, HashMap.class);
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
