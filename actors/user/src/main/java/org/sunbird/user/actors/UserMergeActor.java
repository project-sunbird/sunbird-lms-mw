package org.sunbird.user.actors;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.*;

import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.common.util.ConfigUtil;
import org.sunbird.kafka.client.KafkaClient;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.models.systemsetting.SystemSetting;
import org.sunbird.models.user.User;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.KafkaConfigConstants;

@ActorConfig(
        tasks = {"mergeUser"},
        asyncTasks = {}
)
public class UserMergeActor extends UserBaseActor {
  private static Config config = ConfigUtil.getConfig();
  private static String BOOTSTRAP_SERVERS =
          config.getString(KafkaConfigConstants.SUNBIRD_USER_CERT_KAFKA_SERVICE_CONFIG);
  private static String topic = config.getString(KafkaConfigConstants.SUNBIRD_USER_CERT_KAFKA_TOPIC);
  private static Producer<Long, String> producer;
  private ObjectMapper objectMapper = new ObjectMapper();
  private UserService userService = UserServiceImpl.getInstance();
  private SSOManager keyCloakService = SSOServiceFactory.getInstance();
  private SystemSettingClient systemSettingClient = SystemSettingClientImpl.getInstance();

  @Override
  public void onReceive(Request userRequest) throws Throwable {
    if (producer == null) {
      initKafkaClient();
    }
    updateUserMergeDetails(userRequest);
  }

  private void updateUserMergeDetails(Request userRequest) throws IOException {
    ProjectLogger.log("UserMergeActor:updateUserMergeDetails: starts : ", LoggerEnum.DEBUG.name());
    Response response = new Response();
    Map mergeeDBMap = new HashMap<String, Object>();
    HashMap requestMap = (HashMap)userRequest.getRequest();
    Map userCertMap = (Map) requestMap.clone();
    Map headers = (Map) userRequest.getContext().get(JsonKey.HEADER);
    String mergeeId = (String)requestMap.get(JsonKey.FROM_ACCOUNT_ID);
    String mergerId = (String)requestMap.get(JsonKey.TO_ACCOUNT_ID);
    //validating tokens
    checkTokenDetails(headers,mergeeId,mergerId);
    Map telemetryMap = (HashMap)requestMap.clone();
    User mergee = userService.getUserById(mergeeId);
    User merger = userService.getUserById(mergerId);
    String custodianId = getCustodianValue();
    if((!custodianId.equals(mergee.getRootOrgId())) || custodianId.equals(merger.getRootOrgId())) {
      ProjectLogger.log(
              "UserMergeActor:updateUserMergeDetails: Either custodian id is not matching with mergeeid root-org"
                      + mergeeId + "or matching with mergerid root-org" +mergerId,
              LoggerEnum.ERROR.name());
      throw new ProjectCommonException(
              ResponseCode.internalError.getErrorCode(),
              ProjectUtil.formatMessage(ResponseMessage.Message.INTERNAL_ERROR, mergeeId),
              ResponseCode.SERVER_ERROR.getResponseCode());
    }
    if (!mergee.getIsDeleted()) {
      prepareMergeeAccountData(mergee, mergeeDBMap);
      userRequest.put(JsonKey.USER_MERGEE_ACCOUNT, mergeeDBMap);

      // update the merger-course details in cassandra & ES
      String mergerCourseResponse = updateMergerCourseDetails(requestMap);
      if (mergerCourseResponse.equalsIgnoreCase(JsonKey.SUCCESS)) {
        Response mergeeResponse = getUserDao().updateUser(mergeeDBMap);
        String mergeeResponseStr = (String) mergeeResponse.get(JsonKey.RESPONSE);
        ProjectLogger.log(
                "UserMergeActor: updateUserMergeDetails: mergeeResponseStr = " + mergeeResponseStr,
                LoggerEnum.INFO.name());
        updateUserCertDetails(userCertMap);
        Map result = new HashMap<String, Object>();
        result.put(JsonKey.STATUS, JsonKey.SUCCESS);
        response.put(JsonKey.RESULT, result);
        sender().tell(response, self());

        // update mergee details in ES
        mergeUserDetailsToEs(userRequest);

        // create telemetry event for merge
        triggerUserMergeTelemetry(telemetryMap,merger);
      } else {
        ProjectLogger.log(
                "UserMergeActor:updateUserMergeDetails: User course data is not updated for userId : "
                        + mergerId,
                LoggerEnum.ERROR.name());
        throw new ProjectCommonException(
                ResponseCode.internalError.getErrorCode(),
                ProjectUtil.formatMessage(ResponseMessage.Message.INTERNAL_ERROR, mergeeId),
                ResponseCode.SERVER_ERROR.getResponseCode());
      }
    } else {
      ProjectLogger.log(
              "UserMergeActor:updateUserMergeDetails: User mergee is not exist : " + mergeeId,
              LoggerEnum.ERROR.name());
      throw new ProjectCommonException(
              ResponseCode.invalidIdentifier.getErrorCode(),
              ProjectUtil.formatMessage(
                      ResponseMessage.Message.INVALID_PARAMETER_VALUE, mergeeId, JsonKey.FROM_ACCOUNT_ID),
              ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  private String getCustodianValue() {
    String custodianId = null;
    try {
      Map<String, String> configSettingMap = DataCacheHandler.getConfigSettings();
      custodianId = configSettingMap.get(JsonKey.CUSTODIAN_ORG_ID);
      if(custodianId == null || custodianId.isEmpty()) {
        SystemSetting custodianIdSetting =
                systemSettingClient.getSystemSettingByField(
                        getActorRef(ActorOperations.GET_SYSTEM_SETTING.getValue()), JsonKey.CUSTODIAN_ORG_ID);
        if(custodianIdSetting != null) {
          configSettingMap.put(custodianIdSetting.getId(),custodianIdSetting.getValue());
          custodianId = custodianIdSetting.getValue();
        }
      }
    } catch (Exception e) {
      ProjectLogger.log(
              "UserMergeActor:updateTncInfo: Exception occurred while getting system setting for"
                      + JsonKey.CUSTODIAN_ORG_ID
                      + e.getMessage(),
              LoggerEnum.ERROR.name());
    }
    return custodianId;
  }

  private void updateUserCertDetails(Map<String, Object> requestMap) throws IOException {
    String content = null;
    Map<String, Object> userCertMergeRequest = new HashMap<>();
    userCertMergeRequest.put("messageType", "userCertMerge");
    userCertMergeRequest.put("messageDetails",requestMap);
    content = objectMapper.writeValueAsString(userCertMergeRequest);
    ProducerRecord<Long, String> record = new ProducerRecord<>(topic, content);
    if (producer != null) {
      producer.send(record);
    } else {
      ProjectLogger.log(
              "KafkaTelemetryDispatcherActor:dispatchEvents: Kafka producer is not initialised.",
              LoggerEnum.INFO.name());
    }
  }

  private void triggerUserMergeTelemetry(Map telemetryMap, User merger) {
    ProjectLogger.log(
            "UserMergeActor:triggerUserMergeTelemetry: generating telemetry event for merge");
    Map<String, Object> targetObject = null;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    Map<String, String> rollUp = new HashMap<>();
    rollUp.put("l1", merger.getRootOrgId());
    ExecutionContext.getCurrent().getRequestContext().put(JsonKey.ROLLUP, rollUp);
    targetObject =
            TelemetryUtil.generateTargetObject(
                    (String) telemetryMap.get(JsonKey.FROM_ACCOUNT_ID), TelemetryEnvKey.USER, JsonKey.MERGE, null);
    TelemetryUtil.generateCorrelatedObject((String) telemetryMap.get(JsonKey.FROM_ACCOUNT_ID),JsonKey.FROM_ACCOUNT_ID,null,correlatedObject);
    TelemetryUtil.generateCorrelatedObject((String) telemetryMap.get(JsonKey.TO_ACCOUNT_ID),JsonKey.TO_ACCOUNT_ID,null,correlatedObject);
    telemetryMap.remove(JsonKey.ID);
    telemetryMap.remove(JsonKey.USER_ID);
    TelemetryUtil.telemetryProcessingCall(telemetryMap,targetObject,correlatedObject);
  }

  private void mergeUserDetailsToEs(Request userRequest) {
    userRequest.setOperation(ActorOperations.MERGE_USER_TO_ELASTIC.getValue());
    ProjectLogger.log(
            "UserMergeActor: mergeUserDetailsToEs: Trigger sync of user details to ES for user id"
                    + userRequest.getRequest().get(JsonKey.FROM_ACCOUNT_ID),
            LoggerEnum.INFO.name());
    tellToAnother(userRequest);
  }

  private String updateMergerCourseDetails(Map<String, Object> requestMap) throws IOException {
    // call course service api
    String bodyJson = objectMapper.writeValueAsString(requestMap);
    Map headersMap = new HashMap();
    String responseCode = null;
    // courseurl need to provide
    /*HttpUtilResponse httpResponse = HttpUtil.doPostRequest("courseurl", bodyJson, headersMap);
    if (httpResponse.getStatusCode() == ResponseCode.OK.getResponseCode()) {
        String responseStr = httpResponse.getBody();
        if (responseStr != null) {
            Map<String, Object> responseMap = objectMapper.readValue(responseStr, HashMap.class);
            responseCode =  (String) responseMap.get(JsonKey.RESPONSE_CODE);
        }
    }*/
    return responseCode = "SUCCESS";
  }

  private void prepareMergeeAccountData(User mergee, Map mergeeDBMap) {
    mergeeDBMap.put(JsonKey.STATUS, 0);
    mergeeDBMap.put(JsonKey.IS_DELETED, true);
    mergeeDBMap.put(JsonKey.EMAIL, null);
    mergeeDBMap.put(JsonKey.PHONE, null);
    mergeeDBMap.put(JsonKey.USERNAME, null);
    mergeeDBMap.put(JsonKey.PREV_USED_EMAIL, mergee.getEmail());
    mergeeDBMap.put(JsonKey.PREV_USED_PHONE, mergee.getPhone());
    mergeeDBMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    mergeeDBMap.put(JsonKey.ID, mergee.getId());
  }

  private void checkTokenDetails(Map headers, String mergeeId, String mergerId) {
    String[] userAuthToken = (String[]) headers.get(JsonKey.X_AUTHENTICATED_USER_TOKEN);
    String[] sourceUserAuthToken = (String[]) headers.get(JsonKey.X_SOURCE_USER_TOKEN);

    String userId = keyCloakService.verifyToken(userAuthToken[0]);
    String sourceUserId = keyCloakService.verifyToken(sourceUserAuthToken[0]);
    if(!(mergeeId.equals(sourceUserId) && mergerId.equals(userId))) {
      throw new ProjectCommonException(
              ResponseCode.unAuthorized.getErrorCode(),
              ProjectUtil.formatMessage(ResponseMessage.Message.UNAUTHORIZED_USER, mergeeId),
              ResponseCode.UNAUTHORIZED.getResponseCode());
    }
  }

  private void rollBackUserDetails(User mergee) {
    Map<String, Object> userMap = new HashMap<>();
    userMap.put(JsonKey.STATUS, mergee.getStatus());
    userMap.put(JsonKey.IS_DELETED, mergee.getIsDeleted());
    userMap.put(JsonKey.EMAIL, mergee.getEmail());
    userMap.put(JsonKey.PHONE, mergee.getPhone());
    userMap.put(JsonKey.USERNAME, mergee.getUserName());
    if(mergee.getPrevUsedEmail() == null) {
      userMap.put(JsonKey.PREV_USED_EMAIL, null);
    } else {
      userMap.put(JsonKey.PREV_USED_EMAIL, mergee.getPrevUsedEmail());
    }
    if(mergee.getPrevUsedPhone() == null) {
      userMap.put(JsonKey.PREV_USED_PHONE, null);
    } else {
      userMap.put(JsonKey.PREV_USED_PHONE, mergee.getPrevUsedPhone());
    }
    userMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    getUserDao().updateUser(userMap);
  }

  /** Initialises Kafka producer required for dispatching messages on Kafka. */
  private static void initKafkaClient() {
    ProjectLogger.log(
            "KafkaTelemetryDispatcherActor:initKafkaClient: Bootstrap servers = " + BOOTSTRAP_SERVERS,
            LoggerEnum.INFO.name());
    ProjectLogger.log(
            "UserMergeActor:initKafkaClient: topic = " + topic, LoggerEnum.INFO.name());
    try {
      producer = KafkaClient.createProducer(BOOTSTRAP_SERVERS, KafkaConfigConstants.KAFKA_CLIENT_USER_CERT_PRODUCER);
    } catch (Exception e) {
      ProjectLogger.log("UserMergeActor:initKafkaClient: An exception occurred.", e);
    }
  }

}