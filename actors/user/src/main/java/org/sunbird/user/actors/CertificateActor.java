package org.sunbird.user.actors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.certificate.Certificate;

/** This class helps in interacting with adding and validating the user-certificate details */
@ActorConfig(
  tasks = {"validateCertificate", "addCertificate", "getSignUrl"},
  asyncTasks = {}
)
public class CertificateActor extends UserBaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static Util.DbInfo certDbInfo = Util.dbInfoMap.get(JsonKey.USER_CERT);
  private static ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void onReceive(Request request) throws Throwable {
    if (request.getOperation().equals(ActorOperations.VALIDATE_CERTIFICATE.getValue())) {
      getCertificate(request);
    } else if (request
        .getOperation()
        .equalsIgnoreCase(ActorOperations.ADD_CERTIFICATE.getValue())) {
      addCertificate(request);
    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_SIGN_URL.getValue())) {
      getSignUrl(request);
    } else {
      unSupportedMessage();
    }
  }

  /**
   * This method validates the access-code of the certificate and retrieve certificate details.
   *
   * @param userRequest
   */
  private void getCertificate(Request userRequest) throws IOException {
    Map request = userRequest.getRequest();
    String certificatedId = (String) request.get(JsonKey.CERT_ID);
    String accessCode = (String) request.get(JsonKey.ACCESS_CODE);
    Map<String, Object> responseDetails = getCertificateDetails(certificatedId);
    if (responseDetails.get(JsonKey.ACCESS_CODE.toLowerCase()).equals(accessCode)) {
      Map userResponse = new HashMap<String, Object>();
      Response userResult = new Response();
      Map recordStore = (Map<String, Object>) responseDetails.get(JsonKey.STORE);
      String jsonData = (String) recordStore.get(JsonKey.JSON_DATA);
      userResponse.put(JsonKey.JSON_DATA, objectMapper.readValue(jsonData, Map.class));
      userResponse.put(JsonKey.PDF_URL, recordStore.get(JsonKey.PDF_URL));
      userResponse.put(JsonKey.OTHER_LINK, responseDetails.get(JsonKey.OTHER_LINK));
      userResult.put(JsonKey.RESPONSE, userResponse);
      sender().tell(userResult, self());
    } else {
      ProjectLogger.log(
          "CertificateActor:getCertificate: access code is incorrect : " + accessCode,
          LoggerEnum.ERROR.name());
      throw new ProjectCommonException(
          ResponseCode.invalidParameter.getErrorCode(),
          ProjectUtil.formatMessage(
              ResponseCode.invalidParameter.getErrorMessage(), JsonKey.ACCESS_CODE),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private Map getCertificateDetails(String certificatedId) {
    Map<String, Object> responseDetails = null;
    Response response =
        cassandraOperation.getRecordById(
            certDbInfo.getKeySpace(), certDbInfo.getTableName(), certificatedId);
    Map<String, Object> record = response.getResult();
    if (null != record && null != record.get(JsonKey.RESPONSE)) {
      List responseList = (List) record.get(JsonKey.RESPONSE);
      if (!responseList.isEmpty()) {
        responseDetails = (Map<String, Object>) responseList.get(0);
      } else {
        ProjectLogger.log(
            "CertificateActor:getCertificate: cert id is incorrect : " + certificatedId,
            LoggerEnum.ERROR.name());
        throw new ProjectCommonException(
            ResponseCode.invalidParameter.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.invalidParameter.getErrorMessage(), JsonKey.CERT_ID),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    return responseDetails;
  }

  private void addCertificate(Request request) throws JsonProcessingException {
    Map<String, String> storeMap = new HashMap<>();
    Map<String, Object> certAddReqMap = request.getRequest();
    assureUniqueCertId((String) certAddReqMap.get(JsonKey.ID));
    populateStoreMapWithUrl(storeMap, certAddReqMap);
    certAddReqMap.put(JsonKey.STORE, storeMap);
    certAddReqMap = getRequiredRequest(certAddReqMap);
    certAddReqMap.put(JsonKey.CREATED_AT, getTimeStamp());
    Response response =
        cassandraOperation.insertRecord(
            certDbInfo.getKeySpace(), certDbInfo.getTableName(), certAddReqMap);
    ProjectLogger.log(
        "CertificateActor:addCertificate:successfully added certificate in records with userId"
            + certAddReqMap.get(JsonKey.USER_ID)
            + " and certId:"
            + certAddReqMap.get(JsonKey.CERT_ID),
        LoggerEnum.INFO.name());
    sender().tell(response, self());
  }

  private void populateStoreMapWithUrl(
      Map<String, String> storeMap, Map<String, Object> certAddRequestMap)
      throws JsonProcessingException {
    storeMap.put(JsonKey.PDF_URL, (String) certAddRequestMap.get(JsonKey.PDF_URL));
    storeMap.put(
        JsonKey.JSON_DATA,
        objectMapper.writeValueAsString(certAddRequestMap.get(JsonKey.JSON_DATA)));
  }

  private Map<String, Object> getRequiredRequest(Map<String, Object> certAddReqMap) {
    Certificate certificate = objectMapper.convertValue(certAddReqMap, Certificate.class);
    return objectMapper.convertValue(certificate, Map.class);
  }

  private static Timestamp getTimeStamp() {
    return new Timestamp(Calendar.getInstance().getTime().getTime());
  }

  private void assureUniqueCertId(String certificatedId) {
    if (isIdentityPresent(certificatedId)) {
      ProjectLogger.log(
          "CertificateActor:addCertificate:provided certificateId exists in record "
              .concat(certificatedId),
          LoggerEnum.ERROR.name());
      throw new ProjectCommonException(
          ResponseCode.invalidParameter.getErrorCode(),
          ResponseMessage.Message.DATA_ALREADY_EXIST,
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    ProjectLogger.log(
        "CertificateActor:addCertificate:successfully certId not found in records creating new record",
        LoggerEnum.INFO.name());
  }

  private boolean isIdentityPresent(String certificateId) {
    Response response =
        cassandraOperation.getRecordById(
            certDbInfo.getKeySpace(), certDbInfo.getTableName(), certificateId);
    Map<String, Object> record = response.getResult();
    if (null != record && null != record.get(JsonKey.RESPONSE)) {
      List responseList = (List) record.get(JsonKey.RESPONSE);
      if (!responseList.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public void getSignUrl(Request request) {
    try {
      HashMap<String, Object> certReqMap = new HashMap<>();
      certReqMap.put(JsonKey.REQUEST, request.getRequest());
      String requestBody = objectMapper.writeValueAsString(certReqMap);
      String completeUrl =
          ProjectUtil.getConfigValue(JsonKey.SUNBIRD_CERT_SERVICE_BASE_URL)
              + ProjectUtil.getConfigValue(JsonKey.SUNBIRD_CERT_DOWNLOAD_URI);
      ProjectLogger.log(
          "CertificateActor:getSignUrl complete url found: " + completeUrl, LoggerEnum.INFO.name());
      HttpUtilResponse httpResponse = HttpUtil.doPostRequest(completeUrl, requestBody, null);
      if (httpResponse != null && httpResponse.getStatusCode() == 200) {
        HashMap<String, Object> val =
            (HashMap<String, Object>) objectMapper.readValue(httpResponse.getBody(), Map.class);
        HashMap<String, Object> resultMap = (HashMap<String, Object>) val.get(JsonKey.RESULT);
        Response response = new Response();
        response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
        response.put(JsonKey.SIGNED_URL, resultMap.get(JsonKey.SIGNED_URL));
        sender().tell(response, self());
      } else {
        throw new ProjectCommonException(
            ResponseCode.invalidParameter.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.invalidParameter.getErrorMessage(), JsonKey.PDF_URL),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }

    } catch (Exception e) {
      ProjectLogger.log(
          "CertificateActor:getSignUrl exception occured :" + e, LoggerEnum.ERROR.name());
      throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }
}
