package org.sunbird.user.actors;

import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class helps in interacting with adding and validating the user-certificate details
 */
@ActorConfig(
        tasks = {"validateCertificate"},
        asyncTasks = {}
)
public class CertificateActor extends UserBaseActor{

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  @Override
  public void onReceive(Request request) throws Throwable {
    if(request.getOperation().equals(ActorOperations.VALIDATE_CERTIFICATE.getValue())) {
      getCertificate(request);
    }

  }

  /** This method validates the access-code of the certificate and retrieve certificate details.
   * @param userRequest
   */
  private void getCertificate(Request userRequest) {
    Map request = userRequest.getRequest();
    String certificatedId = (String) request.get(JsonKey.CERT_ID);
    String accessCode = (String) request.get(JsonKey.ACCESS_CODE);
    Map<String, Object> responseDetails = getCertificateDetails(certificatedId);
    if(responseDetails.get(JsonKey.ACCESS_CODE.toLowerCase()).equals(accessCode)) {
      Map userResponse = new HashMap<String, Object>();
      Response userResult = new Response();
      Map recordStore = (Map<String, Object>) responseDetails.get(JsonKey.STORE);
      userResponse.put(JsonKey.JSON,recordStore.get(JsonKey.JSON));
      userResponse.put(JsonKey.PDF,recordStore.get(JsonKey.PDF));
      userResult.put(JsonKey.RESPONSE,userResponse);
      sender().tell(userResult, self());
    } else {
      ProjectLogger.log(
              "CertificateActor:getCertificate: access code is incorrect : "
                      + accessCode,
              LoggerEnum.ERROR.name());
      throw new ProjectCommonException(
              ResponseCode.invalidParameter.getErrorCode(),
              ProjectUtil.formatMessage(ResponseCode.invalidParameter.getErrorMessage(),JsonKey.ACCESS_CODE),
              ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private Map getCertificateDetails(String certificatedId) {
    Map<String, Object> responseDetails = null;
    Response response = cassandraOperation.getRecordById(JsonKey.SUNBIRD,JsonKey.USER_CERT,certificatedId);
    Map<String, Object> record = response.getResult();
    if(null != record && null != record.get(JsonKey.RESPONSE)) {
      List responseList = (List) record.get(JsonKey.RESPONSE);
      if(!responseList.isEmpty()) {
        responseDetails = (Map<String, Object>) responseList.get(0);
      } else {
        ProjectLogger.log(
                "CertificateActor:getCertificate: cert id is incorrect : "
                        + certificatedId,
                LoggerEnum.ERROR.name());
        throw new ProjectCommonException(
                ResponseCode.invalidParameter.getErrorCode(),
                ProjectUtil.formatMessage(ResponseCode.invalidParameter.getErrorMessage(),JsonKey.CERT_ID),
                ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    return responseDetails;
  }
}
