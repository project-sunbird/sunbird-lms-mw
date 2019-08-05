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

  private void getCertificate(Request userRequest) {
    Map request = userRequest.getRequest();
    Map result = new HashMap<String, Object>();
    String certificatedId = (String) request.get(JsonKey.CERT_ID);
    String accessCode = (String) request.get(JsonKey.ACCESS_CODE);
    Response response = cassandraOperation.getRecordById(JsonKey.SUNBIRD,JsonKey.USER_CERT,certificatedId);
    Map<String, Object> record = response.getResult();
    if(null != record && null != record.get(JsonKey.RESPONSE)) {
      List responseList = (List)record.get(JsonKey.RESPONSE);
      Map<String, Object> responseDetails = (Map<String, Object>) responseList.get(0);
      if(accessCode.equals(responseDetails.get(JsonKey.ACCESS_CODE))) {
        Map recordStore = (Map<String, Object>) responseDetails.get(JsonKey.STORE);
        result.put("json",recordStore.get("json"));
        result.put("pdf",recordStore.get("pdf"));
        result.put(JsonKey.STATUS, JsonKey.SUCCESS);
        response.put(JsonKey.RESULT, result);
        // update mergee details in ES
        sender().tell(response, self());
      } else {
        //Exception related column data
      }
    } else {
      ProjectLogger.log(
              "CertificateActor:getCertificate: access code is incorrect for certid : "
                      + certificatedId,
              LoggerEnum.ERROR.name());
      throw new ProjectCommonException(
              ResponseCode.accessCodeInvalid.getErrorCode(),
              ResponseCode.accessCodeInvalid.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
    }

  }
}
