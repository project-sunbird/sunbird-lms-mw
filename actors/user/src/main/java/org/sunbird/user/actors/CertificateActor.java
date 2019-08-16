package org.sunbird.user.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.certificate.Certificate;

import java.sql.Timestamp;
import java.util.*;

/**
 * This class helps in interacting with adding and validating the user-certificate details
 */
@ActorConfig(
        tasks = {"validateCertificate", "addCertificate", "mergeUserCertificate"},
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
        } else if (request.getOperation().equalsIgnoreCase(ActorOperations.ADD_CERTIFICATE.getValue())) {
            addCertificate(request);
        } else if (ActorOperations.MERGE_USER_CERTIFICATE.getValue().equals(request.getOperation())) {
            mergeUserCertificate(request);
        } else{
            unSupportedMessage();
        }
    }

    /** This method merges the certificates from custodian-user to non-custodian-user
     * @param certRequest
     */
    private void mergeUserCertificate(Request certRequest) {
        Response userResult = new Response();
        Map request = certRequest.getRequest();
        String mergeeId = (String) request.get(JsonKey.FROM_ACCOUNT_ID);
        String mergerId = (String) request.get(JsonKey.TO_ACCOUNT_ID);
        Response response = cassandraOperation.getRecordsByProperty(certDbInfo.getKeySpace(),certDbInfo.getTableName(),JsonKey.USER_ID,mergeeId);
        Map<String, Object> record = response.getResult();
        if (null != record && null != record.get(JsonKey.RESPONSE)) {
            List responseList = (List) record.get(JsonKey.RESPONSE);
            if (!responseList.isEmpty()) {
                responseList.forEach(responseMap -> {
                    Map<String, Object> responseDetails = (Map<String, Object>) responseMap;
                    responseDetails.put(JsonKey.USER_ID,mergerId);
                    cassandraOperation.updateRecord(certDbInfo.getKeySpace(),certDbInfo.getTableName(),responseDetails);
                });
            } else {
                ProjectLogger.log(
                        "CertificateActor:getCertificate: cert details unavailable for user id : "
                                + mergeeId,
                        LoggerEnum.ERROR.name());
            }
        }
        userResult.setResponseCode(ResponseCode.success);
        sender().tell(userResult, self());
    }

    /**
     * This method validates the access-code of the certificate and retrieve certificate details.
     *
     * @param userRequest
     */
    private void getCertificate(Request userRequest) {
        Map request = userRequest.getRequest();
        String certificatedId = (String) request.get(JsonKey.CERT_ID);
        String accessCode = (String) request.get(JsonKey.ACCESS_CODE);
        Map<String, Object> responseDetails = getCertificateDetails(certificatedId);
        if (responseDetails.get(JsonKey.ACCESS_CODE.toLowerCase()).equals(accessCode)) {
            Map userResponse = new HashMap<String, Object>();
            Response userResult = new Response();
            Map recordStore = (Map<String, Object>) responseDetails.get(JsonKey.STORE);
            userResponse.put(JsonKey.JSON, recordStore.get(JsonKey.JSON_DATA));
            userResponse.put(JsonKey.PDF, recordStore.get(JsonKey.PDF_URL));
            userResult.put(JsonKey.RESPONSE, userResponse);
            sender().tell(userResult, self());
        } else {
            ProjectLogger.log(
                    "CertificateActor:getCertificate: access code is incorrect : "
                            + accessCode,
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidParameter.getErrorCode(),
                    ProjectUtil.formatMessage(ResponseCode.invalidParameter.getErrorMessage(), JsonKey.ACCESS_CODE),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    private Map getCertificateDetails(String certificatedId) {
        Map<String, Object> responseDetails = null;
        Response response = cassandraOperation.getRecordById(certDbInfo.getKeySpace(), certDbInfo.getTableName(), certificatedId);
        Map<String, Object> record = response.getResult();
        if (null != record && null != record.get(JsonKey.RESPONSE)) {
            List responseList = (List) record.get(JsonKey.RESPONSE);
            if (!responseList.isEmpty()) {
                responseDetails = (Map<String, Object>) responseList.get(0);
            } else {
                ProjectLogger.log(
                        "CertificateActor:getCertificate: cert id is incorrect : "
                                + certificatedId,
                        LoggerEnum.ERROR.name());
                throw new ProjectCommonException(
                        ResponseCode.invalidParameter.getErrorCode(),
                        ProjectUtil.formatMessage(ResponseCode.invalidParameter.getErrorMessage(), JsonKey.CERT_ID),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }
        return responseDetails;
    }


    private void addCertificate(Request request) {
        Map<String, String> storeMap = new HashMap<>();
        Map<String, Object> certAddReqMap = request.getRequest();
        assureUniqueCertId((String) certAddReqMap.get(JsonKey.ID));
        populateStoreMapWithUrl(storeMap, certAddReqMap);
        certAddReqMap.put(JsonKey.STORE, storeMap);
        certAddReqMap = getRequiredRequest(certAddReqMap);
        certAddReqMap.put(JsonKey.CREATED_AT, getTimeStamp());
        Response response = cassandraOperation.insertRecord(certDbInfo.getKeySpace(), certDbInfo.getTableName(), certAddReqMap);
        ProjectLogger.log(
                "CertificateActor:addCertificate:successfully added certificate in records with userId"
                        + certAddReqMap.get(JsonKey.USER_ID)+" and certId:"+certAddReqMap.get(JsonKey.CERT_ID),LoggerEnum.INFO.name());
        sender().tell(response, self());
    }

    private void populateStoreMapWithUrl(Map<String, String> storeMap, Map<String, Object> certAddRequestMap) {
        storeMap.put(JsonKey.PDF_URL, (String) certAddRequestMap.get(JsonKey.PDF_URL));
        storeMap.put(JsonKey.JSON_DATA, certAddRequestMap.get(JsonKey.JSON_DATA).toString());
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
                    "CertificateActor:addCertificate:provided certificateId exists in record ".concat(certificatedId),LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidParameter.getErrorCode(),
                    ResponseMessage.Message.DATA_ALREADY_EXIST,
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        ProjectLogger.log("CertificateActor:addCertificate:successfully certId not found in records creating new record",LoggerEnum.INFO.name());
    }

    private boolean isIdentityPresent(String certificateId) {
        Response response = cassandraOperation.getRecordById(certDbInfo.getKeySpace(), certDbInfo.getTableName(), certificateId);
        Map<String, Object> record = response.getResult();
        if (null != record && null != record.get(JsonKey.RESPONSE)) {
            List responseList = (List) record.get(JsonKey.RESPONSE);
            if (!responseList.isEmpty()) {
                return true;
            } }
        return false;
    }
}
