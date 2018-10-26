package org.sunbird.user.actors;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.util.UserActorOperations;

@ActorConfig(
  tasks = {},
  asyncTasks = {"upsertUserAddress"}
)
public class AddressManagementActor extends BaseActor {

  private EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);

  @Override
  public void onReceive(Request request) throws Throwable {
    if (UserActorOperations.UPSERT_USER_ADDRESS
        .getValue()
        .equalsIgnoreCase(request.getOperation())) {
      upsertAddress(request);
    } else {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
              ResponseCode.invalidOperationName.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  @SuppressWarnings("unchecked")
  private void upsertAddress(Request request) {
    Map<String, Object> requestMap = request.getRequest();
    String operationtype = (String) requestMap.get(JsonKey.OPERATION_TYPE);
    List<Map<String, Object>> addressList =
        (List<Map<String, Object>>) requestMap.get(JsonKey.ADDRESS);
    try {
      String encUserId = encryptionService.encryptData((String) requestMap.get(JsonKey.ID));
      String encCreatedById =
          encryptionService.encryptData((String) requestMap.get(JsonKey.CREATED_BY));
      for (int i = 0; i < addressList.size(); i++) {
        Map<String, Object> address = addressList.get(i);
        if (JsonKey.CREATE.equalsIgnoreCase(operationtype)) {
          ProjectLogger.log("upsertAddress called");
          insertAddressDetails(encUserId, encCreatedById, address);
        } else {
          // update
          if (address.containsKey(JsonKey.IS_DELETED)
              && null != address.get(JsonKey.IS_DELETED)
              && ((boolean) address.get(JsonKey.IS_DELETED))
              && !StringUtils.isBlank((String) address.get(JsonKey.ID))) {
            cassandraOperation.deleteRecord(
                addrDbInfo.getKeySpace(),
                addrDbInfo.getTableName(),
                (String) address.get(JsonKey.ID));
            continue;
          }

          if (!address.containsKey(JsonKey.ID)) {
            insertAddressDetails(encUserId, encCreatedById, address);
          } else {
            updateAddressDetails(encCreatedById, address);
          }
        }
      }
      Response response = new Response();
      response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
      sender().tell(response, self());
      // save Address to ES
      // saveUserAddressToEs(requestMap);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  private void saveUserAddressToEs(Map<String, Object> requestMap) {
    Request userRequest = new Request();
    userRequest.setOperation(UserActorOperations.UPSERT_USER_ADDRESS_TO_ES.getValue());
    userRequest.getRequest().putAll(requestMap);
    tellToAnother(userRequest);
  }

  private void updateAddressDetails(String encCreatedById, Map<String, Object> address) {
    address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    address.put(JsonKey.UPDATED_BY, encCreatedById);
    address.remove(JsonKey.USER_ID);
    cassandraOperation.updateRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
  }

  private void insertAddressDetails(
      String encUserId, String encCreatedById, Map<String, Object> reqMap) {
    ProjectLogger.log("insertAddressDetails called");
    reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
    reqMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    reqMap.put(JsonKey.CREATED_BY, encCreatedById);
    reqMap.put(JsonKey.USER_ID, encUserId);
    cassandraOperation.insertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), reqMap);
  }
}
