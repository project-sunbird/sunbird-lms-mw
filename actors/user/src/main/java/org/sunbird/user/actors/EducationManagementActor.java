package org.sunbird.user.actors;

import java.math.BigInteger;
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
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.user.util.UserActorOperations;

@ActorConfig(
  tasks = {},
  asyncTasks = {"upsertUserEducation"}
)
public class EducationManagementActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
  private Util.DbInfo eduDbInfo = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);

  @Override
  public void onReceive(Request request) throws Throwable {
    if (UserActorOperations.UPSERT_USER_EDUCATION
        .getValue()
        .equalsIgnoreCase(request.getOperation())) {
      upsertEducationDetails(request);
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
  private void upsertEducationDetails(Request request) {
    Map<String, Object> requestMap = request.getRequest();
    String operationtype = (String) requestMap.get(JsonKey.OPERATION_TYPE);
    List<Map<String, Object>> reqList =
        (List<Map<String, Object>>) requestMap.get(JsonKey.EDUCATION);

    for (int i = 0; i < reqList.size(); i++) {
      Map<String, Object> educationDetailsMap = reqList.get(i);
      String createdBy = (String) requestMap.get(JsonKey.ID);
      Response addrResponse = null;
      if (JsonKey.CREATE.equalsIgnoreCase(operationtype)) {
        educationDetailsMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i));
        if (educationDetailsMap.containsKey(JsonKey.ADDRESS)) {
          addrResponse = upsertEducationAddressDetails(educationDetailsMap, createdBy);
        }
        insertEducationDetails(requestMap, educationDetailsMap, addrResponse, createdBy);
      } else {
        if (educationDetailsMap.containsKey(JsonKey.IS_DELETED)
            && null != educationDetailsMap.get(JsonKey.IS_DELETED)
            && ((boolean) educationDetailsMap.get(JsonKey.IS_DELETED))
            && !StringUtils.isBlank((String) educationDetailsMap.get(JsonKey.ID))) {
          deleteEducationDetails(educationDetailsMap);
          continue;
        }
        if (educationDetailsMap.containsKey(JsonKey.ADDRESS)) {
          addrResponse = upsertEducationAddressDetails(educationDetailsMap, createdBy);
        }
        updateEducationDetails(requestMap, educationDetailsMap, addrResponse, createdBy);
      }
    }
    saveUserEducationToEs(requestMap);
  }

  private void saveUserEducationToEs(Map<String, Object> requestMap) {
    Request userRequest = new Request();
    userRequest.setOperation(UserActorOperations.UPSERT_USER_EDUCATION_TO_ES.getValue());
    userRequest.getRequest().putAll(requestMap);
    tellToAnother(userRequest);
  }

  private void updateEducationDetails(
      Map<String, Object> requestMap,
      Map<String, Object> educationDetailsMap,
      Response addrResponse,
      String createdBy) {
    if (null != addrResponse
        && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      educationDetailsMap.put(JsonKey.ADDRESS_ID, addrResponse.get(JsonKey.ADDRESS_ID));
      educationDetailsMap.remove(JsonKey.ADDRESS);
    }
    validateYearOfPassingAndPercentageDetails(educationDetailsMap);
    educationDetailsMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    educationDetailsMap.put(JsonKey.UPDATED_BY, createdBy);
    educationDetailsMap.remove(JsonKey.USER_ID);
    try {
      cassandraOperation.upsertRecord(
          eduDbInfo.getKeySpace(), eduDbInfo.getTableName(), educationDetailsMap);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  private void deleteEducationDetails(Map<String, Object> educationDetailsMap) {
    String addrsId = null;
    if (educationDetailsMap.containsKey(JsonKey.ADDRESS)
        && null != educationDetailsMap.get(JsonKey.ADDRESS)) {
      addrsId =
          (String) ((Map<String, Object>) educationDetailsMap.get(JsonKey.ADDRESS)).get(JsonKey.ID);
    } else {
      addrsId = getAddressId((String) educationDetailsMap.get(JsonKey.ID), eduDbInfo);
    }
    if (null != addrsId) {
      // delete eductaion address
      deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
    }
    deleteRecord(
        eduDbInfo.getKeySpace(),
        eduDbInfo.getTableName(),
        (String) educationDetailsMap.get(JsonKey.ID));
  }

  private void deleteRecord(String keyspaceName, String tableName, String id) {
    try {
      cassandraOperation.deleteRecord(keyspaceName, tableName, id);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  private String getAddressId(String id, DbInfo dbInfo) {
    String addressId = null;
    try {
      Response res =
          cassandraOperation.getPropertiesValueById(
              dbInfo.getKeySpace(), dbInfo.getTableName(), id, JsonKey.ADDRESS_ID);
      if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
        addressId =
            (String)
                (((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0))
                    .get(JsonKey.ADDRESS_ID);
      }
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
    return addressId;
  }

  private void insertEducationDetails(
      Map<String, Object> requestMap,
      Map<String, Object> educationDetailsMap,
      Response addrResponse,
      String createdBy) {
    if (null != addrResponse
        && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      educationDetailsMap.put(JsonKey.ADDRESS_ID, addrResponse.get(JsonKey.ADDRESS_ID));
      educationDetailsMap.remove(JsonKey.ADDRESS);
    }
    validateYearOfPassingAndPercentageDetails(educationDetailsMap);
    educationDetailsMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    educationDetailsMap.put(JsonKey.CREATED_BY, createdBy);
    educationDetailsMap.put(JsonKey.USER_ID, requestMap.get(JsonKey.ID));
    try {
      cassandraOperation.insertRecord(
          eduDbInfo.getKeySpace(), eduDbInfo.getTableName(), educationDetailsMap);
      // telemetryGenerationForUserSubFields(
      // reqMap, userMap, false, JsonKey.EDUCATION, JsonKey.USER);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  private void validateYearOfPassingAndPercentageDetails(Map<String, Object> educationDetailsMap) {
    try {
      if (null != educationDetailsMap.get(JsonKey.YEAR_OF_PASSING)) {
        educationDetailsMap.put(
            JsonKey.YEAR_OF_PASSING,
            ((BigInteger) educationDetailsMap.get(JsonKey.YEAR_OF_PASSING)).intValue());
      } else {
        educationDetailsMap.put(JsonKey.YEAR_OF_PASSING, 0);
      }
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
      educationDetailsMap.put(JsonKey.YEAR_OF_PASSING, 0);
    }
    try {
      if (null != educationDetailsMap.get(JsonKey.PERCENTAGE)) {
        educationDetailsMap.put(
            JsonKey.PERCENTAGE,
            Double.parseDouble(String.valueOf(educationDetailsMap.get(JsonKey.PERCENTAGE))));
      } else {
        educationDetailsMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf("0")));
      }
    } catch (Exception ex) {
      educationDetailsMap.put(JsonKey.PERCENTAGE, Double.parseDouble(String.valueOf("0")));
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  private Response upsertEducationAddressDetails(
      Map<String, Object> educationDetailsMap, String createdBy) {
    Response addrResponse = null;
    String addrId = null;
    Map<String, Object> address = (Map<String, Object>) educationDetailsMap.get(JsonKey.ADDRESS);
    if (!address.containsKey(JsonKey.ID)) {
      addrId = ProjectUtil.getUniqueIdFromTimestamp(1);
      address.put(JsonKey.ID, addrId);
      address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      address.put(JsonKey.CREATED_BY, createdBy);
    } else {
      addrId = (String) address.get(JsonKey.ID);
      address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      address.put(JsonKey.UPDATED_BY, createdBy);
      address.remove(JsonKey.USER_ID);
    }
    try {
      addrResponse =
          cassandraOperation.upsertRecord(
              addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
      addrResponse.put(JsonKey.ADDRESS_ID, addrId);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
    return addrResponse;
  }
}
