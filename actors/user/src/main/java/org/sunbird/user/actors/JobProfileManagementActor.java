package org.sunbird.user.actors;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;
import org.sunbird.user.util.UserActorOperations;

@ActorConfig(
  tasks = {"upsertUserJobProfile"},
  asyncTasks = {"upsertUserJobProfile"}
)
public class JobProfileManagementActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
  private Util.DbInfo jobProDbInfo = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);

  @Override
  public void onReceive(Request request) throws Throwable {
    if (UserActorOperations.UPSERT_USER_JOB_PROFILE
        .getValue()
        .equalsIgnoreCase(request.getOperation())) {
      upsertJobProfileDetails(request);
    } else {
      onReceiveUnsupportedOperation("JobProfileManagementActor");
    }
  }

  @SuppressWarnings("unchecked")
  private void upsertJobProfileDetails(Request request) {
    Map<String, Object> requestMap = request.getRequest();
    String operationtype = (String) requestMap.get(JsonKey.OPERATION_TYPE);
    List<Map<String, Object>> reqList =
        (List<Map<String, Object>>) requestMap.get(JsonKey.JOB_PROFILE);
    for (int i = 0; i < reqList.size(); i++) {
      Map<String, Object> jobProfileMap = reqList.get(i);
      String createdBy = (String) requestMap.get(JsonKey.CREATED_BY);
      Response addrResponse = null;
      if (JsonKey.CREATE.equalsIgnoreCase(operationtype)) {
        jobProfileMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i));
        if (jobProfileMap.containsKey(JsonKey.ADDRESS)) {
          addrResponse = upsertJobProfileAddressDetails(jobProfileMap, createdBy);
        }
        insertJobProfileDetails(requestMap, jobProfileMap, addrResponse, createdBy);
      } else {
        if (requestMap.containsKey(JsonKey.IS_DELETED)
            && null != requestMap.get(JsonKey.IS_DELETED)
            && ((boolean) requestMap.get(JsonKey.IS_DELETED))
            && !StringUtils.isBlank((String) requestMap.get(JsonKey.ID))) {
          deleteJobProfileDetails(requestMap);
          continue;
        }
        if (jobProfileMap.containsKey(JsonKey.ADDRESS)) {
          addrResponse = upsertJobProfileAddressDetails(jobProfileMap, createdBy);
        }
        updateJobProfileDetails(jobProfileMap, addrResponse, createdBy);
      }
    }
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(response, self());
  }

  private void updateJobProfileDetails(
      Map<String, Object> jobProfileMap, Response addrResponse, String createdBy) {
    if (null != addrResponse
        && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      jobProfileMap.put(JsonKey.ADDRESS_ID, addrResponse.get(JsonKey.ADDRESS_ID));
      jobProfileMap.remove(JsonKey.ADDRESS);
    }
    jobProfileMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    jobProfileMap.put(JsonKey.UPDATED_BY, createdBy);
    jobProfileMap.remove(JsonKey.USER_ID);
    try {
      cassandraOperation.upsertRecord(
          jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(), jobProfileMap);
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  private void deleteJobProfileDetails(Map<String, Object> requestMap) {
    String addrsId = null;
    if (requestMap.containsKey(JsonKey.ADDRESS) && null != requestMap.get(JsonKey.ADDRESS)) {
      addrsId = (String) ((Map<String, Object>) requestMap.get(JsonKey.ADDRESS)).get(JsonKey.ID);
    } else {
      addrsId = getAddressId((String) requestMap.get(JsonKey.ID), jobProDbInfo);
    }
    if (null != addrsId) {
      deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addrsId);
    }
    deleteRecord(
        jobProDbInfo.getKeySpace(),
        jobProDbInfo.getTableName(),
        (String) requestMap.get(JsonKey.ID));
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

  @SuppressWarnings("unchecked")
  private void insertJobProfileDetails(
      Map<String, Object> requestMap,
      Map<String, Object> jobProfileMap,
      Response addrResponse,
      String createdBy) {
    Map<String, Object> address = null;
    if (null != addrResponse
        && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      jobProfileMap.put(JsonKey.ADDRESS_ID, addrResponse.get(JsonKey.ADDRESS_ID));
      address = (Map<String, Object>) jobProfileMap.get(JsonKey.ADDRESS);
      jobProfileMap.remove(JsonKey.ADDRESS);
    }
    jobProfileMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    jobProfileMap.put(JsonKey.CREATED_BY, createdBy);
    jobProfileMap.put(JsonKey.USER_ID, requestMap.get(JsonKey.ID));
    try {
      cassandraOperation.insertRecord(
          jobProDbInfo.getKeySpace(), jobProDbInfo.getTableName(), jobProfileMap);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    jobProfileMap.put(JsonKey.ADDRESS, address);
  }

  @SuppressWarnings("unchecked")
  private Response upsertJobProfileAddressDetails(
      Map<String, Object> jobProfileDetailsMap, String createdBy) {
    Response addrResponse = null;
    String addrId = null;
    Map<String, Object> address = (Map<String, Object>) jobProfileDetailsMap.get(JsonKey.ADDRESS);
    if (!address.containsKey(JsonKey.ID)) {
      addrId = ProjectUtil.getUniqueIdFromTimestamp(2);
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
