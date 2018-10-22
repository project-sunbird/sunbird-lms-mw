package org.sunbird.user.actors;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.util.UserActorOperations;

@ActorConfig(
  tasks = {},
  asyncTasks = {"upsertUserExternalIdentityDetails"}
)
public class UserExternalIdManagementActor extends BaseActor {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    if (UserActorOperations.UPSERT_USER_EXTERNAL_IDENTITY_DETAILS
        .getValue()
        .equalsIgnoreCase(request.getOperation())) {
      upsertUserExternalIdentityDetails(request);
    } else {
      ProjectCommonException exception =
          new ProjectCommonException(
              ResponseCode.invalidOperationName.getErrorCode(),
              ResponseCode.invalidOperationName.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }

  private void upsertUserExternalIdentityDetails(Request request) {
    Map<String, Object> requestMap = request.getRequest();
    List<Map<String, String>> externalIds =
        (List<Map<String, String>>) requestMap.get(JsonKey.EXTERNAL_IDS);
    if (CollectionUtils.isNotEmpty(externalIds)) {
      for (Map<String, String> extIdsMap : externalIds) {
        if (StringUtils.isBlank(extIdsMap.get(JsonKey.OPERATION))
            || JsonKey.ADD.equalsIgnoreCase(extIdsMap.get(JsonKey.OPERATION))) {
          upsertUserExternalIdentityData(extIdsMap, requestMap, JsonKey.CREATE);
        }
      }
    }
  }

  private void upsertUserExternalIdentityData(
      Map<String, String> extIdsMap, Map<String, Object> requestMap, String operation) {
    try {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.EXTERNAL_ID, Util.encryptData(extIdsMap.get(JsonKey.ID)));
      map.put(
          JsonKey.ORIGINAL_EXTERNAL_ID,
          Util.encryptData(extIdsMap.get(JsonKey.ORIGINAL_EXTERNAL_ID)));
      map.put(JsonKey.PROVIDER, extIdsMap.get(JsonKey.PROVIDER));
      map.put(JsonKey.ORIGINAL_PROVIDER, extIdsMap.get(JsonKey.ORIGINAL_PROVIDER));
      map.put(JsonKey.ID_TYPE, extIdsMap.get(JsonKey.ID_TYPE));
      map.put(JsonKey.ORIGINAL_ID_TYPE, extIdsMap.get(JsonKey.ORIGINAL_ID_TYPE));
      map.put(JsonKey.USER_ID, requestMap.get(JsonKey.USER_ID));
      if (JsonKey.CREATE.equalsIgnoreCase(operation)) {
        map.put(JsonKey.CREATED_BY, requestMap.get(JsonKey.CREATED_BY));
        map.put(JsonKey.CREATED_ON, new Timestamp(Calendar.getInstance().getTime().getTime()));
      } else {
        map.put(JsonKey.LAST_UPDATED_BY, requestMap.get(JsonKey.UPDATED_BY));
        map.put(JsonKey.LAST_UPDATED_ON, new Timestamp(Calendar.getInstance().getTime().getTime()));
      }
      cassandraOperation.upsertRecord(JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, map);
    } catch (Exception ex) {
      ProjectLogger.log("Util:upsertUserExternalIdentityData : Exception occurred", ex);
    }
  }
}
