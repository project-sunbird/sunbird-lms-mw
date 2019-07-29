package org.sunbird.user.actors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import java.util.List;
import java.util.Map;


@ActorConfig(
        tasks = {"freeUpUserIdentity"},
        asyncTasks = {}
)

/**
 * this Actor class is being used to free Up used User Identifier
 * for now it only free Up user Email, Phone.
 */
public class IdentifierFreeUpActor extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    public static final int FIRST_RECORD = 0;

    @Override
    public void onReceive(Request request) {
        String id = (String) request.get(JsonKey.ID);
        List<String> identifiers = (List) request.get(JsonKey.IDENTIFIER);
        freeUpUserIdentifier(id, identifiers);
    }

    private Map<String, Object> getUserById(String id) {
        Response response = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), id);
        List<Map<String, Object>> responseList = (List) response.get(JsonKey.RESPONSE);
        if (CollectionUtils.isEmpty(responseList)) {
            ProjectLogger.log(String.format("%s:%s:User not found with provided Id:%s", this.getClass().getSimpleName(), "getById", id), LoggerEnum.ERROR.name());
            ProjectCommonException.throwClientErrorException(ResponseCode.invalidUserId);
        }
        return responseList.get(FIRST_RECORD);
    }

    private Response processUserAttribute(Map<String, Object> userDbMap, List<String> identifiers) {

        if (identifiers.contains(JsonKey.EMAIL)) {
            nullifyEmail(userDbMap);
            ProjectLogger.log(String.format("%s:%s:Nullified Email. WITH ID  %s", this.getClass().getSimpleName(), "freeUpUserIdentifier", userDbMap.get(JsonKey.ID)), LoggerEnum.INFO.name());
        }
        if (identifiers.contains(JsonKey.PHONE)) {
            nullifyPhone(userDbMap);
            ProjectLogger.log(String.format("%s:%s:Nullified Phone. WITH ID  %s", this.getClass().getSimpleName(), "freeUpUserIdentifier", userDbMap.get(JsonKey.ID)), LoggerEnum.INFO.name());
        }
        return updateUser(userDbMap);
    }

    private void nullifyEmail(Map<String, Object> userDbMap) {
        if (isNullifyOperationValid((String) userDbMap.get(JsonKey.EMAIL))) {
            userDbMap.replace(JsonKey.PREV_USED_EMAIL, userDbMap.get(JsonKey.EMAIL));
            userDbMap.replace(JsonKey.EMAIL, null);
            userDbMap.replace(JsonKey.MASKED_EMAIL, null);
            userDbMap.replace(JsonKey.EMAIL_VERIFIED, false);
        }
    }

    private void nullifyPhone(Map<String, Object> userDbMap) {
        if (isNullifyOperationValid((String) userDbMap.get(JsonKey.PHONE))) {
            userDbMap.replace(JsonKey.PREV_USED_PHONE, userDbMap.get(JsonKey.PHONE));
            userDbMap.replace(JsonKey.PHONE, null);
            userDbMap.replace(JsonKey.MASKED_PHONE, null);
            userDbMap.replace(JsonKey.PHONE_VERIFIED, false);
            userDbMap.replace(JsonKey.COUNTRY_CODE, null);
        }
    }

    private Response updateUser(Map<String, Object> userDbMap) {
        return cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userDbMap);
    }

    private void freeUpUserIdentifier(String id, List<String> identifiers) {
        Map<String, Object> userDbMap = getUserById(id);
        Response response = processUserAttribute(userDbMap, identifiers);
        sender().tell(response, self());
        ProjectLogger.log(String.format("%s:%s:USER MAP SUCCESSFULLY UPDATED IN CASSANDRA. WITH ID  %s", this.getClass().getSimpleName(), "freeUpUserIdentifier", userDbMap.get(JsonKey.ID)), LoggerEnum.INFO.name());
        saveUserDetailsToEs(userDbMap);
    }

    private void saveUserDetailsToEs(Map<String, Object> userDbMap) {
        Request userUpdatedRequest = new Request();
        userUpdatedRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
        userUpdatedRequest.getRequest().put(JsonKey.ID, userDbMap.get(JsonKey.ID));
        ProjectLogger.log(String.format("%s:%s:Trigger for sync  user details to ES with user updated user Id:%s", this.getClass().getSimpleName(), "saveUserDetailsToEs", userDbMap.get(JsonKey.ID)), LoggerEnum.INFO.name());
        tellToAnother(userUpdatedRequest);
    }

    private boolean isNullifyOperationValid(String identifier) {
        return StringUtils.isNotBlank(identifier);
    }
}