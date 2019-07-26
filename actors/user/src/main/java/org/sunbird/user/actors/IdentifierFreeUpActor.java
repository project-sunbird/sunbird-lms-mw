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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@ActorConfig(
        tasks = {"freeUpUserIdentity"},
        asyncTasks = {}
)
public class IdentifierFreeUpActor extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    private Map<String, Object> userDbMap = new HashMap<>();
    public static final int FIRST_RECORD = 0;

    @Override
    public void onReceive(Request request) {
        String id = (String) request.get(JsonKey.ID);
        List<String> identifiers = (List) request.get(JsonKey.IDENTIFIER);
        freeUpUser(id, identifiers);
    }

    private Map<String, Object> getUserById(String id) {
        Response response = cassandraOperation.getRecordById(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), id);
        List<Map<String, Object>> responseList = (List) response.get(JsonKey.RESPONSE);
        if (CollectionUtils.isEmpty(responseList)) {
            ProjectCommonException.throwClientErrorException(ResponseCode.invalidUserId);
        }
        return responseList.get(FIRST_RECORD);
    }

    private Response processUserAttribute(List<String> identifiers) {

        identifiers.forEach(val -> {
            if (val.equalsIgnoreCase(JsonKey.EMAIL)) {
                nullifyEmail();
            } else if (val.equalsIgnoreCase(JsonKey.PHONE)) {
                nullifyPhone();
            }
        });
        return updateUser();
    }

    private void nullifyEmail() {
        if (isNullifyOperationValid((String) userDbMap.get(JsonKey.EMAIL))) {
            userDbMap.replace(JsonKey.PREV_USED_EMAIL, userDbMap.get(JsonKey.EMAIL));
            userDbMap.replace(JsonKey.EMAIL, null);
            userDbMap.replace(JsonKey.MASKED_EMAIL, null);
            userDbMap.replace(JsonKey.EMAIL_VERIFIED, false);
        }
    }

    private void nullifyPhone() {
        if (isNullifyOperationValid((String) userDbMap.get(JsonKey.PHONE))) {
            userDbMap.replace(JsonKey.PREV_USED_PHONE, userDbMap.get(JsonKey.PHONE));
            userDbMap.replace(JsonKey.PHONE, null);
            userDbMap.replace(JsonKey.MASKED_PHONE, null);
            userDbMap.replace(JsonKey.PHONE_VERIFIED, false);
        }
    }

    private Response updateUser() {
        return cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), userDbMap);
    }

    private void freeUpUser(String id, List<String> identifiers) {
        userDbMap = getUserById(id);
        Response response = processUserAttribute(identifiers);
        sender().tell(response, self());
        ProjectLogger.log(String.format("%s:%s:USER MAP SUCCESSFULLY UPDATED IN CASSANDRA. WITH VALUES  %s", this.getClass().getSimpleName(), "freeUpUser", Collections.singleton(userDbMap.toString())), LoggerEnum.INFO.name());
        saveUserDetailsToEs();
    }

    private void saveUserDetailsToEs() {
        Request userUpdatedRequest = new Request();
        userUpdatedRequest.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
        userUpdatedRequest.getRequest().put(JsonKey.ID, userDbMap.get(JsonKey.ID));
        ProjectLogger.log(String.format("%s:%s:Trigger sync of user details to ES with user updated userMap %s", this.getClass().getSimpleName(), "saveUserDetailsToEs", Collections.singleton(userDbMap.toString())), LoggerEnum.INFO.name());
        tellToAnother(userUpdatedRequest);
    }

    private boolean isNullifyOperationValid(String identifier){
        return StringUtils.isNotBlank(identifier);
    }
}