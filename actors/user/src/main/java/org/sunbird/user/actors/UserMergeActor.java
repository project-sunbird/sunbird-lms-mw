package org.sunbird.user.actors;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.User;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

import java.util.*;

@ActorConfig(
        tasks = {"mergeUser"},
        asyncTasks = {}
)
public class UserMergeActor extends UserBaseActor {

    private UserService userService = UserServiceImpl.getInstance();
    private final SSOManager ssoManager = SSOServiceFactory.getInstance();
    @Override
    public void onReceive(Request userRequest) throws Throwable {
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();
        Map mergeeMap = new HashMap<String, Object>();
        Map mergerMap = new HashMap<String, Object>();
        Map requestMap = userRequest.getRequest();
        String mergeeId = (String)requestMap.get("mergeeId");
        String mergerId = (String)requestMap.get("mergerId");
        User mergee = userService.getUserById(mergeeId);
        User merger = userService.getUserById(mergerId);
        merger.setEmail(mergee.getEmail());
        merger.setPhone(mergee.getPhone());
        merger.setUserName(mergee.getUserName());
        mergee.setStatus(0);
        mergee.setIsDeleted(true);
        //Util.getUserbyUserId();
        //cassandraOperation.getRecordById("keySpace","tableName","userId");
        //update cassandra and elastic search

        mergerMap.put("email", mergee.getEmail());
        mergerMap.put("phone", mergee.getPhone());
        mergerMap.put("userName", mergee.getUserName());
        mergeeMap.put("status", 0);
        mergeeMap.put("isDeleted", true);
        mergeeMap.put(JsonKey.USER_ID, mergeeId);
        userRequest.put("userFromAccount", mergeeMap);
        userRequest.put("userToAccount", mergerMap);
        Response mergeeResponse = getUserDao().updateUser(mergee);
        Response mergerResponse = getUserDao().updateUser(merger);
        String mergeeResponseStr = (String) mergeeResponse.get(JsonKey.RESPONSE);
        String mergerResponseStr = (String) mergerResponse.get(JsonKey.RESPONSE);
        ProjectLogger.log("UserMergeActor:UserMergeActor: mergeeResponseStr = " + mergeeResponseStr + "mergerResponseStr =" +mergerResponseStr);
        mergeUsersDetailsToEs(userRequest);
        //remove the mergee user
        ssoManager.removeUser(mergeeMap);
        targetObject =
                TelemetryUtil.generateTargetObject(
                        (String) userRequest.getRequest().get(JsonKey.USER_ID), TelemetryEnvKey.USER, JsonKey.MERGE, null);
        correlatedObject.add(mergeeMap);
        correlatedObject.add(mergerMap);
        TelemetryUtil.telemetryProcessingCall(userRequest.getRequest(),targetObject,correlatedObject);


    }

    private void mergeUsersDetailsToEs(Request userRequest) {
        userRequest.setOperation(ActorOperations.MERGE_USERS_TO_ELASTIC.getValue());
        ProjectLogger.log(String.format("%s:%s:Trigger sync of user details to ES with user updated userMap %s", this.getClass().getSimpleName(), "mergeUsersDetailsToEs", Collections.singleton(userRequest.toString())));
        tellToAnother(userRequest);
    }

}
