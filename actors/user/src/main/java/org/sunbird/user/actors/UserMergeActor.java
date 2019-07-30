package org.sunbird.user.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.models.user.User;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

import java.io.IOException;
import java.util.*;

@ActorConfig(
        tasks = {"mergeUser"},
        asyncTasks = {}
)
public class UserMergeActor extends UserBaseActor {
    private ObjectMapper objectMapper = new ObjectMapper();
    private UserService userService = UserServiceImpl.getInstance();
    @Override
    public void onReceive(Request userRequest) throws Throwable {
        updateUserMergeDetails(userRequest);
    }

    private void updateUserMergeDetails(Request userRequest) throws IOException {
        ProjectLogger.log(
                "UserMergeActor:updateUserMergeDetails: starts : " ,
                LoggerEnum.DEBUG.name());
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();
        Response response = new Response();
        Map mergeeESMap = new HashMap<String, Object>();
        Map mergeeDBMap = new HashMap<String, Object>();
        Map requestMap = userRequest.getRequest();
        String mergeeId = (String)requestMap.get(JsonKey.FROM_ACCOUNT_ID);
        String mergerId = (String)requestMap.get(JsonKey.TO_ACCOUNT_ID);
        User mergee = userService.getUserById(mergeeId);
        if(!mergee.getIsDeleted()) {
            mergeeDBMap.put(JsonKey.STATUS, 0);
            mergeeDBMap.put(JsonKey.IS_DELETED, true);
            mergeeDBMap.put(JsonKey.EMAIL,null);
            mergeeDBMap.put(JsonKey.PHONE,null);
            mergeeDBMap.put(JsonKey.USERNAME, null);
            mergeeDBMap.put("prevUsedEmail", mergee.getEmail());
            mergeeDBMap.put("prevUsedPhone", mergee.getPhone());
            mergeeDBMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            mergeeDBMap.put(JsonKey.ID,mergee.getId());

            mergeeESMap.put(JsonKey.STATUS, 0);
            mergeeESMap.put(JsonKey.IS_DELETED, true);
            mergeeESMap.put(JsonKey.EMAIL,null);
            mergeeESMap.put(JsonKey.PHONE,null);
            mergeeESMap.put(JsonKey.USERNAME, null);
            mergeeESMap.put("prevUsedEmail", mergee.getEmail());
            mergeeESMap.put("prevUsedPhone", mergee.getPhone());
            mergeeESMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            mergeeESMap.put(JsonKey.USER_ID, mergeeId);
            userRequest.put("userFromAccount", mergeeESMap);

            //update the merger-course details in cassandra & ES
            String mergerCourseResponse = updateMergerCourseDetails(requestMap);

            if(mergerCourseResponse.equalsIgnoreCase(JsonKey.SUCCESS)) {
                Response mergeeResponse = getUserDao().updateUserFieldsWithNullValues(mergeeDBMap);
                String mergeeResponseStr = (String) mergeeResponse.get(JsonKey.RESPONSE);
                ProjectLogger.log("UserMergeActor:UserMergeActor: mergeeResponseStr = " + mergeeResponseStr);
                Map result = new HashMap<String, Object>();
                result.put(JsonKey.STATUS,JsonKey.SUCCESS);
                response.put(JsonKey.RESULT,result);
                //update mergee details in ES
                sender().tell(response, self());
                mergeUserDetailsToEs(userRequest);

                //create telemetry event for merge
            /*targetObject =
                    TelemetryUtil.generateTargetObject(
                            (String) userRequest.getRequest().get(mergeeId), TelemetryEnvKey.USER, JsonKey.MERGE, null);
            correlatedObject.add(mergeeESMap);
            TelemetryUtil.telemetryProcessingCall(userRequest.getRequest(),targetObject,correlatedObject);*/
            } else {
                ProjectLogger.log(
                        "UserMergeActor:updateUserMergeDetails: User course data is not updated for userId : " + mergerId,
                        LoggerEnum.ERROR.name());
            }
        } else {
            ProjectLogger.log(
                    "UserMergeActor:updateUserMergeDetails: User mergee is not exist : " + mergeeId,
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.mergeeIdNotExists.getErrorCode(),
                    ProjectUtil.formatMessage(
                            ResponseCode.mergeeIdNotExists.getErrorMessage(), mergeeId),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }


    }

    private void mergeUserDetailsToEs(Request userRequest) {
        userRequest.setOperation(ActorOperations.MERGE_USERS_TO_ELASTIC.getValue());
        ProjectLogger.log(String.format("%s:%s:Trigger sync of user details to ES with user updated userMap %s", this.getClass().getSimpleName(), "mergeUserDetailsToEs", Collections.singleton(userRequest.toString())));
        tellToAnother(userRequest);
    }

    private String updateMergerCourseDetails(Map<String, Object> requestMap) throws IOException {
        //call course service api
        String bodyJson = objectMapper.writeValueAsString(requestMap);
        Map headersMap = new HashMap();
        String responseCode = null;
        //courseurl need to provide
        /*HttpUtilResponse httpResponse = HttpUtil.doPostRequest("courseurl", bodyJson, headersMap);
        if (httpResponse.getStatusCode() == ResponseCode.OK.getResponseCode()) {
            String responseStr = httpResponse.getBody();
            if (responseStr != null) {
                Map<String, Object> responseMap = objectMapper.readValue(responseStr, HashMap.class);
                responseCode =  (String) responseMap.get(JsonKey.RESPONSE_CODE);
            }
        }*/
        return responseCode = "SUCCESS";
    }

}
