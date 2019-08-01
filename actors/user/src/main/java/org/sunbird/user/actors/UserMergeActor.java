package org.sunbird.user.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.models.user.User;
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
        Map mergeeDBMap = new HashMap<String, Object>();
        Map requestMap = userRequest.getRequest();
        String mergeeId = (String)requestMap.get(JsonKey.FROM_ACCOUNT_ID);
        String mergerId = (String)requestMap.get(JsonKey.TO_ACCOUNT_ID);
        User mergee = userService.getUserById(mergeeId);
        if(!mergee.getIsDeleted()) {
            prepareMergeeAccountData(mergee, mergeeDBMap);
            userRequest.put(JsonKey.USER_MERGEE_ACCOUNT, mergeeDBMap);

            //update the merger-course details in cassandra & ES
            String mergerCourseResponse = updateMergerCourseDetails(requestMap);
            if(mergerCourseResponse.equalsIgnoreCase(JsonKey.SUCCESS)) {
                Response mergeeResponse = getUserDao().updateUser(mergeeDBMap);
                String mergeeResponseStr = (String) mergeeResponse.get(JsonKey.RESPONSE);
                ProjectLogger.log("UserMergeActor: updateUserMergeDetails: mergeeResponseStr = " + mergeeResponseStr,LoggerEnum.INFO.name());
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
                throw new ProjectCommonException(
                        ResponseCode.internalError.getErrorCode(),
                        ProjectUtil.formatMessage(
                                ResponseMessage.Message.INTERNAL_ERROR,
                                mergeeId),
                        ResponseCode.SERVER_ERROR.getResponseCode());
            }
        } else {
            ProjectLogger.log(
                    "UserMergeActor:updateUserMergeDetails: User mergee is not exist : " + mergeeId,
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidIdentifier.getErrorCode(),
                    ProjectUtil.formatMessage(
                            ResponseMessage.Message.INVALID_PARAMETER_VALUE,
                            mergeeId,
                            JsonKey.FROM_ACCOUNT_ID),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }


    }

    private void mergeUserDetailsToEs(Request userRequest) {
        userRequest.setOperation(ActorOperations.MERGE_USER_TO_ELASTIC.getValue());
        ProjectLogger.log("UserMergeActor: mergeUserDetailsToEs: Trigger sync of user details to ES for user id"+userRequest.getRequest().get(JsonKey.FROM_ACCOUNT_ID),
                LoggerEnum.INFO.name());
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

    private void prepareMergeeAccountData(User mergee, Map mergeeDBMap) {
        mergeeDBMap.put(JsonKey.STATUS, 0);
        mergeeDBMap.put(JsonKey.IS_DELETED, true);
        mergeeDBMap.put(JsonKey.EMAIL,null);
        mergeeDBMap.put(JsonKey.PHONE,null);
        mergeeDBMap.put(JsonKey.USERNAME, null);
        mergeeDBMap.put(JsonKey.PREV_USED_EMAIL, mergee.getEmail());
        mergeeDBMap.put(JsonKey.PREV_USED_PHONE, mergee.getPhone());
        mergeeDBMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        mergeeDBMap.put(JsonKey.ID,mergee.getId());
    }

}
