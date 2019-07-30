package org.sunbird.user.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.actor.router.ActorConfig;
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

    private ObjectMapper objectMapper;
    private UserService userService = UserServiceImpl.getInstance();
    @Override
    public void onReceive(Request userRequest) throws Throwable {
        updateUserMergeDetails(userRequest);
    }

    private void updateUserMergeDetails(Request userRequest) throws IOException {
        Map<String, Object> targetObject = null;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();
        Map mergeeMap = new HashMap<String, Object>();
        Map requestMap = userRequest.getRequest();
        String mergeeId = (String)requestMap.get("mergeeId");
        String mergerId = (String)requestMap.get("mergerId");
        User mergee = userService.getUserById(mergeeId);

        mergee.setStatus(0);
        mergee.setIsDeleted(true);
        mergee.setPrevUsedEmail(mergee.getEmail());
        mergee.setPrevUsedPhone(mergee.getPhone());
        mergee.setEmail(null);
        mergee.setPhone(null);
        mergee.setUserName(null);
        mergee.setUpdatedDate(ProjectUtil.getFormattedDate());

        mergeeMap.put("status", 0);
        mergeeMap.put("isDeleted", true);
        mergeeMap.put("email",null);
        mergeeMap.put("phone",null);
        mergeeMap.put("userName", null);
        mergeeMap.put("prevUsedEmail", mergee.getPrevUsedEmail());
        mergeeMap.put("prevUsedPhone", mergee.getPrevUsedPhone());
        mergeeMap.put("updatedDate", mergee.getUpdatedDate());
        mergeeMap.put(JsonKey.USER_ID, mergeeId);
        userRequest.put("userFromAccount", mergeeMap);

        //update the merger-course details in cassandra & ES
        String mergerCourseResponse = updateMergerCourseDetails(requestMap);

        if(mergerCourseResponse.equalsIgnoreCase("SUCCESS")) {
            Response mergeeResponse = getUserDao().updateUser(mergee);
            String mergeeResponseStr = (String) mergeeResponse.get(JsonKey.RESPONSE);
            ProjectLogger.log("UserMergeActor:UserMergeActor: mergeeResponseStr = " + mergeeResponseStr);

            //update mergee details in ES
            mergeUserDetailsToEs(userRequest);

            //create telemetry event for merge
            targetObject =
                    TelemetryUtil.generateTargetObject(
                            (String) userRequest.getRequest().get(mergeeId), TelemetryEnvKey.USER, JsonKey.MERGE, null);
            correlatedObject.add(mergeeMap);
            TelemetryUtil.telemetryProcessingCall(userRequest.getRequest(),targetObject,correlatedObject);
        } else {
            ProjectLogger.log(
                    "UserMergeActor:updateUserMergeDetails: User course data is not updated for userId : " + mergerId,
                    LoggerEnum.ERROR.name());
        }

    }

    private void mergeUserDetailsToEs(Request userRequest) {
        userRequest.setOperation(ActorOperations.MERGE_USERS_TO_ELASTIC.getValue());
        ProjectLogger.log(String.format("%s:%s:Trigger sync of user details to ES with user updated userMap %s", this.getClass().getSimpleName(), "mergeUserDetailsToEs", Collections.singleton(userRequest.toString())));
        tellToAnother(userRequest);
    }

    private String updateMergerCourseDetails(Map<String, Object> requestMap) throws IOException {
        //call course service api
        objectMapper = new ObjectMapper();
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
