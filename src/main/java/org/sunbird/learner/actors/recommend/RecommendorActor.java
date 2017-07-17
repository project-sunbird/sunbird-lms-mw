package org.sunbird.learner.actors.recommend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.UntypedAbstractActor;

/**
 * Class to perform the recommended operations like recommended courses etc.
 * @author arvind
 */
public class RecommendorActor extends UntypedAbstractActor {

    private CassandraOperation cassandraOperation = new CassandraOperationImpl();

    /**
     * @param message
     * @throws Throwable
     */
    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Request) {
            try {
                ProjectLogger.log("RecommendorActor  onReceive called");
                Request request = (Request) message;
                if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_RECOMMENDED_COURSES.getValue())) {
                    ProjectLogger.log("RecommendorActor  -- GET Recommended courses");
                    getRecommendedCourses(request);
                } else {
                    ProjectLogger.log("UNSUPPORTED OPERATION");
                    ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                    sender().tell(exception, self());
                }
            }catch(Exception ex) {
                ProjectLogger.log(ex.getMessage(), ex);
                sender().tell(ex , self());
            }
        } else {
            // Throw exception as message body
            ProjectLogger.log("UNSUPPORTED MESSAGE");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
        }
    }

    /**
     * method to get the recommended contents from EkStep on basis of user language,grade and subbject .
     * @param request
     * @return
     */
    private void getRecommendedCourses(Request request) {

        Response response = new Response();
        Map<String, Object> req = request.getRequest();
        String userId = (String) req.get(JsonKey.REQUESTED_BY);
        //get language , grade , subject on basis of UserId
        Map<String, Object> map = getUserInfo(userId);
        if(null == map){
            ProjectLogger.log("USER ID DOES NOT EXIST");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidUserCredentials.getErrorCode(), ResponseCode.invalidUserCredentials.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
        Map<String, Object> searchQueryMap = new HashMap<String, Object>();
        Map<String, Object> filterMap = new HashMap<String, Object>();

        if (map != null) {
            if (map.get(JsonKey.LANGUAGE) != null) {
                filterMap.put(JsonKey.LANGUAGE, map.get(JsonKey.LANGUAGE));
            }
            if (map.get(JsonKey.GRADE) != null) {
                filterMap.put(JsonKey.GRADE_LEVEL, map.get(JsonKey.GRADE));
            }
            if (map.get(JsonKey.SUBJECT) != null) {
                filterMap.put(JsonKey.SUBJECT, map.get(JsonKey.SUBJECT));
            }
        }

        // add object type and content type in query filter
        filterMap.put(JsonKey.OBJECT_TYPE , JsonKey.CONTENT);
        List<String> contentTypes = new ArrayList<String>();
        contentTypes.add(JsonKey.COURSE);
        filterMap.put(JsonKey.CONTENT_TYPE , contentTypes);

        searchQueryMap.put(JsonKey.FILTERS, filterMap);
        searchQueryMap.put(JsonKey.LIMIT , 10);

        Map<String, Object> ekStepSearchQuery = new HashMap<String, Object>();
        ekStepSearchQuery.put(JsonKey.REQUEST, searchQueryMap);

        String json = null;
        try {
            json = new ObjectMapper().writeValueAsString(ekStepSearchQuery);
            Map<String, Object> query = new HashMap<String, Object>();
            query.put(JsonKey.SEARCH_QUERY, json);
            Util.getContentData(query);
            response.put(JsonKey.RESPONSE ,shuffleList(Arrays.stream((Object[])query.get(JsonKey.CONTENTS)).collect(Collectors.toList())));
            sender().tell(response , self());
            return;
        } catch (JsonProcessingException e) {
            ProjectLogger.log(e.getMessage() , e);
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.internalError.getErrorCode(), ResponseCode.internalError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
            sender().tell(exception, self());
            return;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getUserInfo(String userId) {

        Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String, Object> map = new HashMap<String, Object>();

        List<String> languages = new ArrayList<String>();
        List<String> subjects = new ArrayList<String>();
        List<String> grades = new ArrayList<String>();

        Response response = cassandraOperation.getRecordById(userdbInfo.getKeySpace(), userdbInfo.getTableName(), userId);
        if (response.getResult() != null) {
            if (response.getResult().get(JsonKey.RESPONSE) != null) {
                List<Map<String, Object>> userDBO = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
                if (userDBO != null && userDBO.size() > 0) {
                    //TODO: get the subject,grade , language from userDBO(database object) and create the response ...
                    Map<String, Object> userInfo = (Map<String, Object>)userDBO.get(0);
                    if(userInfo.get(JsonKey.SUBJECT)!= null){
                        subjects = (List<String>)userInfo.get(JsonKey.SUBJECT);
                        map.put(JsonKey.SUBJECT, subjects);
                    }
                    if(userInfo.get(JsonKey.LANGUAGE)!= null){
                        languages = (List<String>)userInfo.get(JsonKey.LANGUAGE);
                        map.put(JsonKey.LANGUAGE , languages);
                    }
                    if(userInfo.get(JsonKey.GRADE)!= null){
                        grades = (List<String>)userInfo.get(JsonKey.GRADE);
                        map.put(JsonKey.GRADE , grades);
                    }
                }else{
                    return null;
                }
            }
        }

        return map;

    }

    private List<Object> shuffleList(List<Object> list){
        if(list.size()<=10){
            return list;
        }
        Collections.shuffle(list);
        return list.subList(0,10);
    }
}
