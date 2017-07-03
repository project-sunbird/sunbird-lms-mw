package org.sunbird.learner.actors.recommend;

import akka.actor.UntypedAbstractActor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.Util;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Class to perform the recommended operations like recommended courses etc.
 * @author arvind
 */
public class RecommendorActor extends UntypedAbstractActor {

    private LogHelper logger = LogHelper.getInstance(RecommendorActor.class.getName());
    private CassandraOperation cassandraOperation = new CassandraOperationImpl();


    /**
     * @param message
     * @throws Throwable
     */
    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Request) {
            logger.info("RecommendorActor  onReceive called");
            Request request = (Request) message;
            if (request.getOperation().equalsIgnoreCase(ActorOperations.GET_RECOMMENDED_COURSES.getValue())) {
                Response response = new Response();
                logger.info("RecommendorActor  -- GET Recommended courses");
                String type = (String) request.getRequest().get(JsonKey.RECOMMEND_TYPE);
                if (JsonKey.COURSE.equalsIgnoreCase(type)) {
                    Map<String, List<Map<String, Object>>> esresponse = getRecommendedCourses(request);
                    response.put(JsonKey.RESPONSE, esresponse);
                    sender().tell(response, self());
                } else if (JsonKey.CONTENT.equalsIgnoreCase(type)) {
                    Object ekStepResponse = getRecommendedContents(request);
                    response.put(JsonKey.RESPONSE, ekStepResponse);
                    sender().tell(response, self());
                } else {
                    logger.info("UNSUPPORTED OPERATION");
                    ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                    sender().tell(exception, self());
                }
            } else {
                logger.info("UNSUPPORTED OPERATION");
                ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, self());
            }
        } else {
            // Throw exception as message body
            logger.info("UNSUPPORTED MESSAGE");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
        }
    }

    /**
     * method to get the recommended contents from EkStep on basis of user language,grade and subbject .
     * @param request
     * @return
     */
    private Object getRecommendedContents(Request request) {

        Map<String, Object> req = request.getRequest();
        Map<String, List<Map<String, Object>>> response = null;
        String userId = (String) req.get(JsonKey.REQUESTED_BY);
        //get language , grade , subject on basis of UserId
        Map<String, Object> map = getUserInfo(userId);
        Map<String, Object> searchQueryMap = new HashMap<String, Object>();
        Map<String, Object> filterMap = new HashMap<String, Object>();

        if (map != null) {
            if (map.get(JsonKey.LANGUAGE) != null) {
                filterMap.put(JsonKey.LANGUAGE, map.get(JsonKey.LANGUAGE));
            }
            if (map.get(JsonKey.GRADE_LEVEL) != null) {
                filterMap.put(JsonKey.GRADE_LEVEL, map.get(JsonKey.GRADE_LEVEL));
            }
            if (map.get(JsonKey.SUBJECT) != null) {
                filterMap.put(JsonKey.SUBJECT, map.get(JsonKey.SUBJECT));
            }
        }

        // add object type and content type in query filter
        filterMap.put(JsonKey.OBJECT_TYPE , JsonKey.CONTENT);
        List<String> contentTypes = new ArrayList<String>();
        contentTypes.add("story");
        contentTypes.add("Worksheet");
        contentTypes.add("Game");
        contentTypes.add("TextBook");
        contentTypes.add("Simulation");
        contentTypes.add("Puzzle");
        contentTypes.add("Diagnostic");
        contentTypes.add("Collection");
        filterMap.put(JsonKey.CONTENT_TYPE , contentTypes);

        searchQueryMap.put(JsonKey.FILTERS, filterMap);
        searchQueryMap.put(JsonKey.LIMIT , 20);

        Map<String, Object> ekStepSearchQuery = new HashMap<String, Object>();
        ekStepSearchQuery.put(JsonKey.REQUEST, searchQueryMap);

        String json = null;
        try {
            json = new ObjectMapper().writeValueAsString(ekStepSearchQuery);
            Map<String, Object> query = new HashMap<String, Object>();
            query.put(JsonKey.SEARCH_QUERY, json);
            Util.getContentData(query);
            return shuffleList(Arrays.stream((Object[])query.get(JsonKey.CONTENTS)).collect(Collectors.toList()));
        } catch (JsonProcessingException e) {
            logger.error(e);
        }
        return null;
    }

    private Map<String, List<Map<String, Object>>> getRecommendedCourses(Request request) {

        Map<String, Object> req = request.getRequest();
        Map<String, List<Map<String, Object>>> response = null;
        String userId = (String) req.get(JsonKey.REQUESTED_BY);
        //get language , grade , subject on basis of UserId
        Map<String, Object> map = getUserInfo(userId);
        Map<String, Object> searchQueryMap = new HashMap<String, Object>();
        Map<String, Object> filterMap = new HashMap<String, Object>();

        if (map != null) {
            if (map.get(JsonKey.LANGUAGE) != null) {
                filterMap.put(JsonKey.LANGUAGE, map.get(JsonKey.LANGUAGE));
            }
            if (map.get(JsonKey.GRADE_LEVEL) != null) {
                filterMap.put(JsonKey.GRADE, map.get(JsonKey.GRADE));
            }
            if (map.get(JsonKey.SUBJECT) != null) {
                filterMap.put(JsonKey.SUBJECT, map.get(JsonKey.SUBJECT));
            }
        }

        searchQueryMap.put(JsonKey.FILTERS, filterMap);
        searchQueryMap.put(JsonKey.LIMIT , 20);
        SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
        response = ElasticSearchUtil.complexSearch(searchDto, ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.course.getTypeName());
        response.put(JsonKey.RESPONSE , shuffleList((List)response.get(JsonKey.RESPONSE)));
        return response;
    }

    private Map<String, Object> getUserInfo(String userId) {

        Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Map<String, Object> map = new HashMap<String, Object>();

        Response response = cassandraOperation.getRecordById(userdbInfo.getKeySpace(), userdbInfo.getTableName(), userId);
        if (response.getResult() != null) {
            if (response.getResult().get(JsonKey.RESPONSE) != null) {
                List<Map<String, Object>> userDBO = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
                if (userDBO != null && userDBO.size() > 0) {
                    //TODO: get the subject,grade , language from userDBO(database object) and create the response ...
                }
            }
        }

        List<String> languages = new ArrayList<String>();
        languages.add("English");
        map.put(JsonKey.LANGUAGE, languages);

        //TODO: subbject is not available in ES need discussion
        List<String> subjects = new ArrayList<String>();
        subjects.add("Physics");
        subjects.add("Math");
        //map.put(JsonKey.SUBJECT, subjects);


        List<String> grades = new ArrayList<String>();
        grades.add("Grade 1");
        map.put(JsonKey.GRADE_LEVEL, grades);

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
