package org.sunbird.learner.actors;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

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

import akka.actor.ActorRef;
import akka.actor.UntypedAbstractActor;

/**
 * This actor will handle leaner's state operation like get course , get content etc.
 *
 * @author Manzarul
 */
public class LearnerStateActor extends UntypedAbstractActor {

    private CassandraOperation cassandraOperation = new CassandraOperationImpl();

    /**
     * Receives the actor message and perform the operation like get course , get content etc.
     * @param message Object
     * @throws Exception
     */
    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Request) {
            try {
                ProjectLogger.log("LearnerStateActor onReceive called");
                Request actorMessage = (Request) message;
                if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_COURSE.getValue())) {
                    String userId = (String) actorMessage.getRequest().get(JsonKey.USER_ID);

                    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
                    Response result = cassandraOperation.getRecordsByProperty(dbInfo.getKeySpace(), dbInfo.getTableName(), JsonKey.USER_ID, userId);
                    removeUnwantedProperties(result);
                    sender().tell(result, self());

                } else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_CONTENT.getValue())) {
                    String userId = (String) actorMessage.getRequest().get(JsonKey.USER_ID);
                    Response res = new Response();
                    if (actorMessage.getRequest().get(JsonKey.COURSE) != null) {
                        res = getContentByCourse(userId, actorMessage.getRequest());
                    } else if (actorMessage.getRequest().get(JsonKey.CONTENT_IDS) != null) {
                        res = getContentByContents(userId, actorMessage.getRequest());
                    } else if (actorMessage.getRequest().get(JsonKey.COURSE_IDS) != null) {
                        res = getContentByCourses(userId, actorMessage.getRequest());
                    }
                    removeUnwantedProperties(res);
                    sender().tell(res, self());
                } else {
                    ProjectLogger.log("UNSUPPORTED OPERATION");
                    ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                    sender().tell(exception, ActorRef.noSender());
                }
            }catch(Exception ex){
                ProjectLogger.log(ex.getMessage(), ex);
                sender().tell(ex, ActorRef.noSender());
            }

        } else {
            ProjectLogger.log("UNSUPPORTED MESSAGE");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, ActorRef.noSender());
        }

    }

    /**
     * Method to get content on basis of course ids.
     * @param userId String
     * @param request  Map<String, Object>
     * @return Response
     */
    @SuppressWarnings("unchecked")
	private Response getContentByCourses(String userId, Map<String, Object> request) {

        Response response = new Response();
        Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
        CopyOnWriteArrayList<String> courseIds = new CopyOnWriteArrayList<String>((List<String>) request.get(JsonKey.COURSE_IDS));
        List<Map<String, Object>> modifiedContentList = new ArrayList<Map<String, Object>>();

        LinkedHashMap<String , Object> queryMap = new LinkedHashMap<String , Object>();
        queryMap.put(JsonKey.USER_ID , userId);

        for(String courseid : courseIds){
            queryMap.put(JsonKey.COURSE_ID , courseid);
            response = cassandraOperation.getRecordsByProperties(dbInfo.getKeySpace() , dbInfo.getTableName() , queryMap);
            modifiedContentList.addAll((List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE));
        }
        response.getResult().put(JsonKey.RESPONSE, modifiedContentList);
        return response;
    }

    /**
     * Method to get content on basis of content ids.
     * @param userId
     * @param request
     * @return
     */
    @SuppressWarnings("unchecked")
	private Response getContentByContents(String userId, Map<String, Object> request) {
        Response response = null;
        Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
        CopyOnWriteArrayList<String> contentIds = new CopyOnWriteArrayList<String>((List<String>) request.get(JsonKey.CONTENT_IDS));

        LinkedHashMap<String , Object> queryMap = new LinkedHashMap<String , Object>();
        queryMap.put(JsonKey.USER_ID , userId);

        List<Map<String, Object>> modifiedContentList = new ArrayList<Map<String, Object>>();

        for(String contentid : contentIds){
            queryMap.put(JsonKey.CONTENT_ID , contentid);
            response = cassandraOperation.getRecordsByProperties(dbInfo.getKeySpace(), dbInfo.getTableName(), queryMap);
            modifiedContentList.addAll((List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE));

        }

        response.getResult().put(JsonKey.RESPONSE, modifiedContentList);

        return response;
    }

    /**
     * Method to get all contents of a given course.
     * @param userId
     * @param request
     * @return
     */
    @SuppressWarnings("unchecked")
	private Response getContentByCourse(String userId, Map<String, Object> request) {

        Response response = null;
        Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);

        if (request.get(JsonKey.COURSE) != null) {
            Map<String, Object> courseMap = (Map<String, Object>) request.get(JsonKey.COURSE);
            String course = (String) courseMap.get(JsonKey.COURSE_ID);
            CopyOnWriteArrayList<String> contentIds = new CopyOnWriteArrayList<String>((List<String>) courseMap.get(JsonKey.CONTENT_IDS));
            LinkedHashMap<String , Object> queryMap = new LinkedHashMap<String , Object>();
            queryMap.put(JsonKey.USER_ID , userId);
            queryMap.put(JsonKey.COURSE_ID , course);
            response = cassandraOperation.getRecordsByProperties(dbInfo.getKeySpace(), dbInfo.getTableName(), queryMap);
            List<Map<String, Object>> modifiedContentList = new ArrayList<Map<String, Object>>();
            List<Map<String, Object>> resultedList = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);

            if (null != contentIds && !(contentIds.isEmpty())) {

                for (Map<String, Object> map : resultedList) {

                    boolean flag = true;
                    for (int i = 0; i < contentIds.size() && flag; i++) {
                        String contentId = contentIds.get(i);
                        if (contentId.equals((String) map.get(JsonKey.CONTENT_ID))) {
                            modifiedContentList.add(map);
                            flag = false;
                        }
                    }

                }
                response.getResult().put(JsonKey.RESPONSE, modifiedContentList);

            }

        }

        return response;
    }

    @SuppressWarnings("unchecked")
	private void removeUnwantedProperties(Response response){
    	List<Map<String, Object>> list = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
    	for(Map<String, Object> map : list){
    		map.remove(JsonKey.DATE_TIME);
    		map.remove(JsonKey.USER_ID);
    		map.remove(JsonKey.ADDED_BY);
    		map.remove(JsonKey.LAST_UPDATED_TIME);
    	}
    }
}