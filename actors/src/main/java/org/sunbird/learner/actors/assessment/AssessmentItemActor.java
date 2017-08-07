package org.sunbird.learner.actors.assessment;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;

import akka.actor.UntypedAbstractActor;

/***
 * this class will handle all operation for Assessment
 * @author Amit Kumar
 *
 */
public class AssessmentItemActor extends UntypedAbstractActor {

	private CassandraOperation cassandraOperation = new CassandraOperationImpl();
	private Util.DbInfo assmntItemDbInfo = Util.dbInfoMap.get(JsonKey.ASSESSMENT_ITEM_DB);
	
	@Override
	public void onReceive(Object message) throws Throwable {
		if (message instanceof Request) {
			try {
			    ProjectLogger.log("AssessmentItemActor onReceive called");
				Request actorMessage = (Request) message;
				if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_ASSESSMENT.getValue())) {
					getAssessment(actorMessage);
				} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.SAVE_ASSESSMENT.getValue())) {
					saveAssessment(actorMessage);
				} else {
			        ProjectLogger.log("UNSUPPORTED OPERATION");
					ProjectCommonException exception = new ProjectCommonException(
							ResponseCode.invalidOperationName.getErrorCode(),
							ResponseCode.invalidOperationName.getErrorMessage(),
							ResponseCode.CLIENT_ERROR.getResponseCode());
					sender().tell(exception, self());
				}
			}catch(Exception ex){
				ProjectLogger.log(ex.getMessage(), ex);
				sender().tell(ex , self());
			}
		} else {
			// Throw exception as message body
		    ProjectLogger.log("UNSUPPORTED MESSAGE");
			ProjectCommonException exception = new ProjectCommonException(
					ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(),
					ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
		}

	}

	@SuppressWarnings("unchecked")
	private void saveAssessment(Request actorMessage) {
		Map<String, Object> req = actorMessage.getRequest();
		Map<String, Object> assmt = (Map<String, Object>) req.get(JsonKey.ASSESSMENT);
		List<Map<String, Object>> assmtItemMapList = (List<Map<String, Object>>) assmt.get(JsonKey.ASSESSMENT);
		String courseId = (String) assmt.get(JsonKey.COURSE_ID);
		String contentId = (String) assmt.get(JsonKey.CONTENT_ID);
		String attemptId = (String) assmt.get(JsonKey.ATTEMPT_ID);
		int assmntStatus = ((BigInteger) assmt.get(JsonKey.STATUS)).intValue();
		Response assmntResponse = new Response();
		try{
		assmntResponse.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
		for(Map<String, Object> assmtMap : assmtItemMapList){
			String uniqueId = ProjectUtil.createUserAuthToken((String)req.get(JsonKey.REQUESTED_BY),"");
			assmtMap.put(JsonKey.ID, uniqueId);
			if(assmtMap.containsKey(JsonKey.TIME_TAKEN)){
				assmtMap.put(JsonKey.TIME_TAKEN, ((BigInteger)assmtMap.get(JsonKey.TIME_TAKEN)).intValue());
			}
			assmtMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
			assmtMap.put(JsonKey.USER_ID,req.get(JsonKey.REQUESTED_BY));
			assmtMap.put(JsonKey.COURSE_ID,courseId);
			assmtMap.put(JsonKey.CONTENT_ID,contentId);
			assmtMap.put(JsonKey.ATTEMPT_ID,attemptId);
			assmtMap.put(JsonKey.PROCESSING_STATUS,false);
			cassandraOperation.insertRecord(assmntItemDbInfo.getKeySpace(), assmntItemDbInfo.getTableName(),assmtMap);
		}
		
		}catch(Exception e){
			assmntResponse.put(JsonKey.RESPONSE, JsonKey.FAILURE);
			sender().tell(assmntResponse, self());
		}
		sender().tell(assmntResponse, self());
		//evaluate the assessment and update the result in content consumption table
		if(assmntStatus == ProjectUtil.ProgressStatus.COMPLETED.getValue()){
			AssessmentUtil util = new AssessmentUtil();
			Map<String,Object> map = new HashMap<>();
			map.put(JsonKey.USER_ID,req.get(JsonKey.REQUESTED_BY));
			map.put(JsonKey.COURSE_ID,courseId);
			map.put(JsonKey.CONTENT_ID,contentId);
			map.put(JsonKey.ATTEMPT_ID,attemptId);
			util.evalAssessment(map);
		}
	}

	@SuppressWarnings("unchecked")
	private void getAssessment(Request actorMessage) {
		Map<String, Object> req = actorMessage.getRequest();
		Map<String, Object> reqMap = (Map<String, Object>) req.get(JsonKey.ASSESSMENT);
		List<String> userIds = (List<String>) reqMap.get(JsonKey.USERIDS);
		String courseId = (String) reqMap.get(JsonKey.COURSE_ID);
		if (reqMap.containsKey(JsonKey.USERIDS) && null != reqMap.get(JsonKey.USERIDS)) {
			List<List<Map<String, Object>>> assmntList = new ArrayList<>();
			Map<String,Object> cassandraReq = new HashMap<>();
			cassandraReq.put(JsonKey.COURSE_ID, courseId);
			for(String userId : userIds){
				cassandraReq.put(JsonKey.USER_ID, userId);
				Response response = cassandraOperation.getRecordsByProperties(assmntItemDbInfo.getKeySpace(), assmntItemDbInfo.getTableName(), cassandraReq);
				if(null != response.get(JsonKey.RESPONSE))
					assmntList.add(((List<Map<String, Object>>)response.get(JsonKey.RESPONSE)));
			}
			Response assmntResponse = new Response();
			assmntResponse.put(JsonKey.RESPONSE, assmntList);
			sender().tell(assmntResponse, self());
		}else{
			Response response = cassandraOperation.getRecordsByProperty(assmntItemDbInfo.getKeySpace(), assmntItemDbInfo.getTableName(), JsonKey.COURSE_ID, courseId);
			sender().tell(response, self());
		}
		
	}

}
