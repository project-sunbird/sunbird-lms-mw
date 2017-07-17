package org.sunbird.learner.actors.assessment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.services.impl.DefaultAssessmentEvaluator;
import org.sunbird.learner.util.Util;

/**
 * this class will run in background for assessment evaluation
 * @author Amit Kumar
 *
 */
public class AssessmentUtil{
	private CassandraOperation cassandraOperation = new CassandraOperationImpl();
	Util.DbInfo assmntEvalDbInfo = Util.dbInfoMap.get(JsonKey.ASSESSMENT_EVAL_DB);
	Util.DbInfo assmntItemDbInfo = Util.dbInfoMap.get(JsonKey.ASSESSMENT_ITEM_DB);
	Util.DbInfo contentConsumptionDbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
	
	
	public void evalAssessment(Map<String, Object> request) {
		Map<String, List<Map<String, Object>>> evalAssmntReq = new HashMap<String, List<Map<String,Object>>>();
		List<Map<String, Object>> assmntListResult = null;
		DefaultAssessmentEvaluator evaluator = new DefaultAssessmentEvaluator();
		evalAssmntReq = createReqFrEvalAssmnt(request);
			assmntListResult = evaluator.evaluateResult(evalAssmntReq);
			if(null != assmntListResult && assmntListResult.size() > 0){ 
				updateScoreIntoContentConsumptionTable(assmntListResult);
				
				insertRecordIntoAssmntEvalTable(assmntListResult,request);
			}
		List<Map<String, Object>> list = evalAssmntReq.get(request.get(JsonKey.USER_ID));
		for(Map<String, Object> map : list){
			Map<String, Object>  reqMap = new HashMap<>();
			reqMap.put(JsonKey.ID, map.get(JsonKey.ID));
			reqMap.put(JsonKey.PROCESSING_STATUS, true);
			try{
				cassandraOperation.upsertRecord(assmntItemDbInfo.getKeySpace(), assmntItemDbInfo.getTableName(),reqMap);
			}catch(Exception e){
				ProjectLogger.log(e.getMessage(), e);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, List<Map<String, Object>>> createReqFrEvalAssmnt(Map<String, Object> request) {
		    Map<String, List<Map<String, Object>>> result = new HashMap<>();
		    List<Map<String, Object>> resMap = null;
		    /**
		     * fetching records from Assessment item table based on courseId,contentId and userId 
		     * and creating request for evaluateAssessment method of DefaultAssessmentEvaluator class
		     */
		    request.put(JsonKey.PROCESSING_STATUS, false);
			Response response = cassandraOperation.getRecordsByProperties(assmntItemDbInfo.getKeySpace(), assmntItemDbInfo.getTableName(),request);
			resMap = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
			if(request.containsKey(JsonKey.USER_ID)){
				result.put((String) request.get(JsonKey.USER_ID), resMap);
			}else{
				for(Map<String, Object> map : resMap){
					String userId = (String) map.get(JsonKey.USER_ID);
					if(result.containsKey(userId)){
						((List<Map<String, Object>>)result.get(userId)).add(map);
					}else{
						List<Map<String, Object>> list = new ArrayList<>();
						list.add(map);
						result.put(userId, list);
					}
				}
			}
		return result;
	}

	@SuppressWarnings("unchecked")
	private void updateScoreIntoContentConsumptionTable(List<Map<String, Object>> assmntListResult) {
		Map<String,Object> reqMap = new HashMap<>();
		for(Map<String, Object> map : assmntListResult){
			String userId = (String) map.get(JsonKey.USER_ID);
			String contentId = (String) map.get(JsonKey.CONTENT_ID);
			String courseId = (String) map.get(JsonKey.COURSE_ID);
			String id = OneWayHashing.encryptVal(userId+JsonKey.PRIMARY_KEY_DELIMETER+contentId+JsonKey.PRIMARY_KEY_DELIMETER+courseId);
			reqMap.put(JsonKey.ASSESSMENT_SCORE, map.get(JsonKey.ASSESSMENT_SCORE));
			reqMap.put(JsonKey.RESULT, map.get(JsonKey.RESULT));
			reqMap.put(JsonKey.ASSESSMENT_GRADE, map.get(JsonKey.ASSESSMENT_GRADE));
			reqMap.put(JsonKey.ID, id);
			reqMap.putAll(map);
			try{
				Response response = cassandraOperation.getRecordById(contentConsumptionDbInfo.getKeySpace(), contentConsumptionDbInfo.getTableName(),id);
				List<Map<String,Object>> responseList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
				if(null != responseList && responseList.isEmpty()){
					cassandraOperation.upsertRecord(contentConsumptionDbInfo.getKeySpace(), contentConsumptionDbInfo.getTableName(),reqMap);
				}else if (null != responseList && responseList.size() == 1){
					Map<String,Object> contentConsumptionMap = responseList.get(0);
					if(((Double.parseDouble((String)contentConsumptionMap.get(JsonKey.ASSESSMENT_SCORE))) - (Double.parseDouble((String)reqMap.get(JsonKey.ASSESSMENT_SCORE)))) < 0){
						cassandraOperation.upsertRecord(contentConsumptionDbInfo.getKeySpace(), contentConsumptionDbInfo.getTableName(),reqMap);
					}
				}
			}catch(Exception e){
			  ProjectLogger.log(e.getMessage(), e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void insertRecordIntoAssmntEvalTable(List<Map<String, Object>> assmntList, Map<String, Object> assmntReq){
			for(Map<String, Object> map : assmntList ){
				try{
					Map<String,Object> request = new HashMap<>();
					request.put(JsonKey.CONTENT_ID, map.get(JsonKey.CONTENT_ID));
					request.put(JsonKey.COURSE_ID, map.get(JsonKey.COURSE_ID));
					request.put(JsonKey.USER_ID, map.get(JsonKey.USER_ID));
					map.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
					map.putAll(assmntReq);
					map.remove(JsonKey.PROCESSING_STATUS);
					map.remove(JsonKey.ASSESSMENT_GRADE);
					Response response = cassandraOperation.getRecordsByProperties(assmntEvalDbInfo.getKeySpace(), assmntEvalDbInfo.getTableName(),request);
					List<Map<String, Object>> resList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
					Map<String, Object> resMap = null;
					if(null != resList && resList.size() > 0){
						resMap = resList.get(0);
						Double scoreFrmDbRes = (resMap.get(JsonKey.ASSESSMENT_SCORE)) != null ? Double.parseDouble((String) resMap.get(JsonKey.ASSESSMENT_SCORE)) : 0d ;
						Double scoreFrmReq = (map.get(JsonKey.ASSESSMENT_SCORE)) != null ? Double.parseDouble((String) map.get(JsonKey.ASSESSMENT_SCORE)) : 0d ;
						
						if(scoreFrmDbRes > scoreFrmReq){
							Map<String,Object> reqMap = new HashMap<>();
							int attemptedCount = (resMap.get(JsonKey.ATTEMPTED_COUNT) != null) ?  (int)resMap.get(JsonKey.ATTEMPTED_COUNT) : 0 ;
							reqMap.put(JsonKey.ATTEMPTED_COUNT, attemptedCount+1);
							reqMap.put(JsonKey.ID, resMap.get(JsonKey.ID));
							cassandraOperation.updateRecord(assmntEvalDbInfo.getKeySpace(), assmntEvalDbInfo.getTableName(),map);
						}else{
							map.put(JsonKey.ID, resMap.get(JsonKey.ID));
							int attemptedCount = (resMap.get(JsonKey.ATTEMPTED_COUNT) != null) ?  (int)resMap.get(JsonKey.ATTEMPTED_COUNT) : 0 ;
							map.put(JsonKey.ATTEMPTED_COUNT, attemptedCount+1);
							cassandraOperation.updateRecord(assmntEvalDbInfo.getKeySpace(), assmntEvalDbInfo.getTableName(),map);
						}
					}else{
						String id = ProjectUtil.getUniqueIdFromTimestamp(1);
						map.put(JsonKey.ATTEMPTED_COUNT, 1);
						map.put(JsonKey.ID, id);
						cassandraOperation.insertRecord(assmntEvalDbInfo.getKeySpace(), assmntEvalDbInfo.getTableName(),map);
					}
				}catch(Exception e){
				  ProjectLogger.log(e.getMessage(), e);
				}
			}
	}

}
