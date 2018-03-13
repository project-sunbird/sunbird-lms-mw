package org.sunbird.learner.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.learner.util.TelemetryUtil;
import org.sunbird.learner.util.Util;

/**
 * This class provides API's to create, update, get and delete user note
 */
public class NotesManagementActor extends BaseActor {

	private Util.DbInfo userNotesDbInfo = Util.dbInfoMap.get(JsonKey.USER_NOTES_DB);
	private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

	public static void init() {
		RequestRouter.registerActor(NotesManagementActor.class,
				Arrays.asList(ActorOperations.CREATE_NOTE.getValue(), ActorOperations.GET_NOTE.getValue(),
						ActorOperations.SEARCH_NOTE.getValue(), ActorOperations.UPDATE_NOTE.getValue(),
						ActorOperations.DELETE_NOTE.getValue()));
	}

	/**
	 * Receives the actor message and perform the operation for user note
	 */
	@Override
	public void onReceive(Request requsst) throws Throwable {
		Util.initializeContext(requsst, JsonKey.USER);
		// set request id fto thread loacl...
		ExecutionContext.setRequestId(requsst.getRequestId());
		if (requsst.getOperation().equalsIgnoreCase(ActorOperations.CREATE_NOTE.getValue())) {
			createNote(requsst);
		} else if (requsst.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_NOTE.getValue())) {
			updateNote(requsst);
		} else if (requsst.getOperation().equalsIgnoreCase(ActorOperations.SEARCH_NOTE.getValue())) {
			searchNote(requsst);
		} else if (requsst.getOperation().equalsIgnoreCase(ActorOperations.GET_NOTE.getValue())) {
			getNote(requsst);
		} else if (requsst.getOperation().equalsIgnoreCase(ActorOperations.DELETE_NOTE.getValue())) {
			deleteNote(requsst);
		} else {
			onReceiveUnsupportedOperation(requsst.getOperation());
		}
	}

	/**
	 * Method to create note using userId, courseId or contentId, title and note
	 * along with tags which are optional
	 * 
	 * @param actorMessage
	 */
	@SuppressWarnings("unchecked")
	private void createNote(Request actorMessage) {
		ProjectLogger.log("Create Note method call start");
		// object of telemetry event...
		Map<String, Object> targetObject = new HashMap<>();
		List<Map<String, Object>> correlatedObject = new ArrayList<>();

		try {
			Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.NOTE);
			if (!validUser((String) req.get(JsonKey.USER_ID))) {
				ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidUserId.getErrorCode(),
						ResponseCode.invalidUserId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, self());
				return;
			}
			String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
			req.put(JsonKey.ID, uniqueId);
			req.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
			req.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
			String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
			if (!(ProjectUtil.isStringNullOREmpty(updatedBy))) {
				req.put(JsonKey.CREATED_BY, updatedBy);
				req.put(JsonKey.UPDATED_BY, updatedBy);
			}
			req.put(JsonKey.IS_DELETED, false);
			Response result = cassandraOperation.insertRecord(userNotesDbInfo.getKeySpace(),
					userNotesDbInfo.getTableName(), req);
			ProjectLogger.log("Note data saved into cassandra.");
			result.getResult().put(JsonKey.ID, uniqueId);
			result.getResult().remove(JsonKey.RESPONSE);
			sender().tell(result, self());

			targetObject = TelemetryUtil.generateTargetObject(uniqueId, JsonKey.NOTE, JsonKey.CREATE, null);
			TelemetryUtil.generateCorrelatedObject(uniqueId, JsonKey.NOTE, null, correlatedObject);
			TelemetryUtil.generateCorrelatedObject(updatedBy, JsonKey.USER, null, correlatedObject);

			Map<String, String> rollup = new HashMap<>();
			rollup.put("l1", (String) req.get(JsonKey.COURSE_ID));
			rollup.put("l2", (String) req.get(JsonKey.CONTENT_ID));
			TelemetryUtil.addTargetObjectRollUp(rollup, targetObject);

			TelemetryUtil.telemetryProcessingCall(actorMessage.getRequest(), targetObject, correlatedObject);

			Request request = new Request();
			request.setOperation(ActorOperations.INSERT_USER_NOTES_ES.getValue());
			request.getRequest().put(JsonKey.NOTE, req);
			ProjectLogger.log("Calling background job to save org data into ES" + uniqueId);
			ActorUtil.tell(request);
		} catch (Exception e) {
			ProjectLogger.log("Error occured", e);
			sender().tell(e, self());
			return;
		}
	}

	/**
	 * Method to update the tags, title and note details of the user note
	 * 
	 * @param actorMessage
	 *            containing noteId and requestedBy
	 */
	@SuppressWarnings("unchecked")
	private void updateNote(Request actorMessage) {
		ProjectLogger.log("Update Note method call start");
		// object of telemetry event...
		Map<String, Object> targetObject = new HashMap<>();
		List<Map<String, Object>> correlatedObject = new ArrayList<>();

		try {
			String noteId = (String) actorMessage.getRequest().get(JsonKey.NOTE_ID);
			String userId = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
			if (!validateUserForNoteUpdation(userId, noteId)) {
				throw new ProjectCommonException(ResponseCode.unAuthorised.getErrorCode(),
						ResponseCode.unAuthorised.getErrorMessage(), ResponseCode.UNAUTHORIZED.getResponseCode());
			}
			Map<String, Object> list = getNoteRecordById(noteId);
			if (list.isEmpty()) {
				ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidNoteId.getErrorCode(),
						ResponseCode.invalidNoteId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, self());
				return;
			}
			Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.NOTE);
			req.remove(JsonKey.USER_ID);
			req.remove(JsonKey.COURSE_ID);
			req.remove(JsonKey.CONTENT_ID);
			req.remove(JsonKey.IS_DELETED);
			req.put(JsonKey.ID, noteId);
			String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
			req.put(JsonKey.UPDATED_BY, updatedBy);
			req.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());

			Response result = cassandraOperation.updateRecord(userNotesDbInfo.getKeySpace(),
					userNotesDbInfo.getTableName(), req);
			ProjectLogger.log("Note data updated");
			result.getResult().put(JsonKey.ID, noteId);
			result.getResult().remove(JsonKey.RESPONSE);
			sender().tell(result, self());

			targetObject = TelemetryUtil.generateTargetObject(noteId, JsonKey.NOTE, JsonKey.UPDATE, null);
			TelemetryUtil.generateCorrelatedObject(noteId, JsonKey.NOTE, null, correlatedObject);
			TelemetryUtil.generateCorrelatedObject(userId, JsonKey.USER, null, correlatedObject);

			Map<String, String> rollup = new HashMap<>();
			rollup.put("l1", (String) ((Map<String, Object>) actorMessage.getRequest().get(JsonKey.NOTE))
					.get(JsonKey.COURSE_ID));
			rollup.put("l2", (String) ((Map<String, Object>) actorMessage.getRequest().get(JsonKey.NOTE))
					.get(JsonKey.CONTENT_ID));
			TelemetryUtil.addTargetObjectRollUp(rollup, targetObject);

			TelemetryUtil.telemetryProcessingCall(actorMessage.getRequest(), targetObject, correlatedObject);

			Request request = new Request();
			request.getRequest().put(JsonKey.NOTE, req);
			request.setOperation(ActorOperations.UPDATE_USER_NOTES_ES.getValue());
			ActorUtil.tell(request);

		} catch (Exception e) {
			ProjectLogger.log("Error occured", e);
			sender().tell(e, self());
			return;
		}
	}

	/**
	 * Method to get the note for the given note Id of the user
	 * 
	 * @param actorMessage
	 *            containing noteId and requestedBy
	 */
	private void getNote(Request actorMessage) {
		ProjectLogger.log("Update Note method call start");
		try {
			String noteId = (String) actorMessage.getRequest().get(JsonKey.NOTE_ID);
			String userId = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
			if (!validateUserForNoteUpdation(userId, noteId)) {
				throw new ProjectCommonException(ResponseCode.unAuthorised.getErrorCode(),
						ResponseCode.unAuthorised.getErrorMessage(), ResponseCode.UNAUTHORIZED.getResponseCode());
			}
			Map<String, Object> request = new HashMap<>();
			Map<String, Object> filters = new HashMap<>();
			filters.put(JsonKey.ID, noteId);

			request.put(JsonKey.FILTERS, filters);
			Response response = new Response();
			Map<String, Object> result = getElasticSearchData(request);
			if (!result.isEmpty() && ((Long) result.get(JsonKey.COUNT) == 0)) {
				ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidNoteId.getErrorCode(),
						ResponseCode.invalidNoteId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, self());
				return;
			}
			response.put(JsonKey.RESPONSE, result);
			sender().tell(response, self());
		} catch (Exception e) {
			ProjectLogger.log("Error occured", e);
			sender().tell(e, self());
			return;
		}
	}

	/**
	 * Method to search the note for the given request
	 * 
	 * @param actorMessage
	 *            containing search parameters
	 */
	private void searchNote(Request actorMessage) {
		ProjectLogger.log("Update Note method call start");
		try {
			Map<String, Object> searchQueryMap = actorMessage.getRequest();
			Response response = new Response();
			Map<String, Object> result = getElasticSearchData(searchQueryMap);
			response.put(JsonKey.RESPONSE, result);
			sender().tell(response, self());
		} catch (Exception e) {
			ProjectLogger.log("Error occured", e);
			sender().tell(e, self());
			return;
		}
	}

	/**
	 * Method to get note data from ElasticSearch
	 * 
	 * @param searchQueryMap
	 * @return Map<String, Object> - note data
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getElasticSearchData(Map<String, Object> searchQueryMap) {
		Map<String, Object> filters = new HashMap<>();
		if (searchQueryMap.containsKey(JsonKey.FILTERS)) {
			filters = (Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS);
		}
		if (null != searchQueryMap.get(JsonKey.REQUESTED_BY)) {
			filters.put(JsonKey.USER_ID, searchQueryMap.get(JsonKey.REQUESTED_BY));
		}
		filters.put(JsonKey.IS_DELETED, false);
		searchQueryMap.put(JsonKey.FILTERS, filters);
		SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
		List<String> excludedFields = new ArrayList<>();
		if (null != searchDto.getExcludedFields()) {
			excludedFields = searchDto.getExcludedFields();
		}
		excludedFields.add(JsonKey.IS_DELETED);
		searchDto.setExcludedFields(excludedFields);
		Map<String, Object> result = ElasticSearchUtil.complexSearch(searchDto,
				ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.usernotes.getTypeName());
		if (result != null) {
			Object count = result.get(JsonKey.COUNT);
			Object note = result.get(JsonKey.CONTENT);
			result = new LinkedHashMap<>();
			result.put(JsonKey.COUNT, count);
			result.put(JsonKey.NOTE, note);
		} else {
			result = new HashMap<>();
		}
		return result;
	}

	/**
	 * Method to mark the note as deleted [Soft Delete i.e to set the isDeleted
	 * field to true]
	 * 
	 * @param actorMessage
	 */
	private void deleteNote(Request actorMessage) {
		ProjectLogger.log("Delete Note method call start");
		// object of telemetry event...
		Map<String, Object> targetObject = new HashMap<>();
		List<Map<String, Object>> correlatedObject = new ArrayList<>();
		try {
			String noteId = (String) actorMessage.getRequest().get(JsonKey.NOTE_ID);
			String userId = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
			if (!validateUserForNoteUpdation(userId, noteId)) {
				throw new ProjectCommonException(ResponseCode.unAuthorised.getErrorCode(),
						ResponseCode.unAuthorised.getErrorMessage(), ResponseCode.UNAUTHORIZED.getResponseCode());
			}
			if (!noteIdExists(noteId)) {
				ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidNoteId.getErrorCode(),
						ResponseCode.invalidNoteId.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, self());
				return;
			}
			Map<String, Object> req = new HashMap<>();
			req.put(JsonKey.ID, noteId);
			req.put(JsonKey.IS_DELETED, true);
			String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
			req.put(JsonKey.UPDATED_BY, updatedBy);
			req.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
			Response result = cassandraOperation.updateRecord(userNotesDbInfo.getKeySpace(),
					userNotesDbInfo.getTableName(), req);
			result.getResult().remove(JsonKey.RESPONSE);
			sender().tell(result, self());

			targetObject = TelemetryUtil.generateTargetObject(noteId, JsonKey.NOTE, JsonKey.DELETE, null);
			TelemetryUtil.generateCorrelatedObject(noteId, JsonKey.NOTE, null, correlatedObject);
			TelemetryUtil.generateCorrelatedObject(userId, JsonKey.USER, null, correlatedObject);

			TelemetryUtil.telemetryProcessingCall(actorMessage.getRequest(), targetObject, correlatedObject);

			Request request = new Request();
			request.getRequest().put(JsonKey.NOTE, req);
			request.setOperation(ActorOperations.UPDATE_USER_NOTES_ES.getValue());
			ActorUtil.tell(request);
		} catch (Exception e) {
			ProjectLogger.log("Error occured", e);
			sender().tell(e, self());
			return;
		}
	}

	/**
	 * Method to validate User based on userId from ElasticSearch Data
	 * 
	 * @param userId
	 * @return true if user data is present in ES else false
	 */
	private Boolean validUser(String userId) {
		Boolean result = false;

		if (!ProjectUtil.isStringNullOREmpty(userId)) {
			Map<String, Object> data = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
					EsType.user.getTypeName(), userId);
			if (null != data && !data.isEmpty()) {
				result = true;
			}
		}
		return result;
	}

	/**
	 * Method to validate note using noteId
	 * 
	 * @param noteId
	 * @return true if note exists in Cassandra else false
	 */
	private Boolean noteIdExists(String noteId) {
		Boolean result = false;
		Map<String, Object> list = getNoteRecordById(noteId);
		if (!list.isEmpty()) {
			result = true;
		}
		return result;
	}

	/**
	 * Method to get Note details using note Id
	 * 
	 * @param noteId
	 * @return Note data as List<Map<String, Object>>
	 */
	private Map<String, Object> getNoteRecordById(String noteId) {
		return ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
				EsType.usernotes.getTypeName(), noteId);
	}

	private Boolean validateUserForNoteUpdation(String userId, String noteId) {
		Boolean result = false;
		Map<String, Object> noteData = getNoteRecordById(noteId);
		if ((null != noteData && !noteData.isEmpty()) && !ProjectUtil.isStringNullOREmpty(userId)
				&& userId.equalsIgnoreCase((String) noteData.get(JsonKey.USER_ID))) {
			result = true;
		}
		return result;
	}

}
