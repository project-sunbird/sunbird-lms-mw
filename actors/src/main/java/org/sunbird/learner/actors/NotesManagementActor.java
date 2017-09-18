package org.sunbird.learner.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;

/**
 * 
 * This class provides API's to create, update, get and delete
 * user note
 *
 */
public class NotesManagementActor extends UntypedAbstractActor {
  
  Util.DbInfo userNotesDbInfo = Util.dbInfoMap.get(JsonKey.USER_NOTES_DB);
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ActorRef backGroundActorRef;
  
  public NotesManagementActor() {
    backGroundActorRef = getContext().actorOf(Props.create(BackgroundJobManager.class), "backGroundActor");
   }

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("NotesManagementActor-onReceive called");
        Request actorMessage = (Request) message;
        if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_NOTE.getValue())) {
          createNote(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.UPDATE_NOTE.getValue())) {
          updateNote(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.SEARCH_NOTE.getValue())) {
          searchNote(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_NOTE.getValue())) {
          getNote(actorMessage);
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.DELETE_NOTE.getValue())) {
          deleteNote(actorMessage);
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION", LoggerEnum.INFO.name());
          ProjectCommonException exception =
              new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                  ResponseCode.invalidOperationName.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
          sender().tell(exception, self());
        }
      } catch (Exception ex) {
        ProjectLogger.log(ex.getMessage(), ex);
        sender().tell(ex, self());
      }
    } else {
      // Throw exception as message body
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
    }
  }
  
  /**
   * Method to create note
   * using userId, courseId or contentId, title and note
   * along with tags which are optional
   * @param actorMessage
   */
  @SuppressWarnings("unchecked")
  private void createNote(Request actorMessage) {
    ProjectLogger.log("Create Note method call start");
    try {
      Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.NOTE);
      if(!validUser((String)req.get(JsonKey.USER_ID))){
        ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidUserId.getErrorCode(),
            ResponseCode.invalidUserId.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
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
      Response result =
          cassandraOperation.insertRecord(userNotesDbInfo.getKeySpace(), userNotesDbInfo.getTableName(), req);
      ProjectLogger.log("Note data saved into cassandra.");
      result.getResult().put(JsonKey.ID, uniqueId);
      result.getResult().remove(JsonKey.RESPONSE);
      sender().tell(result, self());
      
      Response response = new Response();
      response.put(JsonKey.NOTE, req);
      response.put(JsonKey.OPERATION, ActorOperations.INSERT_USER_NOTES_ES.getValue());
      ProjectLogger.log("Calling background job to save org data into ES" + uniqueId);
      backGroundActorRef.tell(response,self());
    } catch (Exception e) {
      ProjectLogger.log("Error occured", e);
      sender().tell(e, self());
      return;
    }
  }

  @SuppressWarnings("unchecked")
  private void updateNote(Request actorMessage) {
    ProjectLogger.log("Update Note method call start");
    try {
      String noteId = (String) actorMessage.getRequest().get(JsonKey.NOTE_ID);
      List<Map<String, Object>> list = getNoteRecordById(noteId);
      if(list.isEmpty()){
        ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidNoteId.getErrorCode(),
            ResponseCode.invalidNoteId.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
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
      
      Response result =
          cassandraOperation.updateRecord(userNotesDbInfo.getKeySpace(), userNotesDbInfo.getTableName(), req);
      ProjectLogger.log("Note data updated");
      result.getResult().put(JsonKey.ID, noteId);
      result.getResult().remove(JsonKey.RESPONSE);
      sender().tell(result, self());
      
      Response noteResponse = new Response();
      noteResponse.put(JsonKey.NOTE, req);
      noteResponse.put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_NOTES_ES.getValue());
      backGroundActorRef.tell(noteResponse,self());
      
    } catch (Exception e) {
      ProjectLogger.log("Error occured", e);
      sender().tell(e, self());
      return;
    }
  }

  private void getNote(Request actorMessage) {
    ProjectLogger.log("Update Note method call start");
    try {
    String noteId = (String) actorMessage.getRequest().get(JsonKey.NOTE_ID);
    Map<String, Object> request = new HashMap<>();
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.ID, noteId);
    
    request.put(JsonKey.FILTERS, filters);
    Response response = new Response();
    Map<String, Object> result = getElasticSearchData(request);
    if (!result.isEmpty() && ((Long) result.get(JsonKey.COUNT) == 0)) {
      ProjectCommonException exception = new ProjectCommonException(
          ResponseCode.invalidNoteId.getErrorCode(), ResponseCode.invalidNoteId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
      sender().tell(exception, self());
      return;
    }
    response.put(JsonKey.RESPONSE, result);
    sender().tell(response, self());
    } catch(Exception e){
      ProjectLogger.log("Error occured", e);
      sender().tell(e, self());
      return;
    }
  }

  private void searchNote(Request actorMessage) {
    ProjectLogger.log("Update Note method call start");
    try {
      Map<String , Object> searchQueryMap = actorMessage.getRequest();
      Response response = new Response();
      Map<String, Object> result =getElasticSearchData(searchQueryMap);
      response.put(JsonKey.RESPONSE, result);
      sender().tell(response, self());
    } catch (Exception e) {
      ProjectLogger.log("Error occured", e);
      sender().tell(e, self());
      return;
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getElasticSearchData(Map<String, Object> searchQueryMap) {
    Map<String, Object> filters = new HashMap<>();
    if(searchQueryMap.containsKey(JsonKey.FILTERS)){
      filters = (Map<String, Object>) searchQueryMap.get(JsonKey.FILTERS);
    }
    if(null != searchQueryMap.get(JsonKey.REQUESTED_BY)){
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
      Object count = (Object) result.get(JsonKey.COUNT);
      Object note = result.get(JsonKey.CONTENT);
      result = new LinkedHashMap<>();
      result.put(JsonKey.COUNT, count);
      result.put(JsonKey.NOTE, note);
    } else {
      result = new HashMap<>();
    }
    return result;
  }
  
  private void deleteNote(Request actorMessage) {
    ProjectLogger.log("Delete Note method call start");
    try {
      String noteId = (String) actorMessage.getRequest().get(JsonKey.NOTE_ID);
      if(!noteIdExists(noteId)){
        ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidNoteId.getErrorCode(),
            ResponseCode.invalidNoteId.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
            sender().tell(exception, self());
            return; 
      }
      Map<String,Object> req = new HashMap<String, Object>();
      req.put(JsonKey.ID, noteId);
      req.put(JsonKey.IS_DELETED, true);
      String updatedBy = (String) actorMessage.getRequest().get(JsonKey.REQUESTED_BY);
      req.put(JsonKey.UPDATED_BY, updatedBy);
      req.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      Response result =
          cassandraOperation.updateRecord(userNotesDbInfo.getKeySpace(), userNotesDbInfo.getTableName(), req);
      result.getResult().remove(JsonKey.RESPONSE); 
      sender().tell(result, self());
      
      Response noteResponse = new Response();
      noteResponse.put(JsonKey.NOTE, req);
      noteResponse.put(JsonKey.OPERATION, ActorOperations.UPDATE_USER_NOTES_ES.getValue());
      backGroundActorRef.tell(noteResponse,self());
    } catch (Exception e) {
      ProjectLogger.log("Error occured", e);
      sender().tell(e, self());
      return;
    }
  }
  
  private Boolean validUser(String userId){
    Boolean result = false;

    if (!ProjectUtil.isStringNullOREmpty(userId)) {
      Map<String, Object> data = ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), userId);
      if(null!= data && !data.isEmpty()){
        result = true;
      }
    }
    return result;
  }
  
  private Boolean noteIdExists(String noteId){
    Boolean result = false;
      List<Map<String,Object>> list = getNoteRecordById(noteId);
      if(!list.isEmpty()){
        result = true;
      }
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getNoteRecordById(String noteId){
    List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
    if (!ProjectUtil.isStringNullOREmpty(noteId)) {
      Response response = cassandraOperation.getRecordById(userNotesDbInfo.getKeySpace(),
          userNotesDbInfo.getTableName(), noteId);
      list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    }
    return list;
  }
  
}