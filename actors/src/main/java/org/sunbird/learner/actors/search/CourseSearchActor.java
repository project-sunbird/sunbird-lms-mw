package org.sunbird.learner.actors.search;

import java.util.HashMap;
import java.util.Map;

import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.learner.util.TelemetryUtil;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.UntypedAbstractActor;

/**
 * This class will handle search operation for course.
 * 
 * @author Amit Kumar
 */
public class CourseSearchActor extends UntypedAbstractActor {

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Request) {
      try {
        ProjectLogger.log("CourseSearchActor  onReceive called");
        Request actorMessage = (Request) message;


        if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.SEARCH_COURSE.getValue())) {
          Map<String, Object> req = actorMessage.getRequest();
          @SuppressWarnings("unchecked")
          Map<String, Object> searchQueryMap = (Map<String, Object>) req.get(JsonKey.SEARCH);
          Map<String, Object> ekStepSearchQuery = new HashMap<>();
          ekStepSearchQuery.put(JsonKey.REQUEST, searchQueryMap);

          String json;
          try {
            json = new ObjectMapper().writeValueAsString(ekStepSearchQuery);
            Map<String, Object> query = new HashMap<>();
            query.put(JsonKey.SEARCH_QUERY, json);
            Util.getContentData(query);
            Object ekStepResponse = query.get(JsonKey.CONTENTS);
            Response response = new Response();
            response.put(JsonKey.RESPONSE, ekStepResponse);
            sender().tell(response, self());
          } catch (JsonProcessingException e) {
            ProjectCommonException projectCommonException =
                new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
                    ResponseCode.internalError.getErrorMessage(),
                    ResponseCode.internalError.getResponseCode());
            sender().tell(projectCommonException, self());
          }
        } else if (actorMessage.getOperation()
            .equalsIgnoreCase(ActorOperations.GET_COURSE_BY_ID.getValue())) {
          Map<String, Object> req = actorMessage.getRequest();
          String courseId = (String) req.get(JsonKey.ID);
          Map<String, Object> result =
              ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
                  ProjectUtil.EsType.course.getTypeName(), courseId);
          Response response = new Response();
          if (result != null) {
            response.put(JsonKey.RESPONSE, result);
          } else {
            result = new HashMap<>();
            response.put(JsonKey.RESPONSE, result);
          }
          sender().tell(response, self());
        } else {
          ProjectLogger.log("UNSUPPORTED OPERATION");
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

  private void generateSearchTelemetryEvent(SearchDTO searchDto, String[] types,
      Map<String, Object> result) {

    Map<String, Object> telemetryContext = TelemetryUtil.getTelemetryContext();

    Map<String, Object> params = new HashMap<>();
    params.put(JsonKey.TYPE , types);
    params.put(JsonKey.QUERY , searchDto.getQuery());
    params.put(JsonKey.FILTERS, searchDto.getAdditionalProperties().get(JsonKey.FILTERS));
    params.put(JsonKey.SORT ,searchDto.getSortBy());
    params.put(JsonKey.SIZE , result.get(JsonKey.COUNT));
    params.put(JsonKey.TOPN ,result.get(JsonKey.CONTENT)); // need to get topn value from response

    //
    Request req = new Request();
    req.setRequest(telemetryRequestForSearch(telemetryContext, params));
    req.setOperation(ActorOperations.TELEMETRY_PROCESSING.getValue());
    ActorUtil.tell(req);
    // lmaxWriter.submitMessage(req);

  }

  private static Map<String,Object> telemetryRequestForSearch(Map<String, Object> telemetryContext,
      Map<String, Object> params) {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.CONTEXT , telemetryContext);
    map.put(JsonKey.PARAMS, params);
    map.put(JsonKey.TELEMETRY_EVENT_TYPE , "SEARCH");
    return map;
  }

}
