package org.sunbird.learner.actors.coursebatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.models.course.batch.CourseBatch;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This actor will handle course enrollment operations.
 *
 * @author github.com/iostream04
 */

@ActorConfig(tasks = { "batchBulk", "batchUpdate", "batchOperation" }, asyncTasks = { "batchBulk", "batchUpdate",
    "batchOperation" })
public class BatchOperationNotifierActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    if (request.getOperation().equalsIgnoreCase(ActorOperations.BATCH_BULK.getValue())) {
      Map<String, Object> requestMap = request.getRequest();
      batchBulkOperationNotifier((CourseBatch) requestMap.get(BackgroundOperations.COURSE_BATCH.name()),
          (String) requestMap.get(BackgroundOperations.USER_TYPE.name()),
          (String) requestMap.get(BackgroundOperations.OPERATION_TYPE.name()));
    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.BATCH_UPDATE.getValue())) {
      Map<String, Object> requestMap = request.getRequest();
      batchUpdateOperationNotifier((CourseBatch) requestMap.get(BackgroundOperations.OLD.name()),
          (CourseBatch) requestMap.get(BackgroundOperations.NEW.name()));
    } else if (request.getOperation().equalsIgnoreCase(ActorOperations.BATCH_UPDATE.getValue())) {
      Map<String, Object> requestMap = request.getRequest();
      batchOperationNotifier((Map<String, Object>) requestMap.get(BackgroundOperations.COURSE_MAP.name()),
          (String) requestMap.get(BackgroundOperations.OPERATION_TYPE.name()));
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  public void batchOperationNotifier(Map<String, Object> courseMap, String operationType) {
    Request request = getMailRequest(courseMap);
    sendMail(request);
  }

  private void batchBulkOperationNotifier(CourseBatch courseBatch, String userType, String operationType) {

    if (userType.equals(BackgroundOperations.MENTOR.name())) {
      if (operationType.equals(JsonKey.ADD)) {
        Request requestForMentor = getMailRequest(courseBatch, BackgroundOperations.MENTOR.name(), JsonKey.ADD);
        sendMail(requestForMentor);
      } else if (operationType.equals(JsonKey.REMOVE)) {
        Request requestForMentor = getMailRequest(courseBatch, BackgroundOperations.MENTOR.name(), JsonKey.ADD);
        sendMail(requestForMentor);
      }
    }
    if (userType.equals(BackgroundOperations.PARTICIPANTS.name())) {

      if (operationType.equals(JsonKey.ADD)) {
        Request requestForParticipients = getMailRequest(courseBatch, BackgroundOperations.PARTICIPANTS.name(),
            JsonKey.ADD);
        sendMail(requestForParticipients);
      } else if (operationType.equals(JsonKey.REMOVE)) {
        Request requestForParticipients = getMailRequest(courseBatch, BackgroundOperations.PARTICIPANTS.name(),
            JsonKey.REMOVE);
        sendMail(requestForParticipients);
      }

    }

  }

  private void sendMail(Request request) {
    request.setOperation(BackgroundOperations.emailService.name());
    tellToAnother(request);
  }

  private void batchUpdateOperationNotifier(CourseBatch courseBatchPrev, CourseBatch courseBatchNew) {
    List<String> prevMentors = courseBatchPrev.getMentors();
    List<String> newMentors = courseBatchNew.getMentors();
    List<String> removedMentors = courseBatchPrev.getMentors();
    if (prevMentors == null) {
      prevMentors = new ArrayList<>();
      removedMentors = new ArrayList<>();
    }
    if (newMentors == null) {
      newMentors = new ArrayList<>();
    }
    for (String mentor : prevMentors) {
      if (newMentors.contains(mentor)) {
        newMentors.remove(mentor);
        removedMentors.remove(mentor);
      }
    }
    courseBatchPrev.setMentors(removedMentors);
    courseBatchNew.setMentors(newMentors);
    if (!removedMentors.isEmpty())
      batchBulkOperationNotifier(courseBatchPrev, BackgroundOperations.MENTOR.name(), JsonKey.REMOVE);
    if (!newMentors.isEmpty())
      batchBulkOperationNotifier(courseBatchNew, BackgroundOperations.MENTOR.name(), JsonKey.ADD);
    Map<String, Boolean> oldParticipants = courseBatchPrev.getParticipant();
    Map<String, Boolean> newParticipants = courseBatchNew.getParticipant();
    Map<String, Boolean> removedParticipants = courseBatchPrev.getParticipant();
    if (oldParticipants == null) {
      oldParticipants = new HashMap<>();
      removedParticipants = new HashMap<>();
    }
    if (newParticipants == null)
      newParticipants = new HashMap<>();
    Set<String> oldParticipantsSet = oldParticipants.keySet();
    for (String participant : oldParticipantsSet) {
      if (newParticipants.containsKey(participant)) {
        newParticipants.remove(participant);
        removedParticipants.remove(participant);
      }
    }
    courseBatchPrev.setParticipant(removedParticipants);
    courseBatchNew.setParticipant(newParticipants);
    if (!removedParticipants.isEmpty())
      batchBulkOperationNotifier(courseBatchPrev, BackgroundOperations.PARTICIPANTS.name(), JsonKey.REMOVE);
    if (!newParticipants.isEmpty())
      batchBulkOperationNotifier(courseBatchNew, BackgroundOperations.PARTICIPANTS.name(), JsonKey.ADD);
  }

  private Request getMailRequest(Map<String, Object> courseMap) {
    Request request = new Request();
    Map<String, Object> requestMap = getRequestMap(courseMap);
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.EMAIL_REQUEST, requestMap);
    request.setRequest(innerMap);
    return request;
  }

  private Request getMailRequest(CourseBatch courseBatch, String userType, String operationType) {

    Request request = new Request();
    Map<String, Object> requestMap = getRequestMap(courseBatch, userType, operationType);
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.EMAIL_REQUEST, requestMap);
    request.setRequest(innerMap);
    return request;
  }

  private Map<String, Object> getRequestMap(Map<String, Object> courseMap) {
    Map<String, Object> requestMap = new HashMap<>();
    List<String> users = new ArrayList<>();
    users.add((String) courseMap.get(JsonKey.USER_ID));
    requestMap.put(JsonKey.RECIPIENT_USERIDS, users);
    requestMap.put(JsonKey.EMAIL_TEMPLATE_TYPE, "default");
    requestMap.put(JsonKey.SUBJECT, courseMap.get(JsonKey.SUBJECT));
    requestMap.put(JsonKey.ORG_NAME, courseMap.get(JsonKey.ORG_NAME));
    requestMap.put(JsonKey.COURSE_LOGO_URL, courseMap.get(JsonKey.COURSE_LOGO_URL));
    requestMap.put(JsonKey.COURSE_NAME, courseMap.get(JsonKey.COURSE_NAME));
    requestMap.put(JsonKey.DESCRIPTION, courseMap.get(JsonKey.DESCRIPTION));
    requestMap.put(JsonKey.ADDED_BY, courseMap.get(JsonKey.ADDED_BY));

    return requestMap;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getRequestMap(CourseBatch courseBatch, String userType, String operationType) {
    Map<String, Object> courseBatchObject = new ObjectMapper().convertValue(courseBatch, Map.class);
    Map<String, String> additionalCourseInfo = (Map<String, String>) courseBatchObject
        .get(JsonKey.COURSE_ADDITIONAL_INFO);

    Map<String, Object> requestMap = new HashMap<>();
    if (userType.equals(BackgroundOperations.MENTOR.name())) {
      getMentors(requestMap, courseBatch);
    }
    if (userType.equals(BackgroundOperations.PARTICIPANTS.name())) {
      getParticipants(requestMap, courseBatch);
    }
    requestMap.put(JsonKey.EMAIL_TEMPLATE_TYPE, "default");
    requestMap.put(JsonKey.SUBJECT, courseBatch.getCourseAdditionalInfo().get(JsonKey.SUBJECT));
    requestMap.put(JsonKey.ORG_NAME, courseBatchObject.get(JsonKey.ORG_NAME));
    requestMap.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.COURSE_LOGO_URL));
    requestMap.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.COURSE_NAME));
    requestMap.put(JsonKey.DESCRIPTION, additionalCourseInfo.get(JsonKey.DESCRIPTION));
    requestMap.put(JsonKey.ADDED_BY, courseBatchObject.get(JsonKey.ADDED_BY));

    return requestMap;
  }

  private void getParticipants(Map<String, Object> requestMap, CourseBatch courseBatch) {

    List<String> users = new ArrayList<>();
    Map<String, Boolean> participants = courseBatch.getParticipant();
    Set<String> key = participants.keySet();
    for (String user : key) {
      users.add(user);
    }
    requestMap.put(JsonKey.RECIPIENT_EMAILS, users);

  }

  private void getMentors(Map<String, Object> requestMap, CourseBatch courseBatch) {

    requestMap.put(JsonKey.RECIPIENT_EMAILS, courseBatch.getMentors());
  }

}