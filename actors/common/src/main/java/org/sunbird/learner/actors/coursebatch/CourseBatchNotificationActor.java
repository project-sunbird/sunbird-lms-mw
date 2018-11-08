package org.sunbird.learner.actors.coursebatch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.actorutil.email.EmailServiceClient;
import org.sunbird.actorutil.email.EmailServiceFactory;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.learner.util.Util;
import org.sunbird.models.course.batch.CourseBatch;

/**
 * Actor responsible to sending email notifications to participants and mentors 
 * in open and invite-only batches.
 */
@ActorConfig(
  tasks = {"courseBatchNotification"},
  asyncTasks = {"courseBatchNotification"}
)
public class CourseBatchNotificationActor extends BaseActor {
  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  private static EmailServiceClient emailServiceClient = EmailServiceFactory.getInstance();
  private static final Props props = Props.create(EmailServiceActor.class);
  private static ActorSystem system = ActorSystem.create("system");

  @Override
  public void onReceive(Request request) throws Throwable {
    String requestedOperation = request.getOperation();
    
    if (requestedOperation.equals(ActorOperations.COURSE_BATCH_NOTIFICATION.getValue())) {
      courseBatchNotication(request);
    } else {
      ProjectLogger.log(
          "CourseBatchNotificationActor:onReceive: Unsupported operation = "
              + request.getOperation(),
          LoggerEnum.ERROR);
    }
  }

  private void courseBatchNotication(Request request) {
    Map<String, Object> requestMap = request.getRequest();
    
    CourseBatch courseBatch = (CourseBatch) requestMap.get(JsonKey.COURSE_BATCH);

    String userId = (String) requestMap.get(JsonKey.USER_ID);
    
    if (userId != null) {
      
      // Open batch
      String template = JsonKey.OPEN_BATCH_LEARNER_UNENROL;
      String subject = JsonKey.UNENROLL_FROM_COURSE_BATCH;

      String operationType = (String) requestMap.get(JsonKey.OPERATION_TYPE);
      
      if (operationType.equals(JsonKey.ADD)) {
        template = JsonKey.BATCH_LEARNER_ENROL;
        subject = JsonKey.COURSE_INVITATION;
      }
            
      triggerEmailNotification(Arrays.asList(userId), courseBatch, JsonKey.UNENROLL_FROM_COURSE_BATCH, JsonKey.BATCH_MENTOR_UNENROL);
      
    } else {
      
      // Invite only batch
      List<String> addedMentors = (List<String>) requestMap.get(JsonKey.ADDED_MENTORS);
      List<String> removedMentors = (List<String>) requestMap.get(JsonKey.REMOVED_MENTORS);
    
      triggerEmailNotification(addedMentors, courseBatch, JsonKey.COURSE_INVITATION, JsonKey.BATCH_MENTOR_ENROL);
      triggerEmailNotification(removedMentors, courseBatch, JsonKey.UNENROLL_FROM_COURSE_BATCH, JsonKey.BATCH_MENTOR_UNENROL);
      
      List<String> addedParticipants = (List<String>) requestMap.get(JsonKey.ADDED_PARTICIPANTS);
      List<String> removedParticipants = (List<String>) requestMap.get(JsonKey.REMOVED_PARTICIPANTS);
    
      triggerEmailNotification(addedParticipants, courseBatch, JsonKey.COURSE_INVITATION, JsonKey.BATCH_MENTOR_ENROL);
      triggerEmailNotification(removedParticipants, courseBatch, JsonKey.UNENROLL_FROM_COURSE_BATCH, JsonKey.BATCH_MENTOR_UNENROL);
      
    }
  }

  private void triggerEmailNotification(
      List<String> userIdList,
      CourseBatch courseBatch,
      String subject,
      String template) {
    
    if (CollectionUtils.isEmpty(userIdList)) return;
    
    List<Map<String, Object>> userMapList = getUsersFromDB(userIdList);

    for (Map<String, Object> user : userMapList) {
      Map<String, Object> requestMap = this.createEmailRequest(user, courseBatch);

      requestMap.put(JsonKey.SUBJECT, subject);
      requestMap.put(JsonKey.EMAIL_TEMPLATE_TYPE, template);
      
      sendMail(requestMap);
    }

  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getUsersFromDB(List<String> userIds) {
    if (userIds != null) {
      Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
      List<String> userIdList = new ArrayList<>(userIds);
      List<String> fields = new ArrayList<>();
      fields.add(JsonKey.FIRST_NAME);
      fields.add(JsonKey.EMAIL);
      Response response =
          cassandraOperation.getRecordsByIdsWithSpecifiedColumns(
              usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), fields, userIdList);
      if (response == null) {
        ProjectLogger.log("No data from cassandra , check connection  ", LoggerEnum.ERROR.name());
      }
      List<Map<String, Object>> userList =
          (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      return userList;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> createEmailRequest(Map<String, Object> userMap, CourseBatch courseBatch) {
    Map<String, Object> courseBatchObject = new ObjectMapper().convertValue(courseBatch, Map.class);
    Map<String, String> additionalCourseInfo =
        (Map<String, String>) courseBatchObject.get(JsonKey.COURSE_ADDITIONAL_INFO);
    
    Map<String, Object> requestMap = new HashMap<String, Object>();
    
    requestMap.put(JsonKey.REQUEST, BackgroundOperations.emailService.name());

    requestMap.put(JsonKey.ORG_NAME, courseBatchObject.get(JsonKey.ORG_NAME));
    requestMap.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.COURSE_LOGO_URL));
    requestMap.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.COURSE_NAME));
    requestMap.put(JsonKey.START_DATE, courseBatchObject.get(JsonKey.START_DATE));
    requestMap.put(JsonKey.END_DATE, courseBatchObject.get(JsonKey.END_DATE));
    requestMap.put(JsonKey.COURSE_ID, courseBatchObject.get(JsonKey.COURSE_ID));
    requestMap.put(JsonKey.NAME, courseBatch.getName());
    
    String userId = (String) userMap.get(JsonKey.USER_ID);

    if (StringUtils.isNotBlank(userId)) {
        requestMap.put(JsonKey.RECIPIENT_USERIDS, userId);
    }
    requestMap.put(JsonKey.FIRST_NAME, userMap.get(JsonKey.FIRST_NAME));

    return requestMap;
  }
  
  private void sendMail(Map<String, Object> requestMap) {
    ActorRef ref = system.actorOf(props);
    try {
      Response response = emailServiceClient.sendMail(ref, requestMap);
      sender().tell(response, self());
      Response res = new Response();
      res.setResponseCode(ResponseCode.OK);
      sender().tell(res, self());
    } catch (Exception e) {
      sender().tell(e, self());
      ProjectLogger.log(
          "CourseBatchNotificationActor:sendMail: Exception occurred with error message = " + e.getMessage(),
          LoggerEnum.ERROR);
    }
  }
  
}
