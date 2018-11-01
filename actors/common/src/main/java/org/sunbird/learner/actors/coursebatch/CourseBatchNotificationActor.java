package org.sunbird.learner.actors.coursebatch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * This actor will initiates email notification when user get enrolled/unenrolled to a open batch
 * and also for Mentor/Participants addition/removal for an "invite-only" batches
 *
 * @author github.com/iostream04
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
          "CourseBatchNotificationActor : onReceive, No suitable Operation found for the request "
              + request.getOperation(),
          LoggerEnum.ERROR);
    }
  }

  private void courseBatchNotication(Request request) {
    Map<String, Object> requestMap = request.getRequest();
    if (requestMap.get(JsonKey.USER_ID) != null) {
      batchEnrollOperationNotifier(
          (String) requestMap.get(JsonKey.USER_ID),
          (CourseBatch) requestMap.get(JsonKey.COURSE_BATCH),
          (String) requestMap.get(JsonKey.OPERATION_TYPE));
    } else if (requestMap.get(JsonKey.UPDATE) != null) {
      batchUpdateOperationNotifier((CourseBatch) requestMap.get(JsonKey.COURSE_BATCH), requestMap);
    } else {
      sendEmailNotification(
          (CourseBatch) requestMap.get(JsonKey.COURSE_BATCH),
          null,
          null,
          (String) requestMap.get(JsonKey.OPERATION_TYPE));
    }
  }

  /*
   * This method process open batch enroll and unenroll operation notifications
   *
   * @param courseMap , map for getting operation and email related data
   *
   * @param courseBatch object for getting course and email related data
   */
  public void batchEnrollOperationNotifier(
      String userId, CourseBatch courseBatch, String operationType) {
    Map<String, Object> requestMap = createRequestMap(courseBatch);
    List<String> userIds = new ArrayList<>();
    userIds.add(userId);
    List<Map<String, Object>> participentList = getUsersFromDB(userIds);
    Map<String, String> user;
    if (!CollectionUtils.isEmpty(participentList)) {
      if (operationType.equals(JsonKey.ADD)) {
        user = getUserData(participentList.get(0), JsonKey.ADD);
        user.put(JsonKey.EMAIL_TEMPLATE_TYPE, JsonKey.BATCH_LEARNER_ENROL);
      } else {
        user = getUserData(participentList.get(0), JsonKey.REMOVE);
        user.put(JsonKey.EMAIL_TEMPLATE_TYPE, JsonKey.OPEN_BATCH_LEARNER_UNENROL);
      }
      sendMail(user, requestMap);
    }
  }

  /*
   * This method takes courseBatch object and ObjectType as parameter and process
   * update batch related operation notifications
   *
   * @param CourseBatch object for course related data
   *
   * @param operationType String for specifying the operation type ie: add/remove
   */

  private void sendEmailNotification(
      CourseBatch courseBatch,
      List<String> mentors,
      List<String> participants,
      String operationType) {
    List<Map<String, Object>> mentorList = null;
    List<Map<String, Object>> participantList = null;
    if (mentors == null) {
      mentorList = getUsersFromDB(courseBatch.getMentors());
    } else {
      mentorList = getUsersFromDB(mentors);
    }
    List<String> userIds = null;
    if (participants == null) {
      userIds = getParticipants(courseBatch);
    } else {
      userIds = participants;
    }
    if (userIds != null) {
      participantList = getUsersFromDB(userIds);
    }
    if (mentorList != null) {
      processUserDataAndSendMail(mentorList, courseBatch, operationType, JsonKey.MENTOR);
    }
    if (participantList != null) {
      processUserDataAndSendMail(participantList, courseBatch, operationType, JsonKey.PARTICIPANT);
    }
  }

  /*
   * This method call sendMail method with all the required data
   *
   * @param CourseBatch object for course related data
   *
   * @param List<Map<String,Object>> list of user
   *
   * @param operationType String for specifying the operation type ie: add/remove
   */
  private void processUserDataAndSendMail(
      List<Map<String, Object>> userList,
      CourseBatch courseBatch,
      String operationType,
      String userType) {

    for (Map<String, Object> user : userList) {
      Map<String, Object> requestMap = new HashMap<String, Object>();
      requestMap = this.createRequestMap(courseBatch);
      Map<String, String> userData = null;
      if (operationType.equals(JsonKey.ADD)) {
        userData = getUserData(user, JsonKey.ADD);
        if (userData != null) {
          if (userType.equals(JsonKey.PARTICIPANT)) {
            userData.put(JsonKey.EMAIL_TEMPLATE_TYPE, JsonKey.BATCH_LEARNER_ENROL);
          } else {
            userData.put(JsonKey.EMAIL_TEMPLATE_TYPE, JsonKey.BATCH_MENTOR_ENROL);
          }
        } else {
          ProjectLogger.log(
              "BatchOperationNotifierActor: processUserDataAndSendMail : User data is NULL",
              LoggerEnum.ERROR.name());
        }
      } else {
        userData = getUserData(user, JsonKey.REMOVE);
        if (userData != null) {
          if (userType.equals(JsonKey.PARTICIPANT)) {
            userData.put(JsonKey.EMAIL_TEMPLATE_TYPE, JsonKey.BATCH_LEARNER_UNENROL);
          } else {
            userData.put(JsonKey.EMAIL_TEMPLATE_TYPE, JsonKey.BATCH_MENTOR_UNENROL);
          }
        } else {
          ProjectLogger.log(
              "BatchOperationNotifierActor: processUserDataAndSendMail : User data is NULL",
              LoggerEnum.ERROR.name());
        }
      }
      if (user.get(JsonKey.USER_ID) != null
          && StringUtils.isNotBlank((String) user.get(JsonKey.USER_ID))) {
        requestMap.put(JsonKey.RECIPIENT_USERIDS, (String) user.get(JsonKey.USER_ID));
      }
      sendMail(userData, requestMap);
    }
  }

  /*
   * This method takes courseBatch object as parameter and process update
   * batch related operation notifications
   *
   * @param courseBatch course and email related data before update
   *
   * @param courseBatchNew course and email related data after update
   */
  @SuppressWarnings("unchecked")
  private void batchUpdateOperationNotifier(
      CourseBatch courseBatch, Map<String, Object> requestMap) {

    if (requestMap.get(JsonKey.REMOVED_MENTORS) != null
        || requestMap.get(JsonKey.REMOVED_PARTICIPANTS) != null) {

      sendEmailNotification(
          courseBatch,
          (List<String>) requestMap.get(JsonKey.REMOVED_MENTORS),
          (List<String>) requestMap.get(JsonKey.REMOVED_PARTICIPANTS),
          JsonKey.REMOVE);
    }
    if (requestMap.get(JsonKey.ADDED_MENTORS) != null
        || requestMap.get(JsonKey.ADDED_PARTICIPANTS) != null) {

      sendEmailNotification(
          courseBatch,
          (List<String>) requestMap.get(JsonKey.ADDED_MENTORS),
          (List<String>) requestMap.get(JsonKey.ADDED_PARTICIPANTS),
          JsonKey.ADD);
    }
  }

  /*
   * This method takes courseBatch object as parameter and returns a map which
   * contains required data for sending email returns Map<String,Object>'
   *
   * @param CourseBatch object course and email related data
   *
   * @return Map<String, Object> which have required data for email notification
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> createRequestMap(CourseBatch courseBatch) {
    Map<String, Object> courseBatchObject = new ObjectMapper().convertValue(courseBatch, Map.class);
    Map<String, String> additionalCourseInfo =
        (Map<String, String>) courseBatchObject.get(JsonKey.COURSE_ADDITIONAL_INFO);
    Map<String, Object> requestMap = new HashMap<String, Object>();
    requestMap.put(JsonKey.ORG_NAME, courseBatchObject.get(JsonKey.ORG_NAME));
    requestMap.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.COURSE_LOGO_URL));
    requestMap.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.COURSE_NAME));
    requestMap.put(JsonKey.START_DATE, courseBatchObject.get(JsonKey.START_DATE));
    requestMap.put(JsonKey.END_DATE, courseBatchObject.get(JsonKey.END_DATE));
    requestMap.put(JsonKey.COURSE_ID, courseBatchObject.get(JsonKey.COURSE_ID));
    requestMap.put(JsonKey.NAME, courseBatch.getName());
    return requestMap;
  }

  /*
   * @param Map<String, Object> data for populating user object
   *
   * @param operationType String for operation type
   *
   * @return map<String, String>
   */
  private Map<String, String> getUserData(Map<String, Object> data, String operationType) {
    Map<String, String> user = new HashMap<String, String>();
    user.put(JsonKey.FIRST_NAME, data.get(JsonKey.FIRST_NAME).toString());
    user.put(JsonKey.EMAIL, data.get(JsonKey.EMAIL).toString());
    if (operationType.equalsIgnoreCase(JsonKey.ADD)) {
      user.put(JsonKey.SUBJECT, JsonKey.COURSE_INVITATION);
    } else if (operationType.equalsIgnoreCase(JsonKey.REMOVE)) {
      user.put(JsonKey.SUBJECT, JsonKey.UNENROLL_FROM_COURSE_BATCH);
    }
    return user;
  }

  /*
   * This method is for sending e-mails for one at a time, EmailServiceActor will
   * be called for sending email
   *
   * @param Map<String, String> user will have user data
   *
   * @param Map<String, Object> RequestMap for email data.
   */
  private void sendMail(Map<String, String> user, Map<String, Object> requestMap) {
    requestMap.put(JsonKey.FIRST_NAME, user.get(JsonKey.FIRST_NAME));
    requestMap.put(JsonKey.EMAIL_TEMPLATE_TYPE, user.get(JsonKey.EMAIL_TEMPLATE_TYPE));
    requestMap.put(JsonKey.RECIPIENT_EMAILS, new String[] {user.get(JsonKey.EMAIL)});
    requestMap.put(JsonKey.SUBJECT, user.get(JsonKey.SUBJECT));
    requestMap.put(JsonKey.REQUEST, BackgroundOperations.emailService.name());
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
          "CourseBatchNotificationActor: sendMail using emailService client failed **** " + e,
          LoggerEnum.ERROR);
    }
  }

  /*
   * This method takes CourseBatch object and returns list of participants
   *
   * @param CourseBatch object
   *
   * @returns list of users
   */
  private List<String> getParticipants(CourseBatch courseBatch) {
    List<String> usersIds = null;
    Map<String, Boolean> participants = courseBatch.getParticipant();
    if (participants != null) {
      usersIds = new ArrayList<>();
      Set<String> keys = participants.keySet();
      for (String user : keys) {
        if (participants.get(user)) {
          usersIds.add(user);
        }
      }
    }
    return usersIds;
  }

  /*
   * This method takes List<String>userIds and returns List<Map<User,Objects>> of
   * participants
   *
   * @param List<String> userIds list of userIds
   *
   * @return List<Map<String, Object>> map of user related data.
   */
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
}
