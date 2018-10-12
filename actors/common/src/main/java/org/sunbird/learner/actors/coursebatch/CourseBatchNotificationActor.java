package org.sunbird.learner.actors.coursebatch;

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
import org.sunbird.actorutil.email.impl.EmailServiceClientImpl;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
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

  private EmailServiceClient emailServiceClient = EmailServiceClientImpl.getInstance();

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
    } else if (requestMap.get(JsonKey.OPERATION_TYPE) != null) {
      batchCreateOperationNotifier(
          (CourseBatch) requestMap.get(JsonKey.COURSE_BATCH),
          (String) requestMap.get(JsonKey.OPERATION_TYPE));
    } else if (requestMap.get(JsonKey.NEW) != null) {
      batchUpdateOperationNotifier(
          (CourseBatch) requestMap.get(JsonKey.COURSE_BATCH),
          (CourseBatch) requestMap.get(JsonKey.NEW));
    }
  }

  /*
   * This method process invite-only batch enrol and unenroll operation
   * notifications mentor and learner add remove Operation
   *
   * @param CourseBatch object for getting course and email related data
   *
   * @param operationType String for specifying the operation type ie: add/remove
   */
  private void batchCreateOperationNotifier(CourseBatch courseBatch, String operationType) {
    if (operationType.equals(JsonKey.ADD)) {
      sendEmailNotification(courseBatch, JsonKey.ADD);
    } else {
      sendEmailNotification(courseBatch, JsonKey.REMOVE);
    }
  }

  /*
   * This method process open batch enroll and unenroll operation notifications
   *
   * @param courseMap , map for getting operation and email related data
   *
   * @param courseBatch object for getting course and email related data
   */
  @SuppressWarnings("unchecked")
  public void batchEnrollOperationNotifier(
      String userId, CourseBatch courseBatch, String operationType) {
    System.out.print(" in batch Enroll Operation");
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
  @SuppressWarnings("unchecked")
  private void sendEmailNotification(CourseBatch courseBatch, String operationType) {
    List<Map<String, Object>> mentorList = null;
    List<Map<String, Object>> participantList = null;
    mentorList = getUsersFromDB(courseBatch.getMentors());
    List<String> userIds = getParticipants(courseBatch);
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
  @SuppressWarnings("unchecked")
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
   * This method takes two courseBatch object as parameter and process update
   * batch related operation notifications
   *
   * @param courseBatchPrev course and email related data before update
   *
   * @param courseBatchNew course and email related data after update
   */
  private void batchUpdateOperationNotifier(
      CourseBatch courseBatchPrev, CourseBatch courseBatchNew) {

    Map<String, Boolean> oldParticipants = courseBatchPrev.getParticipant();
    Map<String, Boolean> newParticipants = courseBatchNew.getParticipant();
    Map<String, Boolean> removedParticipants = courseBatchPrev.getParticipant();
    updateMentor(courseBatchPrev, courseBatchNew);

    if (oldParticipants == null) {
      oldParticipants = new HashMap<>();
      removedParticipants = new HashMap<>();
    }
    if (newParticipants == null) {
      newParticipants = new HashMap<>();
    }
    for (Map.Entry<String, Boolean> entry : oldParticipants.entrySet()) {
      if (newParticipants.containsKey(entry.getKey()) && newParticipants.get(entry.getKey())) {
        newParticipants.remove(entry.getKey());
        removedParticipants.remove(entry.getKey());
      }
    }
    courseBatchPrev.setParticipant(removedParticipants);
    courseBatchNew.setParticipant(newParticipants);
    courseBatchPrev.setMentors(new ArrayList<String>());
    courseBatchNew.setMentors(new ArrayList<String>());
    if (!removedParticipants.isEmpty())
      batchCreateOperationNotifier(courseBatchPrev, JsonKey.REMOVE);
    if (!newParticipants.isEmpty()) batchCreateOperationNotifier(courseBatchNew, JsonKey.ADD);
  }

  private void updateMentor(CourseBatch courseBatchPrev, CourseBatch courseBatchNew) {
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
    removedMentors.removeAll(newMentors);
    newMentors.removeAll(prevMentors);
    courseBatchPrev.setMentors(removedMentors);
    courseBatchNew.setMentors(newMentors);
    courseBatchPrev.setParticipant(new HashMap<String, Boolean>());
    courseBatchNew.setParticipant(new HashMap<String, Boolean>());
    if (!removedMentors.isEmpty()) {
      batchCreateOperationNotifier(courseBatchPrev, JsonKey.REMOVE);
    }
    if (!newMentors.isEmpty()) {
      batchCreateOperationNotifier(courseBatchNew, JsonKey.ADD);
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
    System.out.println("Create Request Map Done");
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
    System.out.println("In get User Data");
    user.put(JsonKey.FIRST_NAME, data.get(JsonKey.FIRST_NAME).toString());
    user.put(JsonKey.EMAIL, data.get(JsonKey.EMAIL).toString());
    if (operationType.equalsIgnoreCase(JsonKey.ADD)) {
      user.put(JsonKey.SUBJECT, JsonKey.COURSE_INVITATION);
    } else if (operationType.equalsIgnoreCase(JsonKey.REMOVE)) {
      user.put(JsonKey.SUBJECT, JsonKey.UNENROLL_FROM_COURSE_BATCH);
    }
    System.out.println("Get User Data Done");
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
    System.out.println("In Send Mail");
    requestMap.put(JsonKey.FIRST_NAME, user.get(JsonKey.FIRST_NAME));
    requestMap.put(JsonKey.EMAIL_TEMPLATE_TYPE, user.get(JsonKey.EMAIL_TEMPLATE_TYPE));
    requestMap.put(JsonKey.RECIPIENT_EMAILS, new String[] {user.get(JsonKey.EMAIL)});
    requestMap.put(JsonKey.SUBJECT, user.get(JsonKey.SUBJECT));
    try {
      Response response =
          emailServiceClient.sendMail(
              getActorRef(BackgroundOperations.emailService.name()), requestMap);
      sender().tell(response, self());
      System.out.println("Send Mail Done");
    } catch (Exception e) {
      sender().tell(e, self());
      ProjectLogger.log(
          "CourseBatchNotificationActor: sendMail using emailService client failed ****",
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
  private List<Map<String, Object>> getUsersFromDB(List<String> userIds) {
    System.out.println("In getUsers From Db");
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
    List<Map<String, Object>> userList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    System.out.println("Get Users From Db Done");
    return userList;
  }
}
