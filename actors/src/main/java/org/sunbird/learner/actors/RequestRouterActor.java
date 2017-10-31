package org.sunbird.learner.actors;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.assessment.AssessmentItemActor;
import org.sunbird.learner.actors.badges.BadgesActor;
import org.sunbird.learner.actors.bulkupload.BulkUploadBackGroundJobActor;
import org.sunbird.learner.actors.bulkupload.BulkUploadManagementActor;
import org.sunbird.learner.actors.bulkupload.UserDataEncryptionDecryptionServiceActor;
import org.sunbird.learner.actors.skill.SkillmanagementActor;
import org.sunbird.learner.actors.fileuploadservice.FileUploadServiceActor;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.learner.actors.recommend.RecommendorActor;
import org.sunbird.learner.actors.search.CourseSearchActor;
import org.sunbird.learner.actors.search.SearchHandlerActor;
import org.sunbird.learner.actors.syncjobmanager.EsSyncActor;
import org.sunbird.learner.actors.tenantpreference.TenantPreferenceManagementActor;
import org.sunbird.learner.audit.impl.ActorAuditLogServiceImpl;
import org.sunbird.learner.util.AuditOperation;
import org.sunbird.learner.util.Util;
import org.sunbird.metrics.actors.CourseMetricsActor;
import org.sunbird.metrics.actors.MetricsBackGroundJobActor;
import org.sunbird.metrics.actors.OrganisationMetricsActor;
import org.sunbird.metrics.actors.UserMetricsActor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.routing.FromConfig;
import akka.util.Timeout;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * @author Amit Kumar
 * @author  arvind .
 * Class to initialize and select the appropriate actor on the basis of message type .
 */
public class RequestRouterActor extends UntypedAbstractActor {

  private ActorRef courseEnrollmentActorRouter;
  private ActorRef learnerStateActorRouter;
  private ActorRef learnerStateUpdateActorRouter;
  private ActorRef userManagementRouter;
  private ActorRef courseManagementRouter;
  private ActorRef pageManagementRouter;
  private ActorRef organisationManagementRouter;
  private ActorRef recommendorActorRouter;
  private ActorRef backgroundJobManager;
  private ActorRef courseSearchActorRouter;
  private ActorRef assessmentItemActor;
  private ActorRef searchHandlerActor;
  private ActorRef bulkUploadManagementActor;
  private ActorRef bulkUploadBackGroundJobActor;
  private ActorRef courseBatchActor;
  private ActorRef userMetricsRouter;
  private ActorRef esSyncActor;
  private ActorRef emailServiceActor;
  private ActorRef fileUploadServiceActor;
  private ActorRef notesActor;
  private ActorRef auditLogManagementActor;
  private ActorRef userDataEncryptionDecryptionServiceActor;
  private ActorRef metricsBackGroungJobActor;
  private ActorRef schedularActor;
  private ActorRef organisationMetricsRouter;
  private ActorRef courseMetricsRouter;
  private ActorRef badgesActor;
  private ActorRef skillManagementActor;
  private ActorRef tenantPrefManagementActor;

  private ExecutionContext ec;

  public static Map<String, ActorRef> routerMap = new HashMap<>();
  private static final int WAIT_TIME_VALUE = 9;
  private static final String COURSE_ENROLLMENT_ROUTER = "courseEnrollmentRouter";
  private static final String LEARNER_ACTOR_ROUTER = "learnerActorRouter";
  private static final String LEARNER_STATE_ROUTER = "learnerStateRouter";
  private static final String USER_MANAGEMENT_ROUTER = "userManagementRouter";
  private static final String COURSE_MANAGEMENT_ROUTER = "courseManagementRouter";
  private static final String PAGE_MANAGEMENT_ROUTER = "pageManagementRouter";
  private static final String ORGANISATION_MANAGEMENT_ROUTER = "organisationManagementRouter";
  private static final String BACKGROUND_JOB = "backgroundJobManager";
  private static final String COURSE_SEARCH_ACTOR_ROUTER = "courseSearchActorRouter";
  private static final String ASSESSMENT_ITEM_ACTOR_ROUTER = "assessmentItemActor";
  private static final String RECOMMENDOR_ACTOR_ROUTER = "recommendorActorRouter";
  private static final String SEARCH_HANDLER_ACTOR_ROUTER = "searchHandlerActor";
  private static final String BULK_UPLOAD_MGMT_ACTOR = "bulkUploadManagementActor";
  private static final String BULK_UPLOAD_BACKGROUND_ACTOR = "bulkUploadBackGroundJobActor";
  private static final String COURSE_BATCH_MANAGEMENT_ACTOR = "courseBatchActor";
  private static final String ORGANISATION_METRICS_ROUTER = "organisationMetricsRouter";
  private static final String COURSE_METRICS_ROUTER = "courseMetricsRouter";
  private static final String USER_METRICS_ROUTER = "userMetricsRouter";
  private static final String ES_SYNC_ROUTER = "esSyncActor";
  private static final String SCHEDULAR_ACTOR = "schedularActor";
  private static final String EMAIL_SERVICE_ACTOR = "emailServiceActor";
  private static final String FILE_UPLOAD_ACTOR = "fileUploadActor";
  private static final String METRICS_ACKGROUNG_JOB__ACTOR = "metricsBackGroungJobActor";
  private static final String BADGES_ACTOR = "badgesActor";
  private static final String NOTES_ACTOR = "notesActor";
  private static final String AUDIT_LOG_MGMT_ACTOR = "auditLogManagementActor";
  private static final String USER_DATA_ENC_DEC_SERVICE_ACTOR =
      "userDataEncryptionDecryptionServiceActor";
  private static final String SKILL_MANAGEMENT_ACTOR = "skillManagementActor";
  private static final String TENANT_PREFERENCE_MNGT_ACTOR = "tenantPreferenceManagementActor";

  

  /**
   * constructor to initialize router actor with child actor pool
   */
  public RequestRouterActor() {
    courseEnrollmentActorRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(CourseEnrollmentActor.class)),
        COURSE_ENROLLMENT_ROUTER);
    learnerStateActorRouter =
        getContext().actorOf(FromConfig.getInstance().props(Props.create(LearnerStateActor.class)),
            LEARNER_ACTOR_ROUTER);
    learnerStateUpdateActorRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(LearnerStateUpdateActor.class)),
        LEARNER_STATE_ROUTER);
    userManagementRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(UserManagementActor.class)),
        USER_MANAGEMENT_ROUTER);
    courseManagementRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(CourseManagementActor.class)),
        COURSE_MANAGEMENT_ROUTER);
    pageManagementRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(PageManagementActor.class)),
        PAGE_MANAGEMENT_ROUTER);
    organisationManagementRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(OrganisationManagementActor.class)),
        ORGANISATION_MANAGEMENT_ROUTER);
    backgroundJobManager = getContext()
        .actorOf(FromConfig.getInstance().props(Props.create(BackgroundJobManager.class)), BACKGROUND_JOB);
    courseSearchActorRouter =
        getContext().actorOf(FromConfig.getInstance().props(Props.create(CourseSearchActor.class)),
            COURSE_SEARCH_ACTOR_ROUTER);
    assessmentItemActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(AssessmentItemActor.class)),
        ASSESSMENT_ITEM_ACTOR_ROUTER);
    recommendorActorRouter =
        getContext().actorOf(FromConfig.getInstance().props(Props.create(RecommendorActor.class)),
            RECOMMENDOR_ACTOR_ROUTER);
    searchHandlerActor =
        getContext().actorOf(FromConfig.getInstance().props(Props.create(SearchHandlerActor.class)),
            SEARCH_HANDLER_ACTOR_ROUTER);
    bulkUploadManagementActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(BulkUploadManagementActor.class)),
        BULK_UPLOAD_MGMT_ACTOR);
    bulkUploadBackGroundJobActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(BulkUploadBackGroundJobActor.class)),
        BULK_UPLOAD_BACKGROUND_ACTOR);
    courseBatchActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(CourseBatchManagementActor.class)),
        COURSE_BATCH_MANAGEMENT_ACTOR);
    organisationMetricsRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(OrganisationMetricsActor.class)),
        ORGANISATION_METRICS_ROUTER);
    courseMetricsRouter =
        getContext().actorOf(FromConfig.getInstance().props(Props.create(CourseMetricsActor.class)),
            COURSE_METRICS_ROUTER);
    userMetricsRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(UserMetricsActor.class)), USER_METRICS_ROUTER);
    esSyncActor = getContext()
        .actorOf(FromConfig.getInstance().props(Props.create(EsSyncActor.class)), ES_SYNC_ROUTER);
    fileUploadServiceActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(FileUploadServiceActor.class)),
        FILE_UPLOAD_ACTOR);
    schedularActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(SchedularActor.class)), SCHEDULAR_ACTOR);
    emailServiceActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(EmailServiceActor.class)), EMAIL_SERVICE_ACTOR);
    metricsBackGroungJobActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(MetricsBackGroundJobActor.class)),
        METRICS_ACKGROUNG_JOB__ACTOR);

    badgesActor = getContext()
        .actorOf(FromConfig.getInstance().props(Props.create(BadgesActor.class)), BADGES_ACTOR);
    notesActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(NotesManagementActor.class)), NOTES_ACTOR);
    userDataEncryptionDecryptionServiceActor = getContext().actorOf(
        FromConfig.getInstance()
            .props(Props.create(UserDataEncryptionDecryptionServiceActor.class)),
        USER_DATA_ENC_DEC_SERVICE_ACTOR);
    auditLogManagementActor = getContext().actorOf(Props.create(ActorAuditLogServiceImpl.class), AUDIT_LOG_MGMT_ACTOR);
    skillManagementActor = getContext().actorOf(FromConfig.getInstance().props(Props.create(SkillmanagementActor.class)), SKILL_MANAGEMENT_ACTOR);
    tenantPrefManagementActor = getContext().actorOf(FromConfig.getInstance().props(Props.create(TenantPreferenceManagementActor.class)), TENANT_PREFERENCE_MNGT_ACTOR);
    ec = getContext().dispatcher();
    initializeRouterMap();
  }

  /**
   * Initialize the map with operation as key and corresponding router as value.
   */
  private void initializeRouterMap() {
    routerMap.put(ActorOperations.ENROLL_COURSE.getValue(), courseEnrollmentActorRouter);
    routerMap.put(ActorOperations.GET_COURSE.getValue(), learnerStateActorRouter);
    routerMap.put(ActorOperations.GET_CONTENT.getValue(), learnerStateActorRouter);
    routerMap.put(ActorOperations.ADD_CONTENT.getValue(), learnerStateUpdateActorRouter);

    routerMap.put(ActorOperations.CREATE_COURSE.getValue(), courseManagementRouter);
    routerMap.put(ActorOperations.UPDATE_COURSE.getValue(), courseManagementRouter);
    routerMap.put(ActorOperations.PUBLISH_COURSE.getValue(), courseManagementRouter);
    routerMap.put(ActorOperations.DELETE_COURSE.getValue(), courseManagementRouter);

    routerMap.put(ActorOperations.CREATE_USER.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.UPDATE_USER.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.LOGIN.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.LOGOUT.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.CHANGE_PASSWORD.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.GET_PROFILE.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.GET_ROLES.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.DOWNLOAD_USERS.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.FORGOT_PASSWORD.getValue(), userManagementRouter); 
    
    routerMap.put(ActorOperations.CREATE_PAGE.getValue(), pageManagementRouter);
    routerMap.put(ActorOperations.UPDATE_PAGE.getValue(), pageManagementRouter);
    routerMap.put(ActorOperations.GET_PAGE_DATA.getValue(), pageManagementRouter);
    routerMap.put(ActorOperations.GET_PAGE_SETTINGS.getValue(), pageManagementRouter);
    routerMap.put(ActorOperations.GET_PAGE_SETTING.getValue(), pageManagementRouter);
    routerMap.put(ActorOperations.CREATE_SECTION.getValue(), pageManagementRouter);
    routerMap.put(ActorOperations.UPDATE_SECTION.getValue(), pageManagementRouter);
    routerMap.put(ActorOperations.GET_SECTION.getValue(), pageManagementRouter);
    routerMap.put(ActorOperations.GET_ALL_SECTION.getValue(), pageManagementRouter);

    routerMap.put(ActorOperations.CREATE_ORG.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.APPROVE_ORG.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.UPDATE_ORG.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.UPDATE_ORG_STATUS.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.GET_ORG_DETAILS.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.ADD_MEMBER_ORGANISATION.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue(),
        organisationManagementRouter);
    routerMap.put(ActorOperations.GET_ORG_TYPE_LIST.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.CREATE_ORG_TYPE.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.UPDATE_ORG_TYPE.getValue(), organisationManagementRouter);

    routerMap.put(ActorOperations.SEARCH_COURSE.getValue(), courseSearchActorRouter);
    routerMap.put(ActorOperations.GET_COURSE_BY_ID.getValue(), courseSearchActorRouter);

    routerMap.put(ActorOperations.GET_ASSESSMENT.getValue(), assessmentItemActor);
    routerMap.put(ActorOperations.SAVE_ASSESSMENT.getValue(), assessmentItemActor);
    routerMap.put(ActorOperations.GET_RECOMMENDED_COURSES.getValue(), recommendorActorRouter);
    routerMap.put(ActorOperations.APPROVE_USER_ORGANISATION.getValue(),
        organisationManagementRouter);
    routerMap.put(ActorOperations.JOIN_USER_ORGANISATION.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.COMPOSITE_SEARCH.getValue(), searchHandlerActor);
    routerMap.put(ActorOperations.REJECT_USER_ORGANISATION.getValue(),
        organisationManagementRouter);
    routerMap.put(ActorOperations.DOWNLOAD_ORGS.getValue(), organisationManagementRouter);
    routerMap.put(ActorOperations.BLOCK_USER.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.ASSIGN_ROLES.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.UNBLOCK_USER.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.BULK_UPLOAD.getValue(), bulkUploadManagementActor);
    routerMap.put(ActorOperations.PROCESS_BULK_UPLOAD.getValue(), bulkUploadBackGroundJobActor);
    routerMap.put(ActorOperations.CREATE_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.UPDATE_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.ADD_USER_TO_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.REMOVE_USER_FROM_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.GET_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.GET_COURSE_BATCH_DETAIL.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.GET_BULK_OP_STATUS.getValue(), bulkUploadManagementActor);
    routerMap.put(ActorOperations.ORG_CREATION_METRICS.getValue(), organisationMetricsRouter);
    routerMap.put(ActorOperations.ORG_CONSUMPTION_METRICS.getValue(), organisationMetricsRouter);
    routerMap.put(ActorOperations.ORG_CREATION_METRICS_DATA.getValue(), organisationMetricsRouter);
    routerMap.put(ActorOperations.ORG_CONSUMPTION_METRICS_DATA.getValue(),
        organisationMetricsRouter);
    routerMap.put(ActorOperations.COURSE_PROGRESS_METRICS.getValue(), courseMetricsRouter);
    routerMap.put(ActorOperations.COURSE_CREATION_METRICS.getValue(), courseMetricsRouter);
    routerMap.put(ActorOperations.USER_CREATION_METRICS.getValue(), userMetricsRouter);
    routerMap.put(ActorOperations.USER_CONSUMPTION_METRICS.getValue(), userMetricsRouter);

    routerMap.put(ActorOperations.ORG_CREATION_METRICS_REPORT.getValue(),
        organisationMetricsRouter);
    routerMap.put(ActorOperations.ORG_CONSUMPTION_METRICS_REPORT.getValue(),
        organisationMetricsRouter);
    routerMap.put(ActorOperations.COURSE_PROGRESS_METRICS_REPORT.getValue(), courseMetricsRouter);
    routerMap.put(ActorOperations.COURSE_CREATION_METRICS_REPORT.getValue(), courseMetricsRouter);

    routerMap.put(ActorOperations.EMAIL_SERVICE.getValue(), emailServiceActor);

    routerMap.put(ActorOperations.SYNC.getValue(), esSyncActor);
    routerMap.put(ActorOperations.FILE_STORAGE_SERVICE.getValue(), fileUploadServiceActor);
    routerMap.put(ActorOperations.FILE_GENERATION_AND_UPLOAD.getValue(), metricsBackGroungJobActor);
    routerMap.put(ActorOperations.GET_ALL_BADGE.getValue(), badgesActor);
    routerMap.put(ActorOperations.ADD_USER_BADGE.getValue(), badgesActor);
    routerMap.put(ActorOperations.HEALTH_CHECK.getValue(), badgesActor);
    routerMap.put(ActorOperations.ACTOR.getValue(), badgesActor);
    routerMap.put(ActorOperations.ES.getValue(), badgesActor);
    routerMap.put(ActorOperations.CASSANDRA.getValue(), badgesActor);

    routerMap.put(ActorOperations.CREATE_NOTE.getValue(), notesActor);
    routerMap.put(ActorOperations.GET_NOTE.getValue(), notesActor);
    routerMap.put(ActorOperations.SEARCH_NOTE.getValue(), notesActor);
    routerMap.put(ActorOperations.UPDATE_NOTE.getValue(), notesActor);
    routerMap.put(ActorOperations.DELETE_NOTE.getValue(), notesActor);
    routerMap.put(ActorOperations.USER_CURRENT_LOGIN.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.ENCRYPT_USER_DATA.getValue(),
        userDataEncryptionDecryptionServiceActor);
    routerMap.put(ActorOperations.DECRYPT_USER_DATA.getValue(),
        userDataEncryptionDecryptionServiceActor);
    routerMap.put(ActorOperations.GET_MEDIA_TYPES.getValue(), userManagementRouter);
    routerMap.put(ActorOperations.SEARCH_AUDIT_LOG.getValue(), auditLogManagementActor);
    routerMap.put(ActorOperations.PROCESS_AUDIT_LOG.getValue(), auditLogManagementActor);
    routerMap.put(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.UPDATE_USER_ROLES_ES.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.PROCESS_DATA.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.FILE_GENERATION_AND_UPLOAD.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.ADD_USER_BADGE_BKG.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.UPDATE_USR_COURSES_INFO_ELASTIC.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.UPDATE_USR_COURSES_INFO_ELASTIC.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.UPDATE_ORG_INFO_ELASTIC.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.UPDATE_USER_ORG_ES.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.REMOVE_USER_ORG_ES.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.INSERT_USER_NOTES_ES.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.UPDATE_USER_NOTES_ES.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.INSERT_USR_COURSES_INFO_ELASTIC.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.UPDATE_COURSE_BATCH_ES.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.INSERT_COURSE_BATCH_ES.getValue(), backgroundJobManager);
    routerMap.put(ActorOperations.SCHEDULE_BULK_UPLOAD.getValue(), schedularActor);
    routerMap.put(ActorOperations.ADD_SKILL.getValue(), skillManagementActor);
    routerMap.put(ActorOperations.GET_SKILL.getValue(), skillManagementActor);
    routerMap.put(ActorOperations.GET_SKILLS_LIST.getValue(), skillManagementActor);
    routerMap.put(ActorOperations.CREATE_TENANT_PREFERENCE.getValue(), tenantPrefManagementActor);
    routerMap.put(ActorOperations.UPDATE_TENANT_PREFERENCE.getValue(), tenantPrefManagementActor);
    routerMap.put(ActorOperations.GET_TENANT_PREFERENCE.getValue(), tenantPrefManagementActor);
    routerMap.put(ActorOperations.UPDATE_TC_STATUS_OF_USER.getValue(), tenantPrefManagementActor);
  }


  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Request) {
      ProjectLogger.log("Actor selector onReceive called");
      Request actorMessage = (Request) message;
      org.sunbird.common.request.ExecutionContext.setRequestId(actorMessage.getRequestId());
      ActorRef ref = routerMap.get(actorMessage.getOperation());
      if (null != ref) {
        route(ref, actorMessage);
      } else {
        ProjectLogger.log("UNSUPPORTED OPERATION TYPE");
        ProjectCommonException exception =
            new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(),
                ResponseCode.invalidOperationName.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, ActorRef.noSender());
      }
    } else {
      ProjectLogger.log("UNSUPPORTED MESSAGE");
      ProjectCommonException exception =
          new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(),
              ResponseCode.invalidRequestData.getErrorMessage(),
              ResponseCode.SERVER_ERROR.getResponseCode());
      sender().tell(exception, ActorRef.noSender());
    }

  }

  /**
   * method will route the message to corresponding router pass into the argument .
   *
   * @param router
   * @param message
   * @return boolean
   */
  private boolean route(ActorRef router, Request message) {
    long startTime = System.currentTimeMillis();
    ProjectLogger.log("Actor Service Call start  for  api ==" + message.getOperation()
        + " start time " + startTime, LoggerEnum.PERF_LOG);
    Timeout timeout = new Timeout(Duration.create(WAIT_TIME_VALUE, TimeUnit.SECONDS));
    Future<Object> future = Patterns.ask(router, message, timeout);
    ActorRef parent = sender();
    future.onComplete(new OnComplete<Object>() {
      @Override
      public void onComplete(Throwable failure, Object result) {
        if (failure != null) {
          ProjectLogger.log("Actor Service Call Ended on Failure for  api =="
              + message.getOperation() + " end time " + System.currentTimeMillis() + "  Time taken "
              + (System.currentTimeMillis() - startTime), LoggerEnum.PERF_LOG);
          // We got a failure, handle it here
          ProjectLogger.log(failure.getMessage(), failure);
          if (failure instanceof ProjectCommonException) {
            parent.tell(failure, ActorRef.noSender());
          } else {
            ProjectCommonException exception =
                new ProjectCommonException(ResponseCode.internalError.getErrorCode(),
                    ResponseCode.internalError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
            parent.tell(exception, ActorRef.noSender());
          }
        } else {
          ProjectLogger.log("PARENT RESULT IS " + result);
          // We got a result, handle it
          ProjectLogger.log("Actor Service Call Ended on Success for  api =="
              + message.getOperation() + " end time " + System.currentTimeMillis() + "  Time taken "
              + (System.currentTimeMillis() - startTime), LoggerEnum.PERF_LOG);
          parent.tell(result, ActorRef.noSender());
          // Audit log method call
          if(result instanceof Response){
            if (Util.auditLogUrlMap.containsKey(message.getOperation())) {
              AuditOperation auditOperation =
                  (AuditOperation) Util.auditLogUrlMap.get(message.getOperation());
              Map<String, Object> map = new HashMap<>();
              map.put(JsonKey.OPERATION, auditOperation);
              map.put(JsonKey.REQUEST, message);
              map.put(JsonKey.RESPONSE, result);
              Request request = new Request();
              request.setOperation(ActorOperations.PROCESS_AUDIT_LOG.getValue());
              request.setRequest(map);
              auditLogManagementActor.tell(request, self());
            }
          }
        }
      }
    }, ec);
    return true;
  }

}