package org.sunbird.learner.actors;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.routing.FromConfig;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.sunbird.common.config.ApplicationConfigActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.assessment.AssessmentItemActor;
import org.sunbird.learner.actors.badges.BadgesActor;
import org.sunbird.learner.actors.badging.BadgeIssuerActor;
import org.sunbird.learner.actors.bulkupload.BulkUploadManagementActor;
import org.sunbird.learner.actors.bulkupload.UserDataEncryptionDecryptionServiceActor;
import org.sunbird.learner.actors.client.ClientManagementActor;
import org.sunbird.learner.actors.fileuploadservice.FileUploadServiceActor;
import org.sunbird.learner.actors.geolocation.GeoLocationManagementActor;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.learner.actors.recommend.RecommendorActor;
import org.sunbird.learner.actors.search.CourseSearchActor;
import org.sunbird.learner.actors.search.SearchHandlerActor;
import org.sunbird.learner.actors.skill.SkillmanagementActor;
import org.sunbird.learner.actors.syncjobmanager.EsSyncActor;
import org.sunbird.learner.actors.syncjobmanager.KeyCloakSyncActor;
import org.sunbird.learner.actors.tenantpreference.TenantPreferenceManagementActor;
import org.sunbird.learner.audit.impl.ActorAuditLogServiceImpl;
import org.sunbird.learner.datapersistence.DbOperationActor;
import org.sunbird.learner.util.AuditOperation;
import org.sunbird.learner.util.Util;
import org.sunbird.metrics.actors.CourseMetricsActor;
import org.sunbird.metrics.actors.OrganisationMetricsActor;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * @author Amit Kumar
 * @author arvind .
 *
 * Class to initialize and select the appropriate actor on the basis of message
 *         type .
 */
public class RequestRouterActor extends UntypedAbstractActor {

  private static ActorSystem system = null;
  private static ActorSelection selection = null;
  private static final String ACTOR_CONFIG_NAME = "RemoteMWConfig";
  private static final String REMOTE_ACTOR_SYSTEM_NAME = "RemoteMiddlewareActorSystem";

  private ActorRef courseEnrollmentActorRouter;
  private ActorRef learnerStateActorRouter;
  private ActorRef learnerStateUpdateActorRouter;
  private ActorRef userManagementRouter;
  private ActorRef courseManagementRouter;
  private ActorRef pageManagementRouter;
  private ActorRef organisationManagementRouter;
  private ActorRef recommendorActorRouter;
  private ActorRef courseSearchActorRouter;
  private ActorRef assessmentItemActor;
  private ActorRef searchHandlerActor;
  private ActorRef bulkUploadManagementActor;
  private ActorRef courseBatchActor;
  private ActorRef esSyncActor;
  private ActorRef emailServiceActor;
  private ActorRef fileUploadServiceActor;
  private ActorRef notesActor;
  private ActorRef auditLogManagementActor;
  private ActorRef userDataEncryptionDecryptionServiceActor;
  private ActorRef schedularActor;
  private ActorRef organisationMetricsRouter;
  private ActorRef courseMetricsRouter;
  private ActorRef badgesActor;
  private ActorRef skillManagementActor;
  private ActorRef tenantPrefManagementActor;
  private ActorRef clientManagementActor;
  private ActorRef geoLocationManagementActor;
  private ActorRef keyCloakSyncActor;
  private ActorRef applicationConfigActor;
  private ActorRef dbOperationActor;
  private ActorRef badgeIssuerActor;

  private ExecutionContext ec;


  public static final Map<String, ActorRef> routerMap = new HashMap<>();
  private static final int WAIT_TIME_VALUE = 9;
  private static final String COURSE_ENROLLMENT_ROUTER = "courseEnrollmentRouter";
  private static final String LEARNER_ACTOR_ROUTER = "learnerActorRouter";
  private static final String LEARNER_STATE_ROUTER = "learnerStateRouter";
  private static final String USER_MANAGEMENT_ROUTER = "userManagementRouter";
  private static final String COURSE_MANAGEMENT_ROUTER = "courseManagementRouter";
  private static final String PAGE_MANAGEMENT_ROUTER = "pageManagementRouter";
  private static final String ORGANISATION_MANAGEMENT_ROUTER = "organisationManagementRouter";
  private static final String COURSE_SEARCH_ACTOR_ROUTER = "courseSearchActorRouter";
  private static final String ASSESSMENT_ITEM_ACTOR_ROUTER = "assessmentItemActor";
  private static final String RECOMMENDOR_ACTOR_ROUTER = "recommendorActorRouter";
  private static final String SEARCH_HANDLER_ACTOR_ROUTER = "searchHandlerActor";
  private static final String BULK_UPLOAD_MGMT_ACTOR = "bulkUploadManagementActor";
  private static final String COURSE_BATCH_MANAGEMENT_ACTOR = "courseBatchActor";
  private static final String ORGANISATION_METRICS_ROUTER = "organisationMetricsRouter";
  private static final String COURSE_METRICS_ROUTER = "courseMetricsRouter";
  private static final String ES_SYNC_ROUTER = "esSyncActor";
  private static final String SCHEDULAR_ACTOR = "schedularActor";
  private static final String EMAIL_SERVICE_ACTOR = "emailServiceActor";
  private static final String FILE_UPLOAD_ACTOR = "fileUploadActor";
  private static final String BADGES_ACTOR = "badgesActor";
  private static final String NOTES_ACTOR = "notesActor";
  private static final String AUDIT_LOG_MGMT_ACTOR = "auditLogManagementActor";
  private static final String USER_DATA_ENC_DEC_SERVICE_ACTOR =
      "userDataEncryptionDecryptionServiceActor";
  private static final String SKILL_MANAGEMENT_ACTOR = "skillManagementActor";
  private static final String TENANT_PREFERENCE_MNGT_ACTOR = "tenantPreferenceManagementActor";
  private static final String CLIENT_MANAGEMENT_ACTOR = "clientManagementActor";
  private static final String GEO_LOCATION_MANAGEMENT_ACTOR = "geoLocationManagementActor";
  private static final String KEYCLOAK_SYNC_ACTOR = "keyCloakSyncActor";
  private static final String APPLICATION_CONFIG_ACTOR = "applicationConfigActor";
  private static final String DBOPERATION_ACTOR = "dbOperationActor";
  private static final String BADGE_ISSUER_ACTOR = "badgeIssuerActor";

  /**
   * @return the system
   */
  public static ActorSystem getSystem() {
    return system;
  }

  /**
   * @param system the system to set
   */
  public static void setSystem(ActorSystem system) {
    RequestRouterActor.system = system;
  }

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
    courseBatchActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(CourseBatchManagementActor.class)),
        COURSE_BATCH_MANAGEMENT_ACTOR);
    organisationMetricsRouter = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(OrganisationMetricsActor.class)),
        ORGANISATION_METRICS_ROUTER);
    courseMetricsRouter =
        getContext().actorOf(FromConfig.getInstance().props(Props.create(CourseMetricsActor.class)),
            COURSE_METRICS_ROUTER);
    esSyncActor = getContext()
        .actorOf(FromConfig.getInstance().props(Props.create(EsSyncActor.class)), ES_SYNC_ROUTER);
    fileUploadServiceActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(FileUploadServiceActor.class)),
        FILE_UPLOAD_ACTOR);
    schedularActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(SchedularActor.class)), SCHEDULAR_ACTOR);
    emailServiceActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(EmailServiceActor.class)), EMAIL_SERVICE_ACTOR);
    badgesActor = getContext()
        .actorOf(FromConfig.getInstance().props(Props.create(BadgesActor.class)), BADGES_ACTOR);
    notesActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(NotesManagementActor.class)), NOTES_ACTOR);
    userDataEncryptionDecryptionServiceActor = getContext().actorOf(
        FromConfig.getInstance()
            .props(Props.create(UserDataEncryptionDecryptionServiceActor.class)),
        USER_DATA_ENC_DEC_SERVICE_ACTOR);
    auditLogManagementActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(ActorAuditLogServiceImpl.class)),
        AUDIT_LOG_MGMT_ACTOR);
    skillManagementActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(SkillmanagementActor.class)),
        SKILL_MANAGEMENT_ACTOR);
    tenantPrefManagementActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(TenantPreferenceManagementActor.class)),
        TENANT_PREFERENCE_MNGT_ACTOR);
    clientManagementActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(ClientManagementActor.class)),
        CLIENT_MANAGEMENT_ACTOR);
    geoLocationManagementActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(GeoLocationManagementActor.class)),
        GEO_LOCATION_MANAGEMENT_ACTOR);
    keyCloakSyncActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(KeyCloakSyncActor.class)),
        KEYCLOAK_SYNC_ACTOR);
    applicationConfigActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(ApplicationConfigActor.class)),
        APPLICATION_CONFIG_ACTOR);
    dbOperationActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(DbOperationActor.class)),
        DBOPERATION_ACTOR);
    badgeIssuerActor = getContext().actorOf(
        FromConfig.getInstance().props(Props.create(BadgeIssuerActor.class)),
        BADGE_ISSUER_ACTOR);
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
    routerMap.put(ActorOperations.PROFILE_VISIBILITY.getValue(), userManagementRouter);

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
    routerMap.put(ActorOperations.CREATE_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.UPDATE_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.ADD_USER_TO_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.REMOVE_USER_FROM_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.GET_BATCH.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.GET_COURSE_BATCH_DETAIL.getValue(), courseBatchActor);
    routerMap.put(ActorOperations.GET_BULK_OP_STATUS.getValue(), bulkUploadManagementActor);
    routerMap.put(ActorOperations.ORG_CREATION_METRICS.getValue(), organisationMetricsRouter);
    routerMap.put(ActorOperations.ORG_CONSUMPTION_METRICS.getValue(), organisationMetricsRouter);
    routerMap.put(ActorOperations.COURSE_PROGRESS_METRICS.getValue(), courseMetricsRouter);
    routerMap.put(ActorOperations.COURSE_CREATION_METRICS.getValue(), courseMetricsRouter);

    routerMap.put(ActorOperations.ORG_CREATION_METRICS_REPORT.getValue(),
        organisationMetricsRouter);
    routerMap.put(ActorOperations.ORG_CONSUMPTION_METRICS_REPORT.getValue(),
        organisationMetricsRouter);
    routerMap.put(ActorOperations.COURSE_PROGRESS_METRICS_REPORT.getValue(), courseMetricsRouter);
    routerMap.put(ActorOperations.COURSE_CREATION_METRICS_REPORT.getValue(), courseMetricsRouter);

    routerMap.put(ActorOperations.EMAIL_SERVICE.getValue(), emailServiceActor);

    routerMap.put(ActorOperations.SYNC.getValue(), esSyncActor);
    routerMap.put(ActorOperations.FILE_STORAGE_SERVICE.getValue(), fileUploadServiceActor);
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
    routerMap.put(ActorOperations.SCHEDULE_BULK_UPLOAD.getValue(), schedularActor);
    routerMap.put(ActorOperations.ADD_SKILL.getValue(), skillManagementActor);
    routerMap.put(ActorOperations.GET_SKILL.getValue(), skillManagementActor);
    routerMap.put(ActorOperations.GET_SKILLS_LIST.getValue(), skillManagementActor);
    routerMap.put(ActorOperations.CREATE_TENANT_PREFERENCE.getValue(), tenantPrefManagementActor);
    routerMap.put(ActorOperations.UPDATE_TENANT_PREFERENCE.getValue(), tenantPrefManagementActor);
    routerMap.put(ActorOperations.GET_TENANT_PREFERENCE.getValue(), tenantPrefManagementActor);
    routerMap.put(ActorOperations.UPDATE_TC_STATUS_OF_USER.getValue(), tenantPrefManagementActor);
    routerMap.put(ActorOperations.REGISTER_CLIENT.getValue(), clientManagementActor);
    routerMap.put(ActorOperations.UPDATE_CLIENT_KEY.getValue(), clientManagementActor);
    routerMap.put(ActorOperations.GET_CLIENT_KEY.getValue(), clientManagementActor);
    routerMap.put(ActorOperations.GET_GEO_LOCATION.getValue(), geoLocationManagementActor);
    routerMap.put(ActorOperations.CREATE_GEO_LOCATION.getValue(), geoLocationManagementActor);
    routerMap.put(ActorOperations.UPDATE_GEO_LOCATION.getValue(), geoLocationManagementActor);
    routerMap.put(ActorOperations.DELETE_GEO_LOCATION.getValue(), geoLocationManagementActor);
    routerMap.put(ActorOperations.SEND_NOTIFICATION.getValue(), geoLocationManagementActor);
    routerMap.put(ActorOperations.SYNC_KEYCLOAK.getValue(), keyCloakSyncActor);
    routerMap.put(ActorOperations.UPDATE_SYSTEM_SETTINGS.getValue(), applicationConfigActor);
    routerMap.put(ActorOperations.GET_USER_COUNT.getValue(), geoLocationManagementActor);
    
    routerMap.put(ActorOperations.CREATE_DATA.getValue(), dbOperationActor);
    routerMap.put(ActorOperations.UPDATE_DATA.getValue(), dbOperationActor);
    routerMap.put(ActorOperations.DELETE_DATA.getValue(), dbOperationActor);
    routerMap.put(ActorOperations.READ_DATA.getValue(), dbOperationActor);
    routerMap.put(ActorOperations.READ_ALL_DATA.getValue(), dbOperationActor);
    routerMap.put(ActorOperations.SEARCH_DATA.getValue(), dbOperationActor);
    routerMap.put(ActorOperations.GET_METRICS.getValue(), dbOperationActor);
    routerMap.put(ActorOperations.CREATE_BADGE_ISSUER.getValue(), badgeIssuerActor);
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
            if (result instanceof Response && Util.auditLogUrlMap.containsKey(message.getOperation())) {
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
    }, ec);
    return true;
  }

  public static void createConnectionForBackgroundActors() {
    String path = PropertiesCache.getInstance().getProperty("background.remote.actor.path");
    ActorSystem system = null;
    String bkghost = System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_IP);
    String bkgport = System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_PORT);
    Config con = null;
    if ("local"
        .equalsIgnoreCase(PropertiesCache.getInstance().getProperty("api_actor_provider"))) {
      con = ConfigFactory.load().getConfig(ACTOR_CONFIG_NAME);
      system = ActorSystem.create(REMOTE_ACTOR_SYSTEM_NAME, con);
    }else{
      system = RequestRouterActor.getSystem();
    }
    try {
      if (!ProjectUtil.isStringNullOREmpty(bkghost) && !ProjectUtil.isStringNullOREmpty(bkgport)) {
        ProjectLogger.log("value is taking from system env");
        path = MessageFormat.format(
            PropertiesCache.getInstance().getProperty("background.remote.actor.env.path"),
            System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_IP),
            System.getenv(JsonKey.BKG_SUNBIRD_ACTOR_SERVICE_PORT));
      }
      ProjectLogger.log("Actor path is ==" + path, LoggerEnum.INFO.name());
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    selection = system.actorSelection(path);
    ProjectLogger.log("ActorUtility selection reference    : " + selection);
  }

  /**
   * @return the selection
   */
  public static ActorSelection getSelection() {
    if (null == selection) {
      createConnectionForBackgroundActors();
    }
    return selection;
  }

}
