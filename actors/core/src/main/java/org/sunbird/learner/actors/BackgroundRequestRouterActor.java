package org.sunbird.learner.actors;

import java.util.HashMap;
import java.util.Map;

import org.sunbird.actor.background.TelemetryProcessorActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.BadgingActorOperations;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.bulkupload.BulkUploadBackGroundJobActor;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.learner.audit.impl.ActorAuditLogServiceImpl;
import org.sunbird.metrics.actors.CourseMetricsBackgroundActor;
import org.sunbird.metrics.actors.MetricsBackGroundJobActor;
import org.sunbird.metrics.actors.OrganisationMetricsBackgroundActor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.routing.FromConfig;

public class BackgroundRequestRouterActor extends UntypedAbstractActor {

	private ActorRef backgroundJobManager;

	private ActorRef bulkUploadBackGroundJobActor;

	private ActorRef metricsBackGroungJobActor;

	private ActorRef auditLogManagementActor;

	private ActorRef organisationMetricsBackgroundActor;

	private ActorRef courseMetricsBackgroundActor;

	private ActorRef emailServiceActor;

	private ActorRef backGroundServiceActor;

	private ActorRef channelRegistrationActor;

	public static Map<String, ActorRef> routerMap = new HashMap<>();

	private static final String BACKGROUND_JOB = "backgroundJobManager";
	private static final String BULK_UPLOAD_BACKGROUND_ACTOR = "bulkUploadBackGroundJobActor";
	private static final String METRICS_BACKGROUND_ACTOR = "metricsBackGroungJobActor";
	private static final String AUDIT_LOG_MGMT_ACTOR = "auditLogManagementActor";
	private static final String ORG_METRICS_BACKGROUND_ACTOR = "organisationMetricsBackgroundActor";
	private static final String COURSE_METRICS_BACKGROUND_ACTOR = "courseMetricsBackgroundActor";
	private static final String EMAIL_SERVICE_ACTOR = "emailServiceActor";
	private static final String BACKGROUND_SERVICE_ACTOR = "backGroundServiceActor";
	private static final String CHANNEL_REG_ACTOR = "channelRegistrationActor";

	/**
	 * constructor to initialize router actor with child actor pool
	 */
	public BackgroundRequestRouterActor() {

		backgroundJobManager = getContext()
				.actorOf(FromConfig.getInstance().props(Props.create(BackgroundJobManager.class)), BACKGROUND_JOB);

		bulkUploadBackGroundJobActor = getContext().actorOf(
				FromConfig.getInstance().props(Props.create(BulkUploadBackGroundJobActor.class)),
				BULK_UPLOAD_BACKGROUND_ACTOR);

		metricsBackGroungJobActor = getContext().actorOf(
				FromConfig.getInstance().props(Props.create(MetricsBackGroundJobActor.class)),
				METRICS_BACKGROUND_ACTOR);

		auditLogManagementActor = getContext().actorOf(
				FromConfig.getInstance().props(Props.create(ActorAuditLogServiceImpl.class)), AUDIT_LOG_MGMT_ACTOR);

		organisationMetricsBackgroundActor = getContext().actorOf(
				FromConfig.getInstance().props(Props.create(OrganisationMetricsBackgroundActor.class)),
				ORG_METRICS_BACKGROUND_ACTOR);

		courseMetricsBackgroundActor = getContext().actorOf(
				FromConfig.getInstance().props(Props.create(CourseMetricsBackgroundActor.class)),
				COURSE_METRICS_BACKGROUND_ACTOR);

		emailServiceActor = getContext().actorOf(FromConfig.getInstance().props(Props.create(EmailServiceActor.class)),
				EMAIL_SERVICE_ACTOR);

		backGroundServiceActor = getContext().actorOf(
				FromConfig.getInstance().props(Props.create(BackGroundServiceActor.class)), BACKGROUND_SERVICE_ACTOR);

		channelRegistrationActor = getContext().actorOf(
				FromConfig.getInstance().props(Props.create(ChannelRegistrationActor.class)), CHANNEL_REG_ACTOR);

		initializeRouterMap();
	}

	/**
	 * Initialize the map with operation as key and corresponding router as value.
	 */
	private void initializeRouterMap() {

		routerMap.put(ActorOperations.PROCESS_BULK_UPLOAD.getValue(), bulkUploadBackGroundJobActor);
		routerMap.put(ActorOperations.FILE_GENERATION_AND_UPLOAD.getValue(), metricsBackGroungJobActor);
		routerMap.put(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue(), backgroundJobManager);
		routerMap.put(ActorOperations.UPDATE_USER_ROLES_ES.getValue(), backgroundJobManager);
		routerMap.put(ActorOperations.PROCESS_DATA.getValue(), metricsBackGroungJobActor);
		routerMap.put(ActorOperations.FILE_GENERATION_AND_UPLOAD.getValue(), metricsBackGroungJobActor);
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
		routerMap.put(ActorOperations.SEARCH_AUDIT_LOG.getValue(), auditLogManagementActor);
		routerMap.put(ActorOperations.PROCESS_AUDIT_LOG.getValue(), auditLogManagementActor);
		routerMap.put(ActorOperations.ORG_CREATION_METRICS_DATA.getValue(), organisationMetricsBackgroundActor);
		routerMap.put(ActorOperations.ORG_CONSUMPTION_METRICS_DATA.getValue(), organisationMetricsBackgroundActor);
		routerMap.put(ActorOperations.COURSE_PROGRESS_METRICS_DATA.getValue(), courseMetricsBackgroundActor);
		routerMap.put(ActorOperations.EMAIL_SERVICE.getValue(), emailServiceActor);
		routerMap.put(ActorOperations.UPDATE_USER_COUNT_TO_LOCATIONID.getValue(), backGroundServiceActor);
		routerMap.put(ActorOperations.REG_CHANNEL.getValue(), channelRegistrationActor);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Request) {
			ProjectLogger.log("BackgroundRequestRouterActor onReceive called");
			Request actorMessage = (Request) message;
			org.sunbird.common.request.ExecutionContext.setRequestId(actorMessage.getRequestId());
			ActorRef ref = routerMap.get(actorMessage.getOperation());
			if (null != ref) {
				ref.tell(message, ActorRef.noSender());
			} else {
				ProjectLogger.log("UNSUPPORTED OPERATION TYPE");
				ProjectCommonException exception = new ProjectCommonException(
						ResponseCode.invalidOperationName.getErrorCode(),
						ResponseCode.invalidOperationName.getErrorMessage(),
						ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, ActorRef.noSender());
			}
		} else {
			ProjectLogger.log("UNSUPPORTED MESSAGE");
			ProjectCommonException exception = new ProjectCommonException(
					ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(),
					ResponseCode.SERVER_ERROR.getResponseCode());
			sender().tell(exception, ActorRef.noSender());
		}

	}
}
