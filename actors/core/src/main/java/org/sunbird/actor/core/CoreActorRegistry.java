package org.sunbird.actor.core;

import org.sunbird.actor.background.ChannelRegistrationActor;
import org.sunbird.actor.background.TelemetryProcessorActor;
import org.sunbird.learner.actors.BackGroundServiceActor;
import org.sunbird.learner.actors.BackgroundJobManager;
import org.sunbird.learner.actors.bulkupload.BulkUploadBackGroundJobActor;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.learner.audit.impl.ActorAuditLogServiceImpl;
import org.sunbird.metrics.actors.CourseMetricsBackgroundActor;
import org.sunbird.metrics.actors.MetricsBackGroundJobActor;
import org.sunbird.metrics.actors.OrganisationMetricsBackgroundActor;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class CoreActorRegistry {

	public CoreActorRegistry() {
		BackgroundJobManager.init();
		BulkUploadBackGroundJobActor.init();
		MetricsBackGroundJobActor.init();
		ActorAuditLogServiceImpl.init();
		OrganisationMetricsBackgroundActor.init();
		CourseMetricsBackgroundActor.init();
		EmailServiceActor.init();
		BackGroundServiceActor.init();
		ChannelRegistrationActor.init();
		TelemetryProcessorActor.init();
	}
}
