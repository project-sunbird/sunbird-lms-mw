package org.sunbird.actor.core;

import org.sunbird.common.config.ApplicationConfigActor;
import org.sunbird.learner.actors.CourseBatchManagementActor;
import org.sunbird.learner.actors.CourseEnrollmentActor;
import org.sunbird.learner.actors.CourseManagementActor;
import org.sunbird.learner.actors.LearnerStateActor;
import org.sunbird.learner.actors.LearnerStateUpdateActor;
import org.sunbird.learner.actors.NotesManagementActor;
import org.sunbird.learner.actors.OrganisationManagementActor;
import org.sunbird.learner.actors.PageManagementActor;
import org.sunbird.learner.actors.SchedularActor;
import org.sunbird.learner.actors.assessment.AssessmentItemActor;
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
import org.sunbird.metrics.actors.CourseMetricsActor;
import org.sunbird.metrics.actors.OrganisationMetricsActor;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class CoreActorRegistry {

	public CoreActorRegistry() {
		ActorAuditLogServiceImpl.init();
		EmailServiceActor.init();

		// TODO: other actors.
		CourseEnrollmentActor.init();
		LearnerStateActor.init();
		LearnerStateUpdateActor.init();
		CourseManagementActor.init();
		PageManagementActor.init();
		OrganisationManagementActor.init();
		RecommendorActor.init();
		CourseSearchActor.init();
		AssessmentItemActor.init();
		SearchHandlerActor.init();
		BulkUploadManagementActor.init();
		CourseBatchManagementActor.init();
		EsSyncActor.init();
		FileUploadServiceActor.init();
		NotesManagementActor.init();
		UserDataEncryptionDecryptionServiceActor.init();
		SchedularActor.init();
		OrganisationMetricsActor.init();
		CourseMetricsActor.init();
		SkillmanagementActor.init();
		TenantPreferenceManagementActor.init();
		ClientManagementActor.init();
		GeoLocationManagementActor.init();
		KeyCloakSyncActor.init();
		ApplicationConfigActor.init();
		DbOperationActor.init();

	}
}
