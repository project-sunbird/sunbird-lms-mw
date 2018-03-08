package org.sunbird.actor.core;

import org.sunbird.actor.background.ChannelRegistrationActor;
import org.sunbird.actor.background.TelemetryProcessorActor;
import org.sunbird.learner.actors.BackGroundServiceActor;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class CoreActorRegistry {

	public CoreActorRegistry() {
		EmailServiceActor.init();
		BackGroundServiceActor.init();
		ChannelRegistrationActor.init();
		TelemetryProcessorActor.init();
	}
}
