package org.sunbird.actor.core;

import org.sunbird.actor.background.ChannelRegistrationActor;
import org.sunbird.actor.background.TelemetryProcessorActor;

public class CoreActorRegistry {

	public CoreActorRegistry() {
		ChannelRegistrationActor.init();
		TelemetryProcessorActor.init();
	}
}
