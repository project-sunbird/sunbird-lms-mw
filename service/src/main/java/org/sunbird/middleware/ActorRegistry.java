package org.sunbird.middleware;

import org.sunbird.actor.core.CoreActorRegistry;
import org.sunbird.badge.BadgeActorRegistry;
import org.sunbird.user.UserActorRegistry;

public class ActorRegistry {

	public ActorRegistry() {
		new CoreActorRegistry();
		new BadgeActorRegistry();
		new UserActorRegistry();
	}
}
