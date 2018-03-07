package org.sunbird.badge;

import java.util.Arrays;

import org.sunbird.actor.core.ActorRegistry;
import org.sunbird.badge.actors.BadgeNotifier;

public class BadgeActorRegistry extends ActorRegistry {

	public BadgeActorRegistry() {
		bgActors.put(BadgeNotifier.class, Arrays.asList("assignBadgeMessage", "revokeBadgeMessage"));
		initBackgroundActors();
		initActors();
	}

}
