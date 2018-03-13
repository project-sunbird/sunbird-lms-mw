package org.sunbird.badge;

import org.sunbird.badge.actors.BadgeAssertionActor;
import org.sunbird.badge.actors.BadgeClassActor;
import org.sunbird.badge.actors.BadgeNotifier;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BadgeActorRegistry {

	public BadgeActorRegistry() {
		BadgeNotifier.init();
		BadgeClassActor.init();
		BadgeAssertionActor.init();
	}

}
