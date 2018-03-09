package org.sunbird.badge;

import org.sunbird.badge.actors.BadgeAssertion;
import org.sunbird.badge.actors.BadgeNotifier;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BadgeActorRegistry {

	public BadgeActorRegistry() {
		BadgeNotifier.init();
		BadgeAssertion.init();
	}

}
