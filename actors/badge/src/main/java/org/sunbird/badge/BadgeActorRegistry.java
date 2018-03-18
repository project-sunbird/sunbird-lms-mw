package org.sunbird.badge;

import org.sunbird.badge.actors.BadgeAssertionActor;
import org.sunbird.badge.actors.BadgeClassActor;
import org.sunbird.badge.actors.BadgeIssuerActor;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BadgeActorRegistry {

    public BadgeActorRegistry() {
        BadgeClassActor.init();
        BadgeAssertionActor.init();
        BadgeIssuerActor.init();
    }

}
