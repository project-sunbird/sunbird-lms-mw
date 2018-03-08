package org.sunbird.learner.util.actorutility.impl;

import org.sunbird.learner.actors.BackgroundRequestRouterActor;
import org.sunbird.learner.util.actorutility.ActorSystem;

/**
 * 
 * @author Amit Kumar
 *
 */
public final class LocalActorSystem implements ActorSystem {

	private static ActorSystem actorSystem = null;

	private LocalActorSystem() {
	}

	public static ActorSystem getInstance() {
		if (null == actorSystem) {
			actorSystem = new LocalActorSystem();
		}
		return actorSystem;
	}

	@Override
	public Object initializeActorSystem(String operationType) {
		return BackgroundRequestRouterActor.routerMap.get(operationType);
	}

}
