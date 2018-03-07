package org.sunbird.learner.util.actorutility.impl;

import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.learner.actors.RequestRouterActor;
import org.sunbird.learner.util.actorutility.ActorSystem;

import akka.actor.ActorSelection;

/**
 * 
 * @author Amit Kumar
 *
 */
public final class RemoteActorSystem implements ActorSystem {
	private static ActorSystem actorSystem = null;
	private ActorSelection selection = null;

	private RemoteActorSystem() {
	}

	public static ActorSystem getInstance() {
		if (null == actorSystem) {
			actorSystem = new RemoteActorSystem();
		}
		return actorSystem;
	}

	@Override
	public Object initializeActorSystem(String operationType) {
		ProjectLogger.log("RemoteActorSystem initializeActorSystem for background actor method start....");
		if (null == selection) {
			selection = RequestRouterActor.getSelection();
		} else {
			ProjectLogger.log("ActorSelection is not null ::" + selection);
		}
		return selection;
	}
}
