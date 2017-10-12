package org.sunbird.learner.util.actorutility.impl;

import org.sunbird.learner.actors.RequestRouterActor;
import org.sunbird.learner.util.actorutility.ActorSystem;

/**
 * 
 * @author Amit Kumar
 *
 */
public class LocalActorSystem implements ActorSystem{

  @Override
  public Object initializeActorSystem(String operationType) {
    return RequestRouterActor.routerMap.get(operationType);
  }

}
