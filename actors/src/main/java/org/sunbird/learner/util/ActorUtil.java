package org.sunbird.learner.util;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.actorutility.ActorSystemFactory;

/**
 * 
 * @author Amit Kumar
 *
 */
public class ActorUtil {
  
  private ActorUtil(){}
  
  public static void tell(Request request){
    Object obj = ActorSystemFactory.getActorSystem().initializeActorSystem(request.getOperation());
     if(obj instanceof ActorRef){
       ProjectLogger.log("In ActorUtil(org.sunbird.learner.util) Actor ref is running "+((ActorSelection)obj));
       ((ActorRef)obj).tell(request, ActorRef.noSender());
     } else {
       ProjectLogger.log("In ActorUtil(org.sunbird.learner.util) Actor selection is running "+((ActorSelection)obj));
       ((ActorSelection)obj).tell(request, ActorRef.noSender());
     }
  }
}
