package org.sunbird.learner.util;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.sunbird.common.message.broker.factory.MessageBrokerFactory;
import org.sunbird.common.message.broker.inf.MessageBroker;
import org.sunbird.common.message.broker.model.EventMessage;
import org.sunbird.common.models.util.JsonKey;

public class GeneratorAndSendEventUtil {
  private static final String AUDIT = "audit";

  private static ObjectMapper mapper = new ObjectMapper();
  private static MessageBroker messageBroker = MessageBrokerFactory.getInstance();

  public static String generateEvent() {
    // Based on Request and targetObject it will generate event

    return null;
  }

  public static void userUpdateEvent(String userId, ActorRef actorRef) {

    Map<String, Object> userDetails = Util.getUserDetails(userId, actorRef);
    EventMessage msg =
        new EventMessage(JsonKey.CREATE, JsonKey.TRANSACTIONAL, userDetails, JsonKey.USER);
    messageBroker.send(JsonKey.USER, msg);
  }
}
