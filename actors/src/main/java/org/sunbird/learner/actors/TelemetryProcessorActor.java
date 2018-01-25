package org.sunbird.learner.actors;

import akka.actor.UntypedAbstractActor;
import java.util.List;
import java.util.Map;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.ActorUtil;
import org.sunbird.telemetry.collector.TelemetryDataAssembler;
import org.sunbird.telemetry.util.lmaxdisruptor.LMAXWriter;

/**
 * Created by arvind on 8/1/18.
 */
public class TelemetryProcessorActor extends UntypedAbstractActor {

  private LMAXWriter lmaxWriter = LMAXWriter.getInstance();

  @Override
  public void onReceive(Object message) throws Throwable {

    if (message instanceof Request) {
      Request actorMessage = (Request) message;
      lmaxWriter.submitMessage(actorMessage);
    }

  }
}
