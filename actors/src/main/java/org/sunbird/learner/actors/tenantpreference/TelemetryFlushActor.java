package org.sunbird.learner.actors.tenantpreference;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.sunbird.telemetry.util.TelemetryDispatcher;
import org.sunbird.telemetry.util.TelemetryDispatcherFactory;

import akka.actor.UntypedAbstractActor;

/**
 * Created by arvind on 9/1/18.
 */
public class TelemetryFlushActor extends UntypedAbstractActor {


  /*
   * TODO: move to the util class so all threads will access the same queue only bcoz this queue can
   * have multiple instances ...
   */
  private Queue<Object> queue = new ConcurrentLinkedQueue<>();
  private int thresholdSize = 10;

  private TelemetryDispatcher telemetryDispatcher = TelemetryDispatcherFactory.get("EK-STEP");

  @Override
  public void onReceive(Object message) throws Throwable {
    writeToQueue(message);
  }

  private void writeToQueue(Object message) {
    queue.offer(message);

    if (queue.size() >= thresholdSize) {
      List list = new ArrayList();
      for (int i = 1; i <= thresholdSize; i++) {
        Object obj = queue.poll();
        if (obj == null) {
          break;
        } else {
          list.add(obj);
        }
      }
      telemetryDispatcher.dispatchTelemetryEvent(list);
    }

  }


}
