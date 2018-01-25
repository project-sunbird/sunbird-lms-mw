package org.sunbird.learner.actors.tenantpreference;

import akka.actor.UntypedAbstractActor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.telemetry.util.TelemetryDispatcher;
import org.sunbird.telemetry.util.TelemetryDispatcherFactory;

/**
 * Created by arvind on 9/1/18.
 */
public class TelemetryFlushActor extends UntypedAbstractActor {


  /*TODO: move to the util class so all threads will access the same queue only bcoz this queue can have multiple instances ...*/
  Queue<Object> queue = new ConcurrentLinkedQueue<Object>();
  int thresholdSize = 10;

  TelemetryDispatcher telemetryDispatcher = TelemetryDispatcherFactory.get("EK-STEP");

  @Override
  public void onReceive(Object message) throws Throwable {
    writeToQueue(message);
  }

  private void writeToQueue(Object message) {
    queue.offer(message);

    if(queue.size()>=thresholdSize){
      List list = new ArrayList();
      for(int i=1;i<=thresholdSize;i++){
        Object obj = queue.poll();
        if(obj == null){
          break;
        }else {
          list.add(obj);
        }
      }
      telemetryDispatcher.dispatchTelemetryEvent(list);
    }

  }


}
