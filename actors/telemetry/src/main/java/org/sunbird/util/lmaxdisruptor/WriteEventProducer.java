package org.sunbird.util.lmaxdisruptor;

import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;


/**
 * 
 * @author Manzarul
 *
 */
public class WriteEventProducer {


  private final Disruptor<TelemetryEvent> disruptor;

  public WriteEventProducer(Disruptor<TelemetryEvent> disruptor) {
    this.disruptor = disruptor;
  }

  private static final EventTranslatorOneArg<TelemetryEvent, TelemetryEvent.EventData> TRANSLATOR_ONE_ARG =
      new EventTranslatorOneArg<TelemetryEvent, TelemetryEvent.EventData>() {
        public void translateTo(TelemetryEvent writeEvent, long sequence,
            TelemetryEvent.EventData request) {
          ProjectLogger.log("Inside translator");
          writeEvent.setData(request);
        }
      };

  public void onData(TelemetryEvent.EventData request) {
    ProjectLogger.log("Publishing " + request.toString(), LoggerEnum.INFO.name());
    // publish the message to disruptor
    disruptor.publishEvent(TRANSLATOR_ONE_ARG, request);
  }
}
