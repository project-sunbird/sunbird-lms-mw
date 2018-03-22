package org.sunbird.util.lmaxdisruptor;

import com.lmax.disruptor.EventFactory;

/**
 * 
 * @author Manzarul
 *
 */
public class WriteEventFactory implements EventFactory<TelemetryEvent> {
  public TelemetryEvent newInstance() {
    return new TelemetryEvent();
  }
}
