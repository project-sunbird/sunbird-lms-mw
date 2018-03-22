/**
 * 
 */
package org.sunbird.services.imp;

import java.util.Map;

import org.sunbird.common.request.Request;
import org.sunbird.services.service.TelemetryDataHandlerService;
import org.sunbird.util.lmaxdisruptor.LMAXWriter;
import org.sunbird.util.lmaxdisruptor.TelemetryEvent;
import org.sunbird.util.lmaxdisruptor.TelemetryEvent.EventData;

/**
 *This is default telemetry handler service.implemented based
 *on lmax disruptor.
 * @author Manzarul
 *
 */
public class TelemetryDefaultHandlerServiceImpl implements TelemetryDataHandlerService {

  @Override
  public void processData(Request request, Map<String, String> headers) {
    
    LMAXWriter lmaxWriter = LMAXWriter.getInstance();
    TelemetryEvent event = new TelemetryEvent();
    EventData data = new TelemetryEvent().new EventData();
    data.setRequest(request);
    data.setHeaders(headers);
    event.setData(data);
    lmaxWriter.submitMessage(event);
  }

}
