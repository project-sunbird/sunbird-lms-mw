/**
 * 
 */
package org.sunbird.util.lmaxdisruptor;

import java.util.Map;

import org.sunbird.common.request.Request;



/**
 * @author Manzarul 
 * This class will hold complete Telemetry request data.
 */
public class TelemetryEvent {
  private EventData data;

  public EventData getData() {
    return data;
  }


  public void setData(EventData data) {
    this.data = data;
  }


  public class EventData {
    private Request request;
    private Map<String, String> headers;

    public void setRequest(Request request) {
      this.request = request;
    }

    public Request getRequest() {
      return request;
    }

    public Map<String, String> getHeaders() {
      return headers;
    }

    public void setHeaders(Map<String, String> headers) {
      this.headers = headers;
    }

  }
}
