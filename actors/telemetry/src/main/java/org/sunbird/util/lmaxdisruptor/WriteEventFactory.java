package org.sunbird.util.lmaxdisruptor;

import org.sunbird.common.request.Request;

import com.lmax.disruptor.EventFactory;

/**
 * 
 * @author Manzarul
 *
 */
public class WriteEventFactory implements EventFactory<Request> {
  public Request newInstance() {
    return new Request();
  }
}
