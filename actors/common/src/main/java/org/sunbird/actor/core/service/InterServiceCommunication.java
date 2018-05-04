package org.sunbird.actor.core.service;

import org.sunbird.common.request.Request;

/** Created by arvind on 24/4/18. */
public interface InterServiceCommunication {

  public Object getResponse(Request request, String operation);
}
