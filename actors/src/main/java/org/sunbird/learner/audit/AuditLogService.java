package org.sunbird.learner.audit;

import java.util.Map;

import org.sunbird.common.request.Request;

public interface AuditLogService {

  public void process(Request actorMessage);

  public void save(Map<String, Object> requestedData);

}
