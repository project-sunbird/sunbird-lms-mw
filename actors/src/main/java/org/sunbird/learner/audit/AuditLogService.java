package org.sunbird.learner.audit;

import java.util.Map;

public interface AuditLogService {

  public void process(Map<String, Object> requestedData);

  public void save(Map<String, Object> requestedData);

}
