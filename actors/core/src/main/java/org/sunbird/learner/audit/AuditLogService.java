package org.sunbird.learner.audit;

import java.util.Map;

import org.sunbird.common.request.Request;

public interface AuditLogService {

	void process(Request actorMessage);

	void save(Map<String, Object> requestedData);

}
