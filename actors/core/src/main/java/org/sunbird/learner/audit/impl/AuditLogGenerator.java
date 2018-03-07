package org.sunbird.learner.audit.impl;

import java.util.HashMap;
import java.util.Map;

import org.sunbird.common.models.util.AuditLog;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AuditLogGenerator {

	private static ObjectMapper mapper = new ObjectMapper();

	private AuditLogGenerator() {
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> generateLogs(Map<String, Object> requestData) {
		AuditLog auditLog = new AuditLog();
		auditLog.setObjectType((String) requestData.get(JsonKey.OBJECT_TYPE));
		auditLog.setObjectId((String) requestData.get(JsonKey.OBJECT_ID));
		auditLog.setOperationType((String) requestData.get(JsonKey.OPERATION_TYPE));
		auditLog.setUserId((String) requestData.get(JsonKey.USER_ID));
		auditLog.setRequestId((String) requestData.get(JsonKey.REQ_ID));
		auditLog.setDate(ProjectUtil.getFormattedDate());
		Map<String, Object> logRecord = new HashMap<>();
		if (requestData.containsKey(JsonKey.LOG_RECORD)) {
			logRecord = (Map<String, Object>) requestData.get(JsonKey.LOG_RECORD);
		}
		auditLog.setLogRecord(logRecord);
		Map<String, Object> auditLogMap = new HashMap<>();
		try {
			auditLogMap = mapper.convertValue(auditLog, Map.class);
		} catch (Exception e) {
			ProjectLogger.log("Error occured", e);
		}
		return auditLogMap;
	}

}
