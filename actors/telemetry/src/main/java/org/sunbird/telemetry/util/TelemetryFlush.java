package org.sunbird.telemetry.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.lang.StringUtils;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.util.lmaxdisruptor.LMAXWriter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by arvind on 11/1/18.
 */
public class TelemetryFlush {

	private Queue<String> queue = new ConcurrentLinkedQueue<>();
	private int thresholdSize = 20;
	private static ObjectMapper mapper = new ObjectMapper();
	private static TelemetryFlush telemetryFlush;

	public static TelemetryFlush getInstance() {
		if (telemetryFlush == null) {
			synchronized (TelemetryFlush.class) {
				if (telemetryFlush == null) {
					telemetryFlush = new TelemetryFlush();
				}
			}
		}
		return telemetryFlush;
	}

	public TelemetryFlush() {
		String queueThreshold = PropertiesCache.getInstance().getProperty(JsonKey.TELEMETRY_QUEUE_THRESHOLD_VALUE);
		if (!StringUtils.isBlank(queueThreshold)
				&& !queueThreshold.equalsIgnoreCase(JsonKey.TELEMETRY_QUEUE_THRESHOLD_VALUE)) {
			try {
				this.thresholdSize = Integer.parseInt(queueThreshold.trim());
			} catch (Exception ex) {
				ProjectLogger.log("Threshold size from config is not integer", ex);
			}
		}
	}

	public void flushTelemetry(String message) {
		writeToQueue(message);
	}

	private void writeToQueue(String message) {
		queue.offer(message);
		if (queue.size() >= thresholdSize) {
			List<String> list = new ArrayList<>();
			for (int i = 1; i <= thresholdSize; i++) {
				String obj = queue.poll();
				if (obj == null) {
					break;
				} else {
					list.add(obj);
				}
			}
			Request req = createTelemetryRequest(list);
			LMAXWriter.getInstance().submitMessage(req);
		}
	}

	private Request createTelemetryRequest(List<String> eventList) {
		Request req = null;
		try {
			List<Map<String, Object>> jsonList = mapper.readValue(eventList.toString(),
					new TypeReference<List<Map<String, Object>>>() {
					});
			Map<String, Object> map = new HashMap<>();
			map.put(JsonKey.ETS, System.currentTimeMillis());
			map.put(JsonKey.EVENTS, jsonList);
			req = new Request();
			req.getRequest().putAll(map);
			return req;
		} catch (Exception e) {
			ProjectLogger.log("Failed to create request for telemetry flush.", e);
		}
		return req;
	}

}
