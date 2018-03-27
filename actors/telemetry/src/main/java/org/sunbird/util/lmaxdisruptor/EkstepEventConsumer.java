package org.sunbird.util.lmaxdisruptor;

import com.google.gson.Gson;
import com.lmax.disruptor.EventHandler;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.TelemetryV3Request;

/**
 * This class will send telemetry data to Ekstep.
 * 
 * @author Manzarul
 *
 */
public class EkstepEventConsumer implements EventHandler<Request> {

	@Override
	public void onEvent(Request req, long sequence, boolean endOfBatch) throws Exception {
		if (req != null) {
			Map<String, Object> reqMap = req.getRequest();
			String contentEncoding = (String) reqMap.get(JsonKey.CONTENT_ENCODING);
			if ("gzip".equalsIgnoreCase(contentEncoding)) {
				if (null != reqMap.get(JsonKey.FILE)) {
					sendTelemetryToEkstep((byte[]) reqMap.get(JsonKey.FILE), req);
				}
			} else {
				try {
					Gson gson = new Gson();
					Map<String, String> headers = (Map<String, String>) req.get(JsonKey.HEADER);
					String response = HttpUtil.sendPostRequest(getTelemetryUrl(),
							gson.toJson(getEkstepTelemetryRequest(req)), headers);
					ProjectLogger.log(response + " processed.", LoggerEnum.INFO.name());
				} catch (Exception e) {
					ProjectLogger.log(e.getMessage(), e);
					ProjectLogger.log("Failure Data==" + req);
				}
			}
		}
	}

	private void sendTelemetryToEkstep(byte[] bs, Request request) {
		Map<String, String> headers = (Map<String, String>) request.get(JsonKey.HEADER);
		headers.put(JsonKey.CONTENT_ENCODING, "gzip");
		headers.remove("content-type");
		headers.remove("Content-Type");

		try {
			HttpUtilResponse response = HttpUtil.postInputStream(bs, headers, getTelemetryUrl());
			ProjectLogger.log(response.getStatusCode() + " " + response.getBody(), LoggerEnum.INFO.name());
		} catch (Exception e) {
			ProjectLogger.log(e.getMessage(), e);
			ProjectLogger.log("Failure Data==" + request);
		}
	}

	/**
	 * This method will return telemetry url.
	 * 
	 * @return
	 */
	private String getTelemetryUrl() {
		String telemetryBaseUrl = System.getenv(JsonKey.EKSTEP_BASE_URL);
		if (StringUtils.isBlank(telemetryBaseUrl)) {
			telemetryBaseUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_BASE_URL);
		}
		telemetryBaseUrl = telemetryBaseUrl
				+ PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_TELEMETRY_API_URL);
		ProjectLogger.log("telemetry url==" + telemetryBaseUrl);
		return telemetryBaseUrl;
	}

	private TelemetryV3Request getEkstepTelemetryRequest(Request request) {
		TelemetryV3Request telemetryV3Request = new TelemetryV3Request();
		if (request.getRequest().get(JsonKey.ETS) != null
				&& request.getRequest().get(JsonKey.ETS) instanceof BigInteger) {
			telemetryV3Request.setEts(((BigInteger) request.getRequest().get(JsonKey.ETS)).longValue());
		}
		if (request.getRequest().get(JsonKey.EVENTS) != null && request.getRequest().get(JsonKey.EVENTS) instanceof List
				&& !(((List) request.getRequest().get(JsonKey.EVENTS)).isEmpty())) {
			List<Map<String, Object>> events = (List<Map<String, Object>>) request.getRequest().get(JsonKey.EVENTS);
			telemetryV3Request.setEvents(events);
			ProjectLogger.log("Sending events to Ekstep - count: " + events.size(), LoggerEnum.INFO.name());
		}
		return telemetryV3Request;
	}
}
