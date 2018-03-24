package org.sunbird.util.lmaxdisruptor;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.TelemetryV3Request;

import com.google.gson.Gson;
import com.lmax.disruptor.EventHandler;


/**
 * This class will send telemetry data to Ekstep.
 * @author Manzarul
 *
 */
public class EkstepEventConsumer implements EventHandler<TelemetryEvent> {

	public void onEvent(TelemetryEvent writeEvent, long sequence, boolean endOfBatch) throws Exception {
		if (writeEvent != null && writeEvent.getData().getRequest() != null) {
			try {
				Gson gson = new Gson();
				String response = HttpUtil.sendPostRequest(getTelemetryUrl(),
						gson.toJson(getEkstepTelemetryRequest(writeEvent.getData().getRequest())), writeEvent.getData().getHeaders());
				ProjectLogger.log(response + " processed.", LoggerEnum.INFO.name());
			} catch (Exception e) {
				ProjectLogger.log(e.getMessage(), e);
				ProjectLogger.log("Failure Data==" + writeEvent.getData().getRequest());
			}
		}
	}

	/**
	 * This method will return telemetry url.
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

	private TelemetryV3Request getEkstepTelemetryRequest(Request request){

		TelemetryV3Request telemetryV3Request = new TelemetryV3Request();
		if(request.getRequest().get(JsonKey.ETS) != null && request.getRequest().get(JsonKey.ETS) instanceof BigInteger){
			telemetryV3Request.setEts(((BigInteger)request.getRequest().get(JsonKey.ETS)).longValue());
		}
		if(request.getRequest().get(JsonKey.EVENTS) != null && request.getRequest().get(JsonKey.EVENTS) instanceof List && !(((List) request.getRequest().get(JsonKey.EVENTS)).isEmpty())){
			telemetryV3Request.setEvents(
					(List<Map<String, Object>>) request.getRequest().get(JsonKey.EVENTS));
		}
		return telemetryV3Request;
	}

}
