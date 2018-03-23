package org.sunbird.util.lmaxdisruptor;

import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;

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
						gson.toJson(writeEvent.getData().getRequest()), writeEvent.getData().getHeaders());
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
		if (ProjectUtil.isStringNullOREmpty(telemetryBaseUrl)) {
			telemetryBaseUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_BASE_URL);
		}
		telemetryBaseUrl = telemetryBaseUrl
				+ PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_TELEMETRY_API_URL);
		ProjectLogger.log("telemetry url==" + telemetryBaseUrl);
		return telemetryBaseUrl;
	}

}
