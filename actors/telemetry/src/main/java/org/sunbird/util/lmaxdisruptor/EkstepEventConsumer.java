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
public class EkstepEventConsumer implements EventHandler<TelemetryEvent> {

    @Override
    public void onEvent(TelemetryEvent writeEvent, long sequence, boolean endOfBatch)
            throws Exception {
        if (writeEvent != null && writeEvent.getData().getRequest() != null) {
            Request req = writeEvent.getData().getRequest();
            Map<String, Object> reqMap = req.getRequest();
            String contentEncoding = (String) reqMap.get("Content-Encoding");
            if ("gzip".equalsIgnoreCase(contentEncoding)) {
                if (null != reqMap.get(JsonKey.FILE)) {
                    sendTelemetryToEkstep((byte[]) reqMap.get(JsonKey.FILE), writeEvent);
                }
            } else {
                try {
                    Gson gson = new Gson();
                    String response = HttpUtil.sendPostRequest(getTelemetryUrl(),
                            gson.toJson(
                                    getEkstepTelemetryRequest(writeEvent.getData().getRequest())),
                            writeEvent.getData().getHeaders());
                    ProjectLogger.log(response + " processed.", LoggerEnum.INFO.name());
                } catch (Exception e) {
                    ProjectLogger.log(e.getMessage(), e);
                    ProjectLogger.log("Failure Data==" + writeEvent.getData().getRequest());
                }
            }
        }
    }

    private void sendTelemetryToEkstep(byte[] bs, TelemetryEvent writeEvent) {
        writeEvent.getData().getHeaders().put("Content-Encoding", "gzip");
        writeEvent.getData().getHeaders().remove("content-type");
        writeEvent.getData().getHeaders().remove("Content-Type");
        writeEvent.getData().getHeaders().put("authorization",
                "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiIxZjMwNWQ1NDg1YjQzNDFiZGEyZmViNmI5ZTU0NjBmYSJ9.0D7D0mPX6o-F9sDmydurspSzH_RpzS1yzXxlTcVIVTo");
        try {
            HttpUtilResponse response =
                    HttpUtil.postInputStream(bs, writeEvent.getData().getHeaders(),
                            "https://dev.ekstep.in/api/data/v3/telemetry");
            ProjectLogger.log(response + " processed.", LoggerEnum.INFO.name());
        } catch (Exception e) {
            ProjectLogger.log(e.getMessage(), e);
            ProjectLogger.log("Failure Data==" + writeEvent.getData().getRequest());
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
            telemetryV3Request
                    .setEts(((BigInteger) request.getRequest().get(JsonKey.ETS)).longValue());
        }
        if (request.getRequest().get(JsonKey.EVENTS) != null
                && request.getRequest().get(JsonKey.EVENTS) instanceof List
                && !(((List) request.getRequest().get(JsonKey.EVENTS)).isEmpty())) {
            telemetryV3Request.setEvents(
                    (List<Map<String, Object>>) request.getRequest().get(JsonKey.EVENTS));
        }
        return telemetryV3Request;
    }

}
