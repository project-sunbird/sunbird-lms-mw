package org.sunbird.util.lmaxdisruptor;


import com.google.gson.Gson;
import com.lmax.disruptor.EventHandler;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;

/**
 * This class will write data inside ES.
 * 
 * @author Manzarul
 *
 */
public class ESEventConsumer implements EventHandler<TelemetryEvent> {
    private static final String INDEX_NAME = "telemetry.raw";

    @Override
    public void onEvent(TelemetryEvent writeEvent, long sequence, boolean endOfBatch)
            throws Exception {
        if (writeEvent != null && writeEvent.getData().getRequest() != null) {
            Request req = writeEvent.getData().getRequest();
            Map<String, Object> reqMap = req.getRequest();
            String contentEncoding = (String) reqMap.get(JsonKey.CONTENT_ENCODING);
            List<String> teleList = null;
            if ("gzip".equalsIgnoreCase(contentEncoding)) {
                if (null != reqMap.get(JsonKey.FILE)) {
                    teleList = parseFile((byte[]) reqMap.get(JsonKey.FILE));
                    extractEventData(teleList);
                }
            } else {
                saveTelemetryDataToES((List<Map<String, Object>>) writeEvent.getData().getRequest()
                        .getRequest().get(JsonKey.EVENTS));
            }
        }
    }

    private void extractEventData(List<String> teleList) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (String teleData : teleList) {
            Gson gson = new Gson();
            Map<String, Object> teleObj = gson.fromJson(teleData, HashMap.class);
            Map<String, Object> data = (Map<String, Object>) teleObj.get(JsonKey.DATA);
            List<Map<String, Object>> events = (List<Map<String, Object>>) data.get(JsonKey.EVENTS);
            list.addAll(events);
            saveTelemetryDataToES(events);
        }

    }

    private void saveTelemetryDataToES(List<Map<String, Object>> events) {
        ElasticSearchUtil.bulkInsertData(createIndex(), ProjectUtil.EsType.telemetry.getTypeName(),
                events);
    }

    private List<String> parseFile(byte[] bs) {
        List<String> teleList = new ArrayList<>();
        InputStream ungzippedResponse = null;
        try (InputStream is = new ByteArrayInputStream(bs)) {
            ungzippedResponse = new GZIPInputStream(is);
            InputStreamReader reader = new InputStreamReader(ungzippedResponse);
            BufferedReader in = new BufferedReader(reader);
            String line = "";
            while ((line = in.readLine()) != null) {
                teleList.add(line);
            }
        } catch (Exception e) {
            ProjectLogger.log("Exception Occurred while reading file", e);
        }
        return teleList;
    }

    /**
     * This method will create index for Elastic search as follow "telemetry.raw.yyyy.mm"
     * 
     * @return
     */
    private static String createIndex() {
        Calendar cal = Calendar.getInstance();
        return new StringBuffer().append(INDEX_NAME).append("." + cal.get(Calendar.YEAR))
                .append("." + ((cal.get(Calendar.MONTH) + 1) > 9 ? (cal.get(Calendar.MONTH) + 1)
                        : "0" + (cal.get(Calendar.MONTH) + 1)))
                .toString();
    }
}
