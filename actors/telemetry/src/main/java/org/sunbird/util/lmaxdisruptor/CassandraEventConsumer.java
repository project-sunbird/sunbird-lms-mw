package org.sunbird.util.lmaxdisruptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.lmax.disruptor.EventHandler;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

/**
 * 
 * @author Amit Kumar
 *
 */
public class CassandraEventConsumer implements EventHandler<TelemetryEvent> {
    private Util.DbInfo teleDbInfo = Util.dbInfoMap.get(BadgingJsonKey.TELEMETRY_DB);
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

    @SuppressWarnings("unchecked")
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
                saveTelemetryDataToCassandra((List<Map<String, Object>>) writeEvent.getData()
                        .getRequest().getRequest().get(JsonKey.EVENTS));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void saveTelemetryDataToCassandra(List<Map<String, Object>> list) {
        ObjectMapper mapper = new ObjectMapper();
        String eventdata = "";
        String reason = "";
        for (Map<String, Object> tele : list) {
            try {
                eventdata = mapper.writeValueAsString(tele);
                reason = validateEventData(tele);
                if (StringUtils.isNotBlank(reason)) {
                    ProjectLogger.log("Telemetry event " + reason + " for event mid : "
                            + tele.get(BadgingJsonKey.TELE_MID) + "and eid : "
                            + tele.get(BadgingJsonKey.TELE_EID), LoggerEnum.WARN.name());
                    continue;
                }

                Timestamp currentTimestamp =
                        new Timestamp(((Double) tele.get(BadgingJsonKey.TELE_ETS)).longValue());
                Map<String, Object> pdata =
                        (Map<String, Object>) tele.get(BadgingJsonKey.TELE_PDATA);
                TelemetryData teleEvent = new TelemetryData(OneWayHashing.encryptVal(eventdata),
                        (String) tele.get(BadgingJsonKey.TELE_MID),
                        (String) tele.get(JsonKey.CHANNEL), currentTimestamp, eventdata,
                        (String) pdata.get(JsonKey.ID), (String) pdata.get(JsonKey.VER),
                        (String) tele.get(BadgingJsonKey.TELE_EID));
                cassandraOperation.upsertRecord(teleDbInfo.getKeySpace(), teleDbInfo.getTableName(),
                        teleEvent.asMap());
            } catch (IOException e) {
                ProjectLogger.log(e.getMessage(), e);
            }

        }

    }

    private String validateEventData(Map<String, Object> tele) {
        if (null == tele.get(BadgingJsonKey.TELE_ETS)) {
            return "ets is null";
        }
        if (null == tele.get(BadgingJsonKey.TELE_PDATA)) {
            return "pdata is null";
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    private void extractEventData(List<String> teleList) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (String teleData : teleList) {
            Gson gson = new Gson();
            Map<String, Object> teleObj = gson.fromJson(teleData, HashMap.class);
            Map<String, Object> data = (Map<String, Object>) teleObj.get(JsonKey.DATA);
            List<Map<String, Object>> events = (List<Map<String, Object>>) data.get(JsonKey.EVENTS);
            list.addAll(events);
        }
        saveTelemetryDataToCassandra(list);

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

}
