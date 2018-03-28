package org.sunbird.util.lmaxdisruptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.lmax.disruptor.EventHandler;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.services.imp.CassandraTelemetryDaoImpl;
import org.sunbird.services.service.TelemetryDao;

/**
 * 
 * @author Amit Kumar
 *
 */
public class CassandraEventConsumer implements EventHandler<Request> {

    private TelemetryDao telemetryDao = new CassandraTelemetryDaoImpl();

    @SuppressWarnings("unchecked")
    @Override
    public void onEvent(Request req, long sequence, boolean endOfBatch) throws Exception {
        if (req != null) {
            Map<String, Object> reqMap = req.getRequest();
            String contentEncoding = (String) reqMap.get(JsonKey.CONTENT_ENCODING);
            List<String> teleList = null;
            if ("gzip".equalsIgnoreCase(contentEncoding)) {
                if (null != reqMap.get(JsonKey.FILE)) {
                    teleList = parseFile((byte[]) reqMap.get(JsonKey.FILE));
                    extractEventData(teleList);
                }
            } else {
                saveTelemetryData((List<Map<String, Object>>) req.getRequest().get(JsonKey.EVENTS));
            }
        }
    }

    private void saveTelemetryData(List<Map<String, Object>> list) {
        String reason = "";
        for (Map<String, Object> tele : list) {
            try {
                reason = validateEventData(tele);
                if (StringUtils.isNotBlank(reason)) {
                    ProjectLogger.log("Telemetry event " + reason + " for event mid : "
                            + tele.get(BadgingJsonKey.TELE_MID) + "and eid : "
                            + tele.get(BadgingJsonKey.TELE_EID), LoggerEnum.WARN.name());
                    continue;
                }
                TelemetryData teleEvent = getTelemetryDataObj(tele);
                if (null != teleEvent) {
                    telemetryDao.save(teleEvent);
                }
            } catch (Exception e) {
                ProjectLogger.log(e.getMessage(), e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private TelemetryData getTelemetryDataObj(Map<String, Object> tele) {
        ObjectMapper mapper = new ObjectMapper();
        String eventdata = "";
        try {
            Timestamp currentTimestamp =
                    new Timestamp(((Double) tele.get(BadgingJsonKey.TELE_ETS)).longValue());
            Map<String, Object> pdata = (Map<String, Object>) tele.get(BadgingJsonKey.TELE_PDATA);
            /**
             * TelemetryData id is mid
             */
            eventdata = mapper.writeValueAsString(tele);
            return new TelemetryData((String) tele.get(BadgingJsonKey.TELE_MID),
                    (String) tele.get(JsonKey.CHANNEL), currentTimestamp, eventdata,
                    (String) pdata.get(JsonKey.ID), (String) pdata.get(JsonKey.VER),
                    (String) tele.get(BadgingJsonKey.TELE_EID));
        } catch (Exception e) {
            ProjectLogger.log("Exception occurred while creating TelemetryData Obj.", e);
        }
        return null;
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
        saveTelemetryData(list);

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
