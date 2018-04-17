package org.sunbird.util.lmaxdisruptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.lmax.disruptor.EventHandler;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.services.imp.CassandraTelemetryDaoImpl;
import org.sunbird.services.service.TelemetryDao;

/** @author Amit Kumar */
public class CassandraEventConsumer implements EventHandler<Request> {

  private TelemetryDao telemetryDao = new CassandraTelemetryDaoImpl();

  @SuppressWarnings("unchecked")
  @Override
  public void onEvent(Request req, long sequence, boolean endOfBatch) throws Exception {
    if (req != null) {
      Map<String, Object> reqMap = req.getRequest();
      Map<String, String> headers = (Map<String, String>) reqMap.get(JsonKey.HEADER);
      String encoding = headers.get(JsonKey.CONTENT_ENCODING);
      List<String> teleList = null;
      if ("gzip".equalsIgnoreCase(encoding)) {
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
    for (Map<String, Object> tele : list) {
      try {
        if (null == tele.get(BadgingJsonKey.TELE_ETS)) {
          ProjectLogger.log(
              "ets for event mid : "
                  + tele.get(BadgingJsonKey.TELE_MID)
                  + "and eid : "
                  + tele.get(BadgingJsonKey.TELE_EID),
              LoggerEnum.WARN.name());
        }
        ProjectLogger.log("TELE_EVENT:: " + tele);
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
    Map<String, Object> pdata = null;
    try {
      Timestamp currentTimestamp = null;
      if (null != tele.get(BadgingJsonKey.TELE_ETS)) {
        currentTimestamp = getTimestamp(tele);
      }
      String pdataId = "";
      String pdataVer = "";
      String channel = "";
      if (tele.get(BadgingJsonKey.TELE_VERSION).equals("3.0")) {
        Map<String, Object> context = (Map<String, Object>) tele.get(BadgingJsonKey.TELE_CONTEXT);
        if (null != context) {
          channel = (String) context.get(JsonKey.CHANNEL);
          pdata = (Map<String, Object>) context.get(BadgingJsonKey.TELE_PDATA);
          if (null != pdata) {
            pdataId = (String) pdata.get(JsonKey.ID);
            pdataVer = (String) pdata.get(JsonKey.VER);
          }
        }
      } else {
        pdata = (Map<String, Object>) tele.get(BadgingJsonKey.TELE_PDATA);
        if (null != pdata) {
          pdataId = (String) pdata.get(JsonKey.ID);
          pdataVer = (String) pdata.get(JsonKey.VER);
        }
        channel = (String) tele.get(JsonKey.CHANNEL);
      }

      /** TelemetryData id is mid */
      eventdata = mapper.writeValueAsString(tele);
      return new TelemetryData(
          (String) tele.get(BadgingJsonKey.TELE_MID),
          channel,
          currentTimestamp,
          eventdata,
          pdataId,
          pdataVer,
          (String) tele.get(BadgingJsonKey.TELE_EID),
          (String) tele.get(BadgingJsonKey.TELE_VERSION));
    } catch (Exception e) {
      ProjectLogger.log("Exception occurred while creating TelemetryData Obj.", e);
    }
    return null;
  }

  private Timestamp getTimestamp(Map<String, Object> tele) {
    Timestamp currentTimestamp;
    // when request coming from controller ets type is BigInteger and when reading from
    // file its double
    if (tele.get(BadgingJsonKey.TELE_ETS) instanceof BigInteger) {
      currentTimestamp =
          new Timestamp(((BigInteger) tele.get(BadgingJsonKey.TELE_ETS)).longValue());
    } else {
      currentTimestamp = new Timestamp(((Double) tele.get(BadgingJsonKey.TELE_ETS)).longValue());
    }
    return currentTimestamp;
  }

  @SuppressWarnings("unchecked")
  private void extractEventData(List<String> teleList) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (String teleData : teleList) {
      try {
        Gson gson = new Gson();
        Map<String, Object> teleObj = gson.fromJson(teleData, HashMap.class);
        Map<String, Object> data = (Map<String, Object>) teleObj.get(JsonKey.DATA);
        List<Map<String, Object>> events = (List<Map<String, Object>>) data.get(JsonKey.EVENTS);
        list.addAll(events);
      } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e);
      }
    }
    saveTelemetryData(list);
  }

  private List<String> parseFile(byte[] bs) {
    List<String> teleList = new ArrayList<>();
    try (InputStream is = new ByteArrayInputStream(bs)) {
      InputStreamReader reader = new InputStreamReader(is);
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
