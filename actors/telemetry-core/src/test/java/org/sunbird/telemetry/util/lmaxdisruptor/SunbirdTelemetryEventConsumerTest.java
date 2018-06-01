package org.sunbird.telemetry.util.lmaxdisruptor;

import com.google.common.net.HttpHeaders;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.junit.Assert;
import org.junit.Test;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.TelemetryV3Request;
import org.sunbird.util.lmaxdisruptor.SunbirdTelemetryEventConsumer;

public class SunbirdTelemetryEventConsumerTest {
  private SunbirdTelemetryEventConsumer writeEventConsumer = new SunbirdTelemetryEventConsumer();

  @Test
  public void testHeader() {
    Map<String, String> map = writeEventConsumer.getHeaders();
    Assert.assertNotNull(map);
    Assert.assertEquals(map.get(HttpHeaders.CONTENT_TYPE), MediaType.APPLICATION_JSON);
  }

  @Test
  public void testTelemetryUrl() {
    String url = writeEventConsumer.getTelemetryUrl();
    Assert.assertNotNull(url);
    boolean response = ProjectUtil.isUrlvalid(url);
    Assert.assertTrue(response);
  }

  @Test
  public void testRequestDataStructure() {
    Request request = new Request();
    Map<String, Object> map = writeEventConsumer.getTelemetryRequest(request);
    Assert.assertNotNull(map);
    Assert.assertNotNull(map.get(JsonKey.REQUEST));
    Assert.assertNotNull(((TelemetryV3Request) map.get(JsonKey.REQUEST)).getEvents());
  }
}
