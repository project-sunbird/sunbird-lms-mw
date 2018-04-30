package org.sunbird.learner.actors.bulkupload;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BulkUploadActorOperation;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

/**
 * Test case for Location Bulk upload.
 *
 * @author arvind on 30/4/18.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocationBulkUploadActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(LocationBulkUploadActor.class);
  private static final String USER_ID = "user123";
  private static final String LOCATION_TYPE = "State";
  private ObjectMapper mapper = new ObjectMapper();

  @BeforeClass
  public static void setUp() {
    SunbirdMWService.init();
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
  }

  @Test
  public void testLocationBulkUploadWithProperData() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    List<String> headerLine =
        Arrays.asList(
            GeoLocationJsonKey.PROPERTY_NAME,
            GeoLocationJsonKey.CODE,
            GeoLocationJsonKey.PARENT_CODE,
            GeoLocationJsonKey.PARENT_ID);
    List<String> firstDataLine = Arrays.asList("location_name", "location-code", null, null);
    String jsonString = createLines(headerLine, firstDataLine);
    Request reqObj = getRequestObjectForLocationBulkUpload(LOCATION_TYPE, jsonString.getBytes());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    String processId = (String) res.get(JsonKey.ID);
    Assert.assertTrue(null != processId);
  }

  @Test
  public void testLocationBulkUploadWithInvalidAttributeNames() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    List<String> headerLine =
        Arrays.asList(
            GeoLocationJsonKey.PROPERTY_NAME + "invalid",
            GeoLocationJsonKey.CODE,
            GeoLocationJsonKey.PARENT_CODE,
            GeoLocationJsonKey.PARENT_ID);
    List<String> firstDataLine = Arrays.asList("location_name", "location-code", null, null);
    String jsonString = createLines(headerLine, firstDataLine);
    Request reqObj = getRequestObjectForLocationBulkUpload(LOCATION_TYPE, jsonString.getBytes());
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testLocationBulkUploadWithoutMandatoryFieldCode() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    List<String> headerLine =
        Arrays.asList(
            GeoLocationJsonKey.PROPERTY_NAME,
            GeoLocationJsonKey.PARENT_CODE,
            GeoLocationJsonKey.PARENT_ID);
    List<String> firstDataLine = Arrays.asList("location_name", null, null);
    String jsonString = createLines(headerLine, firstDataLine);
    Request reqObj = getRequestObjectForLocationBulkUpload(LOCATION_TYPE, jsonString.getBytes());
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testLocationBulkUploadWithoutAnyDataRecord() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    List<String> headerLine =
        Arrays.asList(
            GeoLocationJsonKey.PROPERTY_NAME,
            GeoLocationJsonKey.PARENT_CODE,
            GeoLocationJsonKey.PARENT_ID);
    String jsonString = createLines(headerLine);
    Request reqObj = getRequestObjectForLocationBulkUpload(LOCATION_TYPE, jsonString.getBytes());
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testLocationBulkUploadWithExtraAttributeNameValue() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    List<String> headerLine =
        Arrays.asList(
            GeoLocationJsonKey.PROPERTY_NAME,
            GeoLocationJsonKey.CODE,
            GeoLocationJsonKey.PARENT_CODE,
            GeoLocationJsonKey.PARENT_ID,
            GeoLocationJsonKey.PROPERTY_VALUE);
    List<String> firstDataLine =
        Arrays.asList("location_name", "location-code", null, null, "value");
    String jsonString = createLines(headerLine, firstDataLine);
    Request reqObj = getRequestObjectForLocationBulkUpload(LOCATION_TYPE, jsonString.getBytes());
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  private Request getRequestObjectForLocationBulkUpload(String locationType, byte[] file) {
    Request reqObj = new Request();
    reqObj.setOperation(BulkUploadActorOperation.LOCATION_BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.LOCATION);
    innerMap.put(JsonKey.FILE, file);
    innerMap.put(GeoLocationJsonKey.LOCATION_TYPE, locationType);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);
    return reqObj;
  }

  private String createLines(List<String>... list) throws JsonProcessingException {

    StringBuilder builder = new StringBuilder();
    for (List<String> l : list) {
      String.join(",", l);
      builder.append(String.join(",", l));
      builder.append(System.lineSeparator());
    }
    return builder.toString();
  }
}
