package org.sunbird.learner.actors.bulkupload;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.cacheloader.PageCacheLoaderService;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BulkUploadActorOperation;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  Util.class,
  RequestRouter.class,
  ElasticSearchUtil.class,
  PageCacheLoaderService.class,
})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class LocationBulkUploadActorTest {

  private static ActorSystem system = ActorSystem.create("system");;
  private static final Props props = Props.create(LocationBulkUploadActor.class);
  private static final String USER_ID = "user123";
  private static final String LOCATION_TYPE = "State";
  private static CassandraOperationImpl cassandraOperation;

  @Before
  public void beforeTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordById());
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getInsertResponse());
  }

  private Response getInsertResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private static Response cassandraGetRecordById() {
    Response response = new Response();
    List list = new ArrayList();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.NAME, "anyName");
    map.put(JsonKey.ID, "anyId");
    map.put(JsonKey.SECTIONS, "anySection");
    list.add(map);
    response.put(JsonKey.RESPONSE, list);
    return response;
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
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
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
