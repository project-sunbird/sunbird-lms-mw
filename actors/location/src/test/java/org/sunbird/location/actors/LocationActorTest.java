package org.sunbird.location.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LocationActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, ElasticSearchUtil.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class LocationActorTest {

  private static ObjectMapper mapper = new ObjectMapper();
  private static final ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(LocationActor.class);
  private static final Map<String, Object> esRespone = new HashMap<>();
  private static Request actorMessage;
  private static Map<String, Object> data;

  @BeforeClass
  public static void init() {

    data = new HashMap();
    data.put(GeoLocationJsonKey.LOCATION_TYPE, "STATE");
    data.put(GeoLocationJsonKey.CODE, "S01");
    data.put(JsonKey.NAME, "DUMMY_STATE");
    data.put(JsonKey.ID, "id_01");
    esRespone.put(JsonKey.CONTENT, new ArrayList<>());
    esRespone.put(GeoLocationJsonKey.LOCATION_TYPE, "state");
    PowerMockito.mockStatic(ServiceFactory.class);
    CassandraOperationImpl cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.deleteRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSuccessResponse());
  }

  private static Response getSuccessResponse() {

    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  @Before
  public void setUp() {

    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.when(
            ElasticSearchUtil.complexSearch(
                Mockito.any(SearchDTO.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(esRespone);
    PowerMockito.when(
            ElasticSearchUtil.getDataByIdentifier(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(esRespone);
  }

  /*@Test
    public void testCreateLocation() {

  //    TestKit probe = new TestKit(system);
  //    ActorRef subject = system.actorOf(props);
      actorMessage = new Request();
      actorMessage.setOperation(LocationActorOperation.CREATE_LOCATION.getValue());
      actorMessage.getRequest().putAll(data);
      subject.tell(actorMessage, probe.getRef());
      Response res = probe.expectMsgClass(duration("10 second"), Response.class);
      Assert.assertTrue(null != res);

      boolean result = testScenario(LocationActorOperation.CREATE_LOCATION.getValue());
      assertTrue(result);
    }*/

  /*private boolean testScenario(String actorOperation) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    actorMessage = new Request();
    actorMessage.setOperation(actorOperation);
  }*/

  @Test
  public void testUpdateLocation() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    actorMessage = new Request();
    actorMessage.setOperation(LocationActorOperation.UPDATE_LOCATION.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testDeleteLocation() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    actorMessage = new Request();
    actorMessage.setOperation(LocationActorOperation.DELETE_LOCATION.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testSearchLocation() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    actorMessage = new Request();
    actorMessage.setOperation(LocationActorOperation.SEARCH_LOCATION.getValue());
    actorMessage.getRequest().put(JsonKey.FILTERS, data);
    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testCreateLocationWithInvalidValue() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    actorMessage = new Request();
    actorMessage.setOperation(LocationActorOperation.CREATE_LOCATION.getValue());
    data.put(GeoLocationJsonKey.LOCATION_TYPE, "anyType");
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(
        res.getCode().equals(ResponseCode.invalidValue.getErrorCode())
            || res.getResponseCode() == ResponseCode.invalidValue.getResponseCode());
  }

  @Test
  public void testCreateLocationWithoutMandatoryParams() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    actorMessage = new Request();
    actorMessage.setOperation(LocationActorOperation.CREATE_LOCATION.getValue());
    data.put(GeoLocationJsonKey.LOCATION_TYPE, "block");
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(
        res.getCode().equals(ResponseCode.mandatoryParamsMissing.getErrorCode())
            || res.getResponseCode() == ResponseCode.mandatoryParamsMissing.getResponseCode());
  }

  @Test
  public void testCreateLocationWithParentNotAllowed() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    actorMessage = new Request();
    actorMessage.setOperation(LocationActorOperation.CREATE_LOCATION.getValue());
    data.put(GeoLocationJsonKey.LOCATION_TYPE, "state");
    data.put(GeoLocationJsonKey.PARENT_CODE, "anyCode");
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(
        res.getCode().equals(ResponseCode.parentNotAllowed.getErrorCode())
            || res.getResponseCode() == ResponseCode.parentNotAllowed.getResponseCode());
  }

  @Test
  public void testDeleteLocationWithInvalidLocationDeleteRequest() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    actorMessage = new Request();
    actorMessage.setOperation(LocationActorOperation.DELETE_LOCATION.getValue());
    actorMessage.getRequest().putAll(data);

    List<Map<String, Object>> lst = new ArrayList<>();
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("any", "any");
    lst.add(innerMap);

    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.CONTENT, lst);

    esRespone.put(JsonKey.CONTENT, new ArrayList<>());
    PowerMockito.when(
            ElasticSearchUtil.complexSearch(
                Mockito.any(SearchDTO.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(map);
    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(
        res.getCode().equals(ResponseCode.invalidLocationDeleteRequest.getErrorCode())
            || res.getResponseCode()
                == ResponseCode.invalidLocationDeleteRequest.getResponseCode());
  }
}
