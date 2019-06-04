package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.CassandraConnectionManager;
import org.sunbird.helper.CassandraConnectionMngrFactory;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.datapersistence.DbOperationActor;
import org.sunbird.learner.util.Util;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  Util.class,
  RequestRouter.class,
  CassandraConnectionMngrFactory.class,
  ElasticSearchUtil.class
})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class DbOperationActorTest {

  private static ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(DbOperationActor.class);
  private static CassandraConnectionManager cassandraConnectionManager;
  private static CassandraOperationImpl cassandraOperation;

  @Before
  public void beforeTest() {

    cassandraConnectionManager = PowerMockito.mock(CassandraConnectionManager.class);
    PowerMockito.mockStatic(CassandraConnectionMngrFactory.class);
    Mockito.when(CassandraConnectionMngrFactory.getObject(Mockito.anyString()))
        .thenReturn(cassandraConnectionManager);
    Mockito.when(cassandraConnectionManager.getTableList(Mockito.anyString()))
        .thenReturn(getTableData());

    PowerMockito.mockStatic(ElasticSearchUtil.class);
    Mockito.when(
            ElasticSearchUtil.createData(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn("response");

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(cassandraInsertRecord());
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getCassandraResponse());
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCassandraRecordByIdResponse());
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getAllRecordsResponse());
    when(cassandraOperation.deleteRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getDeleteResponse());
  }

  private Response getDeleteResponse() {

    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private Response getAllRecordsResponse() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    list.add(map);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Response getCassandraRecordByIdResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private Response getCassandraResponse() {

    Response response = new Response();
    response.put("response", "SUCCESS");
    return response;
  }

  private Response cassandraInsertRecord() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private List<String> getTableData() {
    List<String> list = new ArrayList<>();
    list.add("announcement");
    return list;
  }

  @Test
  public void testInvalidOperation() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("INVALID_OPERATION");

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testA1Create() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    map.put("indexed", true);
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("id", "454ee9-17-a2-47-id");
    innerMap.put("sourceid", "45_sourceId");
    innerMap.put("userid", "230cb747-userId");
    innerMap.put("status", "active");
    map.put("payload", innerMap);
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testCreateFailureWithImproperTableOrDocName() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    //        map.put("entityName", "announcement");
    map.put("indexed", true);
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("id", "454ee9-17-a2-47-id");
    innerMap.put("sourceid", "45_sourceId");
    innerMap.put("userid", "230cb747-userId");
    innerMap.put("status1", "active");
    map.put("payload", innerMap);
    reqObj.setRequest(map);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(exc.getCode().equals(ResponseCode.tableOrDocNameError.getErrorCode()));
  }

  @Test
  public void testUpdateWithIndex() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    map.put("indexed", true);
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("id", "454ee9-17-a2-47-id");
    innerMap.put("sourceid", "45_sourceId");
    innerMap.put("userid", "230cb747-userId");
    innerMap.put("status", "inactive");
    map.put("payload", innerMap);
    reqObj.setRequest(map);

    Mockito.when(
            ElasticSearchUtil.updateData(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(true);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateWithoutIndex() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    map.put("indexed", false);
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("id", "454ee9-17-a2-47-id");
    innerMap.put("sourceid", "45_sourceId");
    innerMap.put("userid", "230cb747-userId");
    innerMap.put("status", "inactive");
    map.put("payload", innerMap);
    reqObj.setRequest(map);

    Mockito.when(
            ElasticSearchUtil.getDataByIdentifier(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getDataByIdentifierResponse());

    Mockito.when(
            ElasticSearchUtil.updateData(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(true);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  private Map<String, Object> getDataByIdentifierResponse() {
    Map<String, Object> map = new HashMap<>();
    return map;
  }

  @Test
  public void testUpdateFailed() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    map.put("indexed", true);
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("id", "454ee9-17-a2-47-id");
    innerMap.put("sourceid", "45_sourceId");
    innerMap.put("userid", "230cb747-userId");
    innerMap.put("status1", "inactive");
    map.put("payload", innerMap);
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(exc.getCode().equals(ResponseCode.updateFailed.getErrorCode()));
  }

  @Test
  public void testReadSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.READ_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    map.put("id", "454ee9-17-a2-47-id");
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testReadFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.READ_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement1");
    map.put("id", "454ee9-17-a2-47-id");
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(exc.getCode().equals(ResponseCode.tableOrDocNameError.getErrorCode()));
  }

  @Test
  public void testReadAllSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.READ_ALL_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testReadAllFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.READ_ALL_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement1");
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(exc.getCode().equals(ResponseCode.tableOrDocNameError.getErrorCode()));
  }

  @Test
  public void testSearchSuccessWithoutElasticSearchResult() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.SEARCH_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    List<String> list = new ArrayList<>();
    list.add("id");
    map.put("requiredFields", list);
    Map<String, Object> filter = new HashMap<>();
    map.put(JsonKey.FILTERS, filter);
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testSearchSuccessWithElasticSearchResult() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.SEARCH_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    List<String> list = new ArrayList<>();
    list.add(JsonKey.ID);
    list.add(JsonKey.CONTENT);
    map.put("requiredFields", list);
    Map<String, Object> filter = new HashMap<>();
    map.put(JsonKey.FILTERS, filter);
    reqObj.setRequest(map);

    when(ElasticSearchUtil.complexSearch(
            Mockito.any(SearchDTO.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getComplexSearchresponse());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testSearchFailureWithoutEntityName() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.SEARCH_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    List<String> list = new ArrayList<>();
    list.add(JsonKey.ID);
    map.put("requiredFields", list);
    Map<String, Object> filter = new HashMap<>();
    map.put(JsonKey.FILTERS, filter);
    reqObj.setRequest(map);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    assertTrue(exc.getCode().equals(ResponseCode.tableOrDocNameError.getErrorCode()));
  }

  private Map<String, Object> getComplexSearchresponse() {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ID, "123");

    List<Map<String, Object>> content = new ArrayList<>();
    Map<String, Object> m = new HashMap<>();
    m.put(JsonKey.IDENTIFIER, 1);
    content.add(m);
    map.put(JsonKey.CONTENT, content);
    return map;
  }

  @Test
  public void testA6delete() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.DELETE_DATA.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    map.put("indexed", true);
    map.put("id", "454ee9-17-a2-47-id");
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testA7getMetrics() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_METRICS.getValue());
    Map<String, Object> map = new HashMap<>();
    map.put("entityName", "announcement");
    Map<String, Object> query = new HashMap<>();
    map.put("rawQuery", query);
    reqObj.setRequest(map);

    when(ElasticSearchUtil.searchMetricsData(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSearchMetrixData());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  private Response getSearchMetrixData() {
    Response response = new Response();
    Map<String, Object> map = new HashMap<>();
    response.put(JsonKey.RESPONSE, map);
    return response;
  }
}
