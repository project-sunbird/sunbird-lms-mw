package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
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
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.health.HealthActor;
import org.sunbird.learner.util.Util;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, Util.class, RequestRouter.class, ElasticSearchUtil.class})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class HealthActorTest {

  private static ActorSystem system = ActorSystem.create("system");;
  private static final Props props = Props.create(HealthActor.class);
  private static CassandraOperationImpl cassandraOperation;

  @Before
  public void beforeTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(ElasticSearchUtil.class);

    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetAllRecords());
    when(ElasticSearchUtil.healthCheck()).thenReturn(true);
  }

  private static Response cassandraGetAllRecords() {

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
  public void checkTelemetryKeyFailure() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    String telemetryEnvKey = "user";
    PowerMockito.mockStatic(Util.class);
    PowerMockito.doNothing()
        .when(
            Util.class,
            "initializeContext",
            Mockito.any(Request.class),
            Mockito.eq(telemetryEnvKey));

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.HEALTH_CHECK.getValue());
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("10 seconds"), Response.class);
    Assert.assertTrue(!(telemetryEnvKey.charAt(0) >= 65 && telemetryEnvKey.charAt(0) <= 90));
  }

  @Test
  public void getHealthCheck() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.HEALTH_CHECK.getValue());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void getACTORHealthCheck() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ACTOR.getValue());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void getESHealthCheck() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ES.getValue());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void getCASSANDRAHealthCheck() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CASSANDRA.getValue());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }
}
