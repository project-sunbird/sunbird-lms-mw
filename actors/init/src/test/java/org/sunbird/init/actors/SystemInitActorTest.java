package org.sunbird.init.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.init.service.SystemSettingService;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;

// @Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SystemInitActorTest {

  private static ActorSystem system;
  private static CassandraOperation operation = ServiceFactory.getInstance();
  private static final Props props = Props.create(SystemInitActor.class);
  private static Util.DbInfo orgDB = null;
  private String orgId = "";
  private static BadgrServiceImpl mockBadgingService;
  private static SystemSettingService systemSettingService;

  @BeforeClass
  public static void setUp() {
    SunbirdMWService.init();
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    orgDB = Util.dbInfoMap.get(JsonKey.ORG_DB);
    new DataCacheHandler().run();
  }

  @Test
  public void testSystemInitRootOrg() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    String testStr = "test";
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.SYSTEM_INIT_ROOT_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "testactorOrg");
    orgMap.put(JsonKey.CHANNEL, "1234fmdmdmde");
    innerMap.put(JsonKey.ORGANISATION, orgMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    orgId = (String) res.getResult().get(JsonKey.ORGANISATION_ID);
    System.out.println("orgId : " + orgId);
    Assert.assertTrue(null != orgId);
  }
}
