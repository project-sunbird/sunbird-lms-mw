package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.OrgStatus;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;

/** @author loganathan. */
// @Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InitialisationActorTest {

  private static ActorSystem system;
  private static CassandraOperation operation = ServiceFactory.getInstance();
  private static final Props props = Props.create(InitialisationActor.class);
  private static Util.DbInfo orgDB = null;
  private String orgId = "";
  @BeforeClass
  public static void setUp() {
    SunbirdMWService.init();
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);   
    orgDB = Util.dbInfoMap.get(JsonKey.ORG_DB);
    new DataCacheHandler().run();
  }

  @Test
  public void testcreateFirstRootOrg() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    String testStr = "test";
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_FIRST_ROOTORG.getValue());
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
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }  
}
