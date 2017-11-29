package org.sunbird.metrics.actors;

import static akka.testkit.JavaTestKit.duration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.util.Util.DbInfo;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;

public class OrganisationMetricsActorTest {
  
    private static ActorSystem system;
    private final static Props prop = Props.create(OrganisationMetricsActor.class);
    private static TestActorRef<OrganisationMetricsActor> ref;
    private static CassandraOperation operation = ServiceFactory.getInstance();
    private static String userId = "456-123";

    @BeforeClass
    public static void setUp() {
      system = ActorSystem.create("system");
      ref = TestActorRef.create(system, prop, "testActor");
      Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
      insertUserDataToES();
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Test
    public void testOrgCreation() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(prop);

      Request actorMessage = new Request();
      actorMessage.put(JsonKey.ORG_ID, "ORG_001");
      actorMessage.put(JsonKey.PERIOD, "7d");
      actorMessage.setOperation(ActorOperations.ORG_CREATION_METRICS.getValue());

      subject.tell(actorMessage, probe.getRef());
      Response res = probe.expectMsgClass(duration("50 second"), Response.class);
      Map<String, Object> data = res.getResult();
      Assert.assertEquals("7d", data.get(JsonKey.PERIOD));
      Assert.assertEquals("ORG_001", ((Map<String, Object>) data.get("org")).get(JsonKey.ORG_ID));
      Map<String, Object> series = (Map<String, Object>) data.get(JsonKey.SERIES);
      Assert.assertTrue(series.containsKey("org.creation.content[@status=draft].count"));
      Assert.assertTrue(series.containsKey("org.creation.content[@status=review].count"));
      Assert.assertTrue(series.containsKey("org.creation.content[@status=published].count"));
      List<Map<String, Object>> buckets = (List<Map<String, Object>>) ((Map<String, Object>) series
          .get("org.creation.content[@status=draft].count")).get("buckets");
      Assert.assertEquals(7, buckets.size());
      Map<String, Object> snapshot = (Map<String, Object>) data.get(JsonKey.SNAPSHOT);
      Assert.assertTrue(snapshot.containsKey("org.creation.content.count"));
      Assert.assertTrue(snapshot.containsKey("org.creation.authors.count"));
      Assert.assertTrue(snapshot.containsKey("org.creation.reviewers.count"));
      Assert.assertTrue(snapshot.containsKey("org.creation.content[@status=draft].count"));
      Assert.assertTrue(snapshot.containsKey("org.creation.content[@status=review].count"));
      Assert.assertTrue(snapshot.containsKey("org.creation.content[@status=published].count"));
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    @Test
    public void testOrgConsumption() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(prop);

      Request actorMessage = new Request();
      actorMessage.put(JsonKey.ORG_ID, "ORG_001");
      actorMessage.put(JsonKey.PERIOD, "7d");
      actorMessage.setOperation(ActorOperations.ORG_CONSUMPTION_METRICS.getValue());

      subject.tell(actorMessage, probe.getRef());
      Response res = probe.expectMsgClass(duration("10 second"), Response.class);
      Map<String, Object> data = res.getResult();
      Assert.assertEquals("7d", data.get(JsonKey.PERIOD));
      Assert.assertEquals("ORG_001", ((Map<String, Object>) data.get("org")).get(JsonKey.ORG_ID));
      Map<String, Object> series = (Map<String, Object>) data.get(JsonKey.SERIES);
      Assert.assertTrue(series.containsKey("org.consumption.content.users.count"));
      Assert.assertTrue(series.containsKey("org.consumption.content.time_spent.sum"));
      List<Map<String, Object>> buckets = (List<Map<String, Object>>) ((Map<String, Object>) series
          .get("org.consumption.content.users.count")).get("buckets");
      Assert.assertEquals(7, buckets.size());
      Map<String, Object> snapshot = (Map<String, Object>) data.get(JsonKey.SNAPSHOT);
      Assert.assertTrue(snapshot.containsKey("org.consumption.content.session.count"));
      Assert.assertTrue(snapshot.containsKey("org.consumption.content.time_spent.sum"));
      Assert.assertTrue(snapshot.containsKey("org.consumption.content.time_spent.average"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testOrgCreationInvalidOrgId() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(prop);

      Request actorMessage = new Request();
      actorMessage.put(JsonKey.ORG_ID, "ORG_001_INVALID");
      actorMessage.put(JsonKey.PERIOD, "7d");
      actorMessage.setOperation(ActorOperations.ORG_CREATION_METRICS.getValue());

      subject.tell(actorMessage, probe.getRef());
      ProjectCommonException e =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      Assert.assertEquals(ResponseCode.invalidOrgData.getErrorCode(), e.getCode());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testOrgConsumptionInvalidOrgId() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(prop);

      Request actorMessage = new Request();
      actorMessage.put(JsonKey.ORG_ID, "ORG_001_INVALID");
      actorMessage.put(JsonKey.PERIOD, "7d");
      actorMessage.setOperation(ActorOperations.ORG_CONSUMPTION_METRICS.getValue());

      subject.tell(actorMessage, probe.getRef());
      ProjectCommonException e =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      Assert.assertEquals(ResponseCode.invalidOrgData.getErrorCode(), e.getCode());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testOrgCreationInvalidPeriod() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(prop);

      Request actorMessage = new Request();
      actorMessage.put(JsonKey.ORG_ID, "ORG_001");
      actorMessage.put(JsonKey.PERIOD, "10d");
      actorMessage.setOperation(ActorOperations.ORG_CREATION_METRICS.getValue());

      subject.tell(actorMessage, probe.getRef());
      ProjectCommonException e =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      Assert.assertEquals("INVALID_ORGANIZATION_DATA", e.getCode());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testOrgConsumptionInvalidPeriod() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(prop);

      Request actorMessage = new Request();
      actorMessage.put(JsonKey.ORG_ID, "ORG_001");
      actorMessage.put(JsonKey.PERIOD, "10d");
      actorMessage.setOperation(ActorOperations.ORG_CONSUMPTION_METRICS.getValue());

      subject.tell(actorMessage, probe.getRef());
      ProjectCommonException e =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      Assert.assertEquals("INVALID_ORGANIZATION_DATA", e.getCode());
    }

    private static void insertUserDataToES() {
      Map<String, Object> userMap = new HashMap<>();
      userMap.put(JsonKey.USER_ID, userId);
      userMap.put(JsonKey.FIRST_NAME, "alpha");
      userMap.put(JsonKey.ID, userId);
      userMap.put(JsonKey.ROOT_ORG_ID, "ORG_001");
      userMap.put(JsonKey.USERNAME, "alpha-beta");
      userMap.put(JsonKey.EMAIL, "alpha123invalid@gmail.com");
      ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), userId,
          userMap);
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Test
    public void testOrgCreationReportData() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(prop);

      Request actorMessage = new Request();
      actorMessage.put(JsonKey.ORG_ID, "ORG_001");
      actorMessage.put(JsonKey.PERIOD, "7d");
      actorMessage.put(JsonKey.REQUESTED_BY, userId);
      actorMessage.put(JsonKey.FORMAT, "csv");
      actorMessage.setOperation(ActorOperations.ORG_CREATION_METRICS_REPORT.getValue());

      subject.tell(actorMessage, probe.getRef());
      Response res = probe.expectMsgClass(duration("100 second"), Response.class);
      Map<String, Object> data = res.getResult();
      String id = (String) data.get(JsonKey.REQUEST_ID);
      DbInfo dbinfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);
      try {
        Thread.sleep(2200);
      } catch (InterruptedException e) {
        ProjectLogger.log("Error", e);
      }
      Response resp = operation.getRecordById(dbinfo.getKeySpace(), dbinfo.getTableName(), id);
      List<Map<String, Object>> dataMap = (List<Map<String, Object>>) resp.get(JsonKey.RESPONSE);
      Integer status = (Integer) dataMap.get(0).get(JsonKey.STATUS);
      Assert.assertTrue(status > 0);
      Assert.assertEquals("Creation Report", dataMap.get(0).get(JsonKey.TYPE));
      operation.deleteRecord(dbinfo.getKeySpace(), dbinfo.getTableName(), id);
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Test
    public void testOrgConsumptionReportData() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(prop);

      Request actorMessage = new Request();
      actorMessage.put(JsonKey.ORG_ID, "ORG_001");
      actorMessage.put(JsonKey.PERIOD, "7d");
      actorMessage.put(JsonKey.REQUESTED_BY, userId);
      actorMessage.put(JsonKey.FORMAT, "csv");
      actorMessage.setOperation(ActorOperations.ORG_CONSUMPTION_METRICS_REPORT.getValue());

      subject.tell(actorMessage, probe.getRef());
      Response res = probe.expectMsgClass(duration("50 second"), Response.class);
      Map<String, Object> data = res.getResult();
      String id = (String) data.get(JsonKey.REQUEST_ID);
      DbInfo dbinfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        ProjectLogger.log("Error", e);
      }
      Response resp = operation.getRecordById(dbinfo.getKeySpace(), dbinfo.getTableName(), id);
      List<Map<String, Object>> dataMap = (List<Map<String, Object>>) resp.get(JsonKey.RESPONSE);
      Integer status = (Integer) dataMap.get(0).get(JsonKey.STATUS);
      Assert.assertTrue(status > 0);
      Assert.assertEquals("Consumption Report", dataMap.get(0).get(JsonKey.TYPE));
      operation.deleteRecord(dbinfo.getKeySpace(), dbinfo.getTableName(), id);
    }

    @AfterClass
    public static void destroy() {
      ElasticSearchUtil.removeData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), userId);
    }


}
