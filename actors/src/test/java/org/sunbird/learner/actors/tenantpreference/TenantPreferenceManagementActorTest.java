package org.sunbird.learner.actors.tenantpreference;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.LearnerStateActor;
import org.sunbird.learner.util.Util;

/**
 * Created by arvind on 30/10/17.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TenantPreferenceManagementActorTest {


  static ActorSystem system;
  final static Props props = Props.create(TenantPreferenceManagementActor.class);
  static TestActorRef<LearnerStateActor> ref;
  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static Util.DbInfo tenantPreferenceDbInfo = Util.dbInfoMap.get(JsonKey.TENANT_PREFERENCE_DB);
  private static  Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private static Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);

  private static final String orgId = "hhjcjr79fw4p89";
  private static final String USER_ID = "vcurc633r8911";

  @BeforeClass
  public static void setUp(){

    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    system = ActorSystem.create("system");
    Map<String , Object> userMap = new HashMap<>();
    userMap.put(JsonKey.ID , USER_ID);
    //userMap.put(JsonKey.ROOT_ORG_ID, ROOT_ORG_ID);
    cassandraOperation.insertRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), userMap);
    userMap.put(JsonKey.USER_ID, USER_ID);
    ElasticSearchUtil
        .createData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), USER_ID, userMap);

    Map<String , Object> orgMap = new HashMap<String , Object>();
    orgMap.put(JsonKey.ID , orgId);
    cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgMap);

  }

  @Test
  public void testCreateTanentPreference(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();
    List<Map<String , Object>> reqList = new ArrayList<>();

    Map<String , Object> map = new HashMap<>();
    map.put(JsonKey.ROLE , "admin");
    reqList.add(map);

    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , reqList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , orgId);
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.CREATE_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"),Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testCreateTanentPreferenceDuplicate(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();
    List<Map<String , Object>> reqList = new ArrayList<>();

    Map<String , Object> map = new HashMap<>();
    map.put(JsonKey.ROLE , "admin");
    reqList.add(map);

    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , reqList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , orgId);
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.CREATE_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"),Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testCreateTanentPreferenceWithInvalidOrgId(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();
    List<Map<String , Object>> reqList = new ArrayList<>();

    Map<String , Object> map = new HashMap<>();
    map.put(JsonKey.ROLE , "admin");
    reqList.add(map);

    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , reqList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , "");
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.CREATE_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCreateTanentPreferenceWithInvalidOrgIdValue(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();
    List<Map<String , Object>> reqList = new ArrayList<>();

    Map<String , Object> map = new HashMap<>();
    map.put(JsonKey.ROLE , "admin");
    reqList.add(map);

    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , reqList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , "nc389f3ffi");
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.CREATE_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testCreateTanentPreferenceWithInvalidReqData(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();
    List<Map<String , Object>> reqList = new ArrayList<>();

    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , reqList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , orgId);
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.CREATE_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }


  @Test
  public void testUpdateTanentPreference(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();
    List<Map<String , Object>> reqList = new ArrayList<>();

    Map<String , Object> map = new HashMap<>();
    map.put(JsonKey.ROLE , "admin");
    reqList.add(map);

    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , reqList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , orgId);
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.UPDATE_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"),Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }


  @Test
  public void testUpdateTanentPreferenceWithInvalidRequestData(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();
    List<Map<String , Object>> reqList = new ArrayList<>();

    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , reqList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , orgId);
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.UPDATE_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateTanentPreferenceWithInvalidOrgId(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();
    List<Map<String , Object>> reqList = new ArrayList<>();

    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , reqList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , "");
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.UPDATE_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testWGetTanentPreference(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();

    actorMessage.getRequest().put(JsonKey.ORG_ID , orgId);
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.GET_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"),Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testWGetTanentPreferenceWithInvalidOrgId(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();

    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , "");
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.GET_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testWGetTanentPreferenceWithInvalidOrgIdValue(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();

    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , "org647bdg7");
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.GET_TENANT_PREFERENCE.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testUpdateUserTcStatus(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();

    Map<String , Object> map = new HashMap<>();
    map.put(JsonKey.TERM_AND_CONDITION_STATUS , "ACCEPTED");


    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , map);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , orgId);
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation(ActorOperations.UPDATE_TC_STATUS_OF_USER.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"),Response.class);
    Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testWithInvalidOperationType(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();

    Map<String , Object> map = new HashMap<>();
    map.put(JsonKey.TERM_AND_CONDITION_STATUS , "ACCEPTED");


    actorMessage.getRequest().put(JsonKey.TENANT_PREFERENCE , map);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , orgId);
    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.setOperation("InvalidOperation");

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testWithInvalidMessageType(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    subject.tell("Invalid message", probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @AfterClass
  public static void destroy(){

    cassandraOperation.deleteRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), USER_ID);
    ElasticSearchUtil.removeData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), USER_ID);

    cassandraOperation.deleteRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgId);

    Response response1 = cassandraOperation.getRecordsByProperty(tenantPreferenceDbInfo.getKeySpace(), tenantPreferenceDbInfo.getTableName(), JsonKey.ORG_ID, orgId);
    List<Map<String, Object>> list = (List<Map<String, Object>>) response1.get(JsonKey.RESPONSE);

    for(Map<String , Object> map : list){
      String id = (String)map.get(JsonKey.ID);
      cassandraOperation.deleteRecord(tenantPreferenceDbInfo.getKeySpace(), tenantPreferenceDbInfo.getTableName(), id);
    }

  }

}
