package org.sunbird.learner.actors.client;

import static akka.testkit.JavaTestKit.duration;

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
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.NotesManagementActor;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClientManagementActorTest {
  
  static ActorSystem system;
  final static Props props = Props.create(ClientManagementActor.class);
  static TestActorRef<NotesManagementActor> ref;
  private static String masterKey = "";
  private static String clientId = "";
 
  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    ref = TestActorRef.create(system, props, "testActor");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test1RegisterClientSuccess(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.CLIENT_NAME , "Test");
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.REGISTER_CLIENT.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"),Response.class);
    clientId = (String)res.getResult().get(JsonKey.CLIENT_ID);
    masterKey = (String)res.getResult().get(JsonKey.MASTER_KEY);
    Assert.assertTrue(!ProjectUtil.isStringNullOREmpty(clientId));
    Assert.assertTrue(!ProjectUtil.isStringNullOREmpty(masterKey));
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test2RegisterClientException(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.CLIENT_NAME , "Test");
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.REGISTER_CLIENT.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res = probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);
    if(null != res){
      Assert.assertEquals(ResponseMessage.Message.INVALID_CLIENT_NAME, res.getMessage());
    }
  }
  
  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void test3GetClientKeySuccess(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.CLIENT_ID , clientId);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.GET_CLIENT_KEY.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("20 second"),Response.class);
    List<Map<String,Object>> dataList = (List<Map<String, Object>>) res.getResult().get(JsonKey.RESPONSE);
    Assert.assertEquals(clientId, dataList.get(0).get(JsonKey.ID));
    Assert.assertEquals(masterKey, dataList.get(0).get(JsonKey.MASTER_KEY));
    Assert.assertEquals("test", dataList.get(0).get(JsonKey.CLIENT_NAME));
    Assert.assertTrue(!ProjectUtil.isStringNullOREmpty((String)dataList.get(0).get(JsonKey.CREATED_DATE)));
    Assert.assertTrue(!ProjectUtil.isStringNullOREmpty((String)dataList.get(0).get(JsonKey.UPDATED_DATE)));
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test4GetClientKeyFailure(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.CLIENT_ID , "test123");
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.GET_CLIENT_KEY.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res = probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);
    if(null != res){
      Assert.assertEquals(ResponseMessage.Message.INVALID_REQUESTED_DATA, res.getMessage());
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test5UpdateClientKeySuccess(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.CLIENT_ID , clientId);
    request.put(JsonKey.MASTER_KEY, masterKey);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.UPDATE_CLIENT_KEY.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("20 second"),Response.class);
    Assert.assertEquals(clientId, res.getResult().get(JsonKey.CLIENT_ID));
    Assert.assertNotEquals(masterKey, res.getResult().get(JsonKey.MASTER_KEY));
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void test6UpdateClientKeyFailure(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.CLIENT_ID , "test");
    request.put(JsonKey.MASTER_KEY, masterKey);
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.UPDATE_CLIENT_KEY.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res = probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);
    if(null != res){
      Assert.assertEquals(ResponseMessage.Message.INVALID_REQUESTED_DATA, res.getMessage());
    }
  }
  @SuppressWarnings("deprecation")
  @Test
  public void test7UpdateClientKeyFailureInvalidKey(){
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    Map<String, Object> request = new HashMap<>();
    request.put(JsonKey.CLIENT_ID , clientId);
    request.put(JsonKey.MASTER_KEY, "test");
    actorMessage.setRequest(request);
    actorMessage.setOperation(ActorOperations.UPDATE_CLIENT_KEY.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res = probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);
    if(null != res){
      Assert.assertEquals(ResponseMessage.Message.INVALID_REQUESTED_DATA, res.getMessage());
    }
  }
  
  @AfterClass
  public static void destroy(){
    CassandraOperation operation = ServiceFactory.getInstance();
    Util.DbInfo clientInfoDB = Util.dbInfoMap.get(JsonKey.CLIENT_INFO_DB);
    //Delete client data from cassandra
    operation.deleteRecord(clientInfoDB.getKeySpace(), clientInfoDB.getTableName(), clientId);
  }

}