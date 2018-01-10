package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.Application;
import org.sunbird.learner.actors.search.SearchHandlerActor;
import org.sunbird.learner.util.Util;

public class SearchHandlerActorTest {

  static ActorSystem system;
  final static Props props = Props.create(SearchHandlerActor.class);
  String courseId = "";

  @BeforeClass
  public static void setUp() {
      Application.startLocalActorSystem();
      system = ActorSystem.create("system");
      Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
  }

  @Test
  public void searchUser() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
     
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.COMPOSITE_SEARCH.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.QUERY, "");
      Map<String, Object> filters = new HashMap<>();
      List<String> objectType = new ArrayList<String>();
      objectType.add("user");
      filters.put(JsonKey.OBJECT_TYPE , objectType);
      filters.put(JsonKey.ROOT_ORG_ID, "ORG_001");
      innerMap.put(JsonKey.FILTERS, filters);
      innerMap.put(JsonKey.LIMIT,1);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("200 second"),Response.class);
    }
  
  @Test
  public void testInvalidOperation(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation("INVALID_OPERATION");

      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
  }
  
  @Test
  public void testInvalidRequestData(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Response reqObj = new Response();

      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
  }
  
}
