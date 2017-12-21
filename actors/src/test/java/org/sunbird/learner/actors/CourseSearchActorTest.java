package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.cql3.Json;
import org.json.JSONArray;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.Application;
import org.sunbird.learner.actors.search.CourseSearchActor;
import org.sunbird.learner.util.Util;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Manzarul
 *
 */
public class CourseSearchActorTest {
    static ActorSystem system;
    final static Props props = Props.create(CourseSearchActor.class);
    String courseId = "";

    @BeforeClass
    public static void setUp() {
      Application.startLocalActorSystem();
        system = ActorSystem.create("system");
        
        Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    }

    @Test
    public void searchCourseOnReceiveTest() {
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
       
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.SEARCH_COURSE.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.QUERY, "");
        Map<String, Object> filters = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        List<String> objectType = new ArrayList<String>();
        objectType.add("Content");
        filters.put(JsonKey.OBJECT_TYPE , objectType);
        List<String> mimeType = new ArrayList<String>();
        mimeType.add("application/vnd.ekstep.html-archive");
        filters.put("mimeType" , mimeType);
        List<String> status = new ArrayList<String>();
        status.add("Draft");
        status.add("Live");
        filters.put(JsonKey.STATUS , status);
        innerMap.put(JsonKey.FILTERS, filters);
        innerMap.put(JsonKey.LIMIT,1);
        map.put(JsonKey.SEARCH, innerMap);
        reqObj.setRequest(map);
        subject.tell(reqObj, probe.getRef());
        Response res = probe.expectMsgClass(Response.class);
        Object[] objects = ((Object[]) res.getResult().get(JsonKey.RESPONSE));
        
        if(null != objects && objects.length>0){
          Map<String,Object> obj = (Map<String, Object>) objects[0];
          courseId = (String) obj.get(JsonKey.IDENTIFIER);
          System.out.println(courseId);
        }
        getCourseByIdOnReceiveTest();
      }
       
    public void getCourseByIdOnReceiveTest() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.GET_COURSE_BY_ID.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.ID, courseId);
      innerMap.put(JsonKey.REQUESTED_BY, "user-001");
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
    public void testAAAInvalidRequest(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Response resObj = new Response();

        subject.tell(resObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }
}
