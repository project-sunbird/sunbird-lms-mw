package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.search.CourseSearchActor;
import org.sunbird.learner.util.Util;

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

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
    }

    //@Test
    public void getCourseByIdOnReceiveTest() {
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_COURSE_BY_ID.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.ID, "NTP course id_71");
        innerMap.put(JsonKey.REQUESTED_BY, "user-001");
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        Response res = probe.expectMsgClass(Response.class);

    }
    @Test
    public void searchCourseOnReceiveTest() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.SEARCH_COURSE.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.QUERY, "NTP course");
        Map<String, Object> map = new HashMap<>();
        List<String> fields = new ArrayList<String>();
        fields.add("noOfLecture");
        innerMap.put(JsonKey.FIELDS , fields);
        map.put(JsonKey.SEARCH, innerMap);
        reqObj.setRequest(map);
        subject.tell(reqObj, probe.getRef());
        Response res = probe.expectMsgClass(Response.class);
    }
}
