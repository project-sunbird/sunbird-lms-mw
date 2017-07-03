package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.util.*;

import static akka.testkit.JavaTestKit.duration;

/**
 * @author  arvind
 */
public class LearnerStateActorTest {

    static ActorSystem system;
    final static Props props = Props.create(LearnerStateActor.class);
    static TestActorRef<LearnerStateActor> ref;
    private String USER_ID = "dummyUser";

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        ref = TestActorRef.create(system, props, "testActor");
    }

    @Test
    public void onReceiveTestForGetCourse() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request request = new Request();
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.USER_ID, "123");
        request.setRequest(map);
        request.setOperation(ActorOperations.GET_COURSE.getValue());
        subject.tell(request, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test
    public void onReceiveTestForGetCourseWithInvalidOperation() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request request = new Request();
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.USER_ID, "123");
        request.setRequest(map);
        request.setOperation("INVALID_OPERATION");
        subject.tell(request, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }

    @Test
    public void onReceiveTestForGetContent() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        HashMap<String, Object> innerMap = new HashMap<>();
        Request request = new Request();
        innerMap.put(JsonKey.USER_ID, USER_ID);
        List<String> contentList = Arrays.asList("10","11");
        innerMap.put(JsonKey.CONTENT_IDS, contentList);
        request.setRequest(innerMap);
        request.setOperation(ActorOperations.GET_CONTENT.getValue());
        subject.tell(request, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test
    public void onReceiveTestForGetContentByCourse() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        HashMap<String, Object> innerMap = new HashMap<>();
        Request request = new Request();
        innerMap.put(JsonKey.USER_ID, USER_ID);
        innerMap.put(JsonKey.COURSE_ID , "112");
        List<String> contentList = Arrays.asList("10","11");
        innerMap.put(JsonKey.CONTENT_IDS, contentList);
        innerMap.put(JsonKey.COURSE, innerMap);
        request.setRequest(innerMap);
        request.setOperation(ActorOperations.GET_CONTENT.getValue());
        subject.tell(request, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test
    public void onReceiveTestForGetContentByCourseIds() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        HashMap<String, Object> innerMap = new HashMap<>();
        Request request = new Request();
        innerMap.put(JsonKey.USER_ID, USER_ID);
        List<String> courseList = Arrays.asList("10","11");
        innerMap.put(JsonKey.COURSE_IDS, courseList);
        request.setRequest(innerMap);
        request.setOperation(ActorOperations.GET_CONTENT.getValue());
        subject.tell(request, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

}
