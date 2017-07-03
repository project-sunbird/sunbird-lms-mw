package org.sunbird.learner.actors.recommend;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.util.HashMap;

import static akka.testkit.JavaTestKit.duration;

/**
 * @author arvind on 27/6/17.
 */
public class RecommendorActorTest {


    static ActorSystem system;
    final static Props props = Props.create(RecommendorActor.class);

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
    }

    @Test()
    public void getRecommendedCourses() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1");
        reqObj.setOperation(ActorOperations.GET_RECOMMENDED_COURSES.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.REQUESTED_BY, "USR");
        innerMap.put(JsonKey.RECOMMEND_TYPE, JsonKey.COURSE);
        reqObj.setRequest(innerMap);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("1000 second"),Response.class);

    }

    @Test()
    public void getRecommendedContents() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1");
        reqObj.setOperation(ActorOperations.GET_RECOMMENDED_COURSES.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.REQUESTED_BY, "USR");
        innerMap.put(JsonKey.RECOMMEND_TYPE, JsonKey.CONTENT);
        reqObj.setRequest(innerMap);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("1000 second"),Response.class);

    }

    @Test()
    public void getRecommendedCoursesWithInvelidRecommendType() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1");
        reqObj.setOperation(ActorOperations.GET_RECOMMENDED_COURSES.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.REQUESTED_BY, "USR");
        innerMap.put(JsonKey.RECOMMEND_TYPE, "INVALID_RECOMMEND_TYPE");
        reqObj.setRequest(innerMap);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }

    @Test()
    public void onReceiveTestWithInvalidOperation() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1211");
        reqObj.setOperation("INVALID_OPERATION");
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }

}
