package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.util.HashMap;
import java.util.Map;

import static akka.testkit.JavaTestKit.duration;

/**
 * @author arvind .
 */
public class CourseManagementActorTest {

    static ActorSystem system;
    final static Props props = Props.create(CourseManagementActor.class);
    private static TestActorRef<CourseManagementActor> ref;
    static CourseManagementActor courseManagementActor;

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        ref = TestActorRef.create(system, props, "testActor");
        courseManagementActor = ref.underlyingActor();
    }

    @Test
    public void onReceiveTest() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.CREATE_COURSE.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> courseObject = new HashMap<String , Object>();
        courseObject.put(JsonKey.CONTENT_ID , "123");
        courseObject.put(JsonKey.COURSE_NAME , "PHYSICS");
        courseObject.put(JsonKey.ORGANISATION_ID , "121");
        innerMap.put(JsonKey.COURSE , courseObject);
        innerMap.put(JsonKey.REQUESTED_BY,"user-001");
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }
}
