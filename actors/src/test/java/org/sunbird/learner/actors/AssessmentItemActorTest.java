package org.sunbird.learner.actors;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.assessment.AssessmentItemActor;
import org.sunbird.learner.util.Util;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;

public class AssessmentItemActorTest {

    static ActorSystem system;
    final static  Props props = Props.create(AssessmentItemActor.class);
    private static TestActorRef<AssessmentItemActor> ref;

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
        ref = TestActorRef.create(system, props, "testActor");
    }


    @Test()
    public void onReceiveTest() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequestId("121");
        reqObj.setOperation(ActorOperations.GET_ASSESSMENT.getValue());
        reqObj.getRequest().put(JsonKey.CONTENT_ID, "1");
        reqObj.getRequest().put(JsonKey.COURSE_ID, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_ITEM_ID, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_TYPE, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_ATTEMPT_DATE, "11");
        reqObj.getRequest().put(JsonKey.TIME_TAKEN, new BigInteger("11"));
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> assessmentMap = new HashMap<String , Object>();
        assessmentMap.put(JsonKey.ID , "123");
        List<String> userids = new ArrayList<String>();
        userids.add("1");
        assessmentMap.put(JsonKey.USERIDS ,userids);
        assessmentMap.put(JsonKey.COURSE_ID , "123");
        innerMap.put(JsonKey.ASSESSMENT, assessmentMap);
        innerMap.put(JsonKey.USER_ID, "USR1");
        innerMap.put(JsonKey.ID, "");
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test()
    public void onReceiveTestWithoutUserIds() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequestId("121");
        reqObj.setOperation(ActorOperations.GET_ASSESSMENT.getValue());
        reqObj.getRequest().put(JsonKey.CONTENT_ID, "1");
        reqObj.getRequest().put(JsonKey.COURSE_ID, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_ITEM_ID, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_TYPE, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_ATTEMPT_DATE, "11");
        reqObj.getRequest().put(JsonKey.TIME_TAKEN, new BigInteger("11"));
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> assessmentMap = new HashMap<String , Object>();
        assessmentMap.put(JsonKey.ID , "123");
        assessmentMap.put(JsonKey.COURSE_ID , "123");
        innerMap.put(JsonKey.ASSESSMENT, assessmentMap);
        innerMap.put(JsonKey.USER_ID, "USR1");
        innerMap.put(JsonKey.ID, "");
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test
    public void onReceiveTestSaveAssessment() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequestId("121");
        reqObj.setOperation(ActorOperations.SAVE_ASSESSMENT.getValue());
        reqObj.getRequest().put(JsonKey.CONTENT_ID, "1");
        reqObj.getRequest().put(JsonKey.COURSE_ID, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_ITEM_ID, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_TYPE, "11");
        reqObj.getRequest().put(JsonKey.ASSESSMENT_ATTEMPT_DATE, "11");
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.COURSE, reqObj.getRequest());
        innerMap.put(JsonKey.USER_ID, "USR1");
        innerMap.put(JsonKey.ID, "");
       // innerMap.put("unknown-column", "bad-value");
        Map<String , Object> assessmentMap = new HashMap<String , Object>();
        assessmentMap.put(JsonKey.ID , "123");
        assessmentMap.put(JsonKey.TIME_TAKEN , new BigInteger("10"));
        innerMap.put(JsonKey.ASSESSMENT , assessmentMap);
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
       // probe.expectMsgClass(Response.class);

    }

    @Test
    public void onReceiveTestWithInvalidOperation() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequestId("121");
        reqObj.setOperation("INVALID_OPERATION");
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.COURSE, reqObj.getRequest());
        innerMap.put(JsonKey.USER_ID, "USR1");
        innerMap.put(JsonKey.ID, "");
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }

    @Test
    public void onReceiveTestWithUnsupportedMessage() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        subject.tell(new String("UNSUPPORTED"), probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }


}
