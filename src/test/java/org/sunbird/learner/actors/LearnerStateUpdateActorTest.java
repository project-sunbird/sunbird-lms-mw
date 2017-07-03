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
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author  arvind
 */
public class LearnerStateUpdateActorTest {

    static ActorSystem system;
    final static Props props = Props.create(LearnerStateUpdateActor.class);
    static TestActorRef<LearnerStateUpdateActor> ref;
    private String USER_ID = "dummyUser";

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        //ref = TestActorRef.create(system, props, "testActor");
    }

    @Test
    public void onReceiveAddContentTest() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request req = new Request();
        List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
        Map<String , Object> content1 = createContent();
        content1.put(JsonKey.STATUS , new BigInteger("1"));
        contentList.add(content1);

        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.CONTENTS, contentList);
        innerMap.put(JsonKey.USER_ID, USER_ID);
        req.setOperation(ActorOperations.ADD_CONTENT.getValue());
        req.setRequest(innerMap);

        subject.tell(req, probe.getRef());
        probe.expectMsgClass(Response.class);

    }

    @Test
    public void onReceiveUpdateContentTest() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request req = new Request();
        List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
        Map<String , Object> content1 = createContent();
        content1.put(JsonKey.STATUS , new BigInteger("1"));

        contentList.add(content1);
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.CONTENTS, contentList);
        innerMap.put(JsonKey.USER_ID, USER_ID);
        req.setOperation(ActorOperations.ADD_CONTENT.getValue());
        req.setRequest(innerMap);

        subject.tell(req, probe.getRef());
        probe.expectMsgClass(Response.class);

    }

    @Test
    public void onReceiveUpdateContentWithInvalidOperationTest() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request req = new Request();
        List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
        Map<String , Object> content1 = createContent();

        contentList.add(content1);
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.CONTENTS, contentList);
        innerMap.put(JsonKey.USER_ID, USER_ID);
        req.setOperation("INVALID_OPERATION");
        req.setRequest(innerMap);

        subject.tell(req, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }


    private Map<String , Object> createContent(){

        Map<String , Object> content = new HashMap<String , Object>();
        content.put(JsonKey.CONTENT_ID, "123");
        content.put(JsonKey.USER_ID , USER_ID);
        content.put(JsonKey.VIEW_POSITION , "123");
        content.put(JsonKey.VIEW_COUNT , "123");
        content.put(JsonKey.LAST_ACCESS_TIME , ProjectUtil.getFormattedDate());
        content.put(JsonKey.COMPLETED_COUNT , "0");
        content.put(JsonKey.STATUS , "1");
        content.put(JsonKey.LAST_UPDATED_TIME , ProjectUtil.getFormattedDate());
        content.put(JsonKey.LAST_COMPLETED_TIME , ProjectUtil.getFormattedDate());

        return content;
    }

}
