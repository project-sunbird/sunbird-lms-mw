package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
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
    private static CassandraOperation cassandraOperation = new CassandraOperationImpl();
    private static Util.DbInfo contentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
    private static Util.DbInfo coursedbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    static String userId = "user121gama";
    static String courseId = "alpha01crs";
    private static final String contentId = "cont3544TeBuk";

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        ref = TestActorRef.create(system, props, "testActor");
        insertCourse();
        insertContent();
    }

    private static void insertContent() {

        Map<String , Object> contentMap = new HashMap<String , Object>();
        String key = userId + JsonKey.PRIMARY_KEY_DELIMETER + contentId + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId;
        String id = OneWayHashing.encryptVal(key);
        contentMap.put(JsonKey.ID , id);
        contentMap.put(JsonKey.COURSE_ID , courseId);
        contentMap.put(JsonKey.USER_ID , userId);
        contentMap.put(JsonKey.CONTENT_ID , contentId);
        cassandraOperation.insertRecord(contentdbInfo.getKeySpace() , contentdbInfo.getTableName() , contentMap);

    }

    private static void insertCourse() {
        Map<String , Object> courseMap = new HashMap<String , Object>();
        courseMap.put(JsonKey.ID , OneWayHashing.encryptVal(userId+JsonKey.PRIMARY_KEY_DELIMETER+courseId));
        courseMap.put(JsonKey.COURSE_ID , courseId);
        courseMap.put(JsonKey.USER_ID , userId);
        courseMap.put(JsonKey.CONTENT_ID , courseId);
        cassandraOperation.insertRecord(coursedbInfo.getKeySpace() , coursedbInfo.getTableName() , courseMap);

    }

    @Test
    public void onReceiveTestForGetCourse() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request request = new Request();
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.USER_ID, userId);
        request.setRequest(map);
        request.setOperation(ActorOperations.GET_COURSE.getValue());
        subject.tell(request, probe.getRef());
        Response res= probe.expectMsgClass(duration("10 second"),Response.class);
        List list = (List) res.getResult().get(JsonKey.RESPONSE);
        Assert.assertEquals(1 , list.size());

    }

    @Test
    public void onReceiveTestForGetCourseWithInvalidOperation() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request request = new Request();
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.USER_ID, userId);
        request.setRequest(map);
        request.setOperation("INVALID_OPERATION");
        subject.tell(request, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }

    @Test
    public void onReceiveTestForGetCourseWithInvalidRequestType() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        subject.tell("INVALID REQUEST", probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }

    @Test
    public void onReceiveTestForGetContent() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        HashMap<String, Object> innerMap = new HashMap<>();
        Request request = new Request();
        innerMap.put(JsonKey.USER_ID, userId);
        List<String> contentList = Arrays.asList(contentId);
        innerMap.put(JsonKey.CONTENT_IDS, contentList);
        request.setRequest(innerMap);
        request.setOperation(ActorOperations.GET_CONTENT.getValue());
        subject.tell(request, probe.getRef());
        Response res= probe.expectMsgClass(duration("10 second"),Response.class);
        List list = (List) res.getResult().get(JsonKey.RESPONSE);
        Assert.assertEquals(1 , list.size());
    }

    @Test
    public void onReceiveTestForGetContentByCourse() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        HashMap<String, Object> innerMap = new HashMap<>();
        Request request = new Request();
        innerMap.put(JsonKey.USER_ID, userId);
        innerMap.put(JsonKey.COURSE_ID , courseId);
        List<String> contentList = Arrays.asList(contentId);
        innerMap.put(JsonKey.CONTENT_IDS, contentList);
        innerMap.put(JsonKey.COURSE, innerMap);
        request.setRequest(innerMap);
        request.setOperation(ActorOperations.GET_CONTENT.getValue());
        subject.tell(request, probe.getRef());
        Response res= probe.expectMsgClass(duration("10 second"),Response.class);
        List list = (List) res.getResult().get(JsonKey.RESPONSE);
        Assert.assertEquals(1 , list.size());
    }

    @Test
    public void onReceiveTestForGetContentByCourseIds() throws Exception {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        HashMap<String, Object> innerMap = new HashMap<>();
        Request request = new Request();
        innerMap.put(JsonKey.USER_ID, userId);
        List<String> courseList = Arrays.asList(courseId);
        innerMap.put(JsonKey.COURSE_IDS, courseList);
        request.setRequest(innerMap);
        request.setOperation(ActorOperations.GET_CONTENT.getValue());
        subject.tell(request, probe.getRef());
        Response res= probe.expectMsgClass(duration("10 second"),Response.class);
        List list = (List) res.getResult().get(JsonKey.RESPONSE);
        Assert.assertEquals(1 , list.size());
    }


    @AfterClass
    public static void destroy(){
        cassandraOperation.deleteRecord(coursedbInfo.getKeySpace() , coursedbInfo.getTableName(),OneWayHashing.encryptVal(userId+JsonKey.PRIMARY_KEY_DELIMETER+courseId));
        String key = userId + JsonKey.PRIMARY_KEY_DELIMETER + contentId + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId;
        String contentid = OneWayHashing.encryptVal(key);
        cassandraOperation.deleteRecord(contentdbInfo.getKeySpace() , contentdbInfo.getTableName(),contentid);
    }

}
