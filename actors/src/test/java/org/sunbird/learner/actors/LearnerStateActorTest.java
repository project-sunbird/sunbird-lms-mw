package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;

/**
 * @author  arvind
 */
public class LearnerStateActorTest {

    private static ActorSystem system;
    private static final Props props = Props.create(LearnerStateActor.class);
    private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private static Util.DbInfo contentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
    private static Util.DbInfo coursedbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    private static String userId = "user121gama";
    private static String courseId = "alpha01crs12";
    private static String batchId ="115";
    private static final String contentId = "cont3544TeBuk";

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
        insertCourse();
        insertContent();
    }

    private static void insertContent() {

        Map<String , Object> contentMap = new HashMap<String , Object>();
        String key = userId + JsonKey.PRIMARY_KEY_DELIMETER + contentId + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId;
        String id = OneWayHashing.encryptVal(key);
        contentMap.put(JsonKey.ID , id);
        contentMap.put(JsonKey.COURSE_ID , courseId);
        contentMap.put(JsonKey.USER_ID , userId);
        contentMap.put(JsonKey.CONTENT_ID , contentId);
        contentMap.put(JsonKey.BATCH_ID , batchId);
        System.out.println("CONTENT ID "+id);
        cassandraOperation.insertRecord(contentdbInfo.getKeySpace() , contentdbInfo.getTableName() , contentMap);

    }

    private static void insertCourse() {
        Map<String , Object> courseMap = new HashMap<String , Object>();
        courseMap.put(JsonKey.ID , OneWayHashing.encryptVal(userId+JsonKey.PRIMARY_KEY_DELIMETER+courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId));
        courseMap.put(JsonKey.COURSE_ID , courseId);
        courseMap.put(JsonKey.USER_ID , userId);
        courseMap.put(JsonKey.CONTENT_ID , courseId);
        courseMap.put(JsonKey.BATCH_ID , batchId);
        cassandraOperation.insertRecord(coursedbInfo.getKeySpace() , coursedbInfo.getTableName() , courseMap);
    }

    //@Test
    public void onReceiveTestForGetCourse() {

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
    public void onReceiveTestForGetCourseWithInvalidOperation() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request request = new Request();
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.USER_ID, userId);
        request.setRequest(map);
        request.setOperation("INVALID_OPERATION");
        subject.tell(request, probe.getRef());
        ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
        Assert.assertTrue(null != exc);

    }

    @Test
    public void onReceiveTestForGetCourseWithInvalidRequestType() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        subject.tell("INVALID REQUEST", probe.getRef());
        ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
        Assert.assertTrue(null != exc);

    }

    @Test
    public void onReceiveTestForGetContent() {

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
    public void onReceiveTestForGetContentByCourse() {

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
    public void onReceiveTestForGetContentByCourseIds() {

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
        cassandraOperation.deleteRecord(coursedbInfo.getKeySpace() , coursedbInfo.getTableName(),OneWayHashing.encryptVal(userId+JsonKey.PRIMARY_KEY_DELIMETER+courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId));
        String key = userId + JsonKey.PRIMARY_KEY_DELIMETER + contentId + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId;
        String contentid = OneWayHashing.encryptVal(key);
        System.out.println("CONTENT ID "+contentid);
        cassandraOperation.deleteRecord(contentdbInfo.getKeySpace() , contentdbInfo.getTableName(),contentid);
    }

}
