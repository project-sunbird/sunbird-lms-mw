package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author  arvind
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LearnerStateUpdateActorTest {

    static ActorSystem system;
    final static Props props = Props.create(LearnerStateUpdateActor.class);
    static String userId = "user121gama";
    static String courseId = "alpha01crs";
    private static final String contentId = "cont3544TeBukGame";
    private static CassandraOperation cassandraOperation = new CassandraOperationImpl();
    private static Util.DbInfo contentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
    private static Util.DbInfo coursedbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    private static final String batchId = "220";

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        insertCourse();
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

    @Test
    public void addContentTest() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request req = new Request();
        List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
        Map<String , Object> content1 = createContent();
        content1.put(JsonKey.STATUS , new BigInteger("2"));
        contentList.add(content1);

        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.CONTENTS, contentList);
        innerMap.put(JsonKey.USER_ID, userId);
        req.setOperation(ActorOperations.ADD_CONTENT.getValue());
        req.setRequest(innerMap);
        subject.tell(req, probe.getRef());
        //probe.expectMsgClass(Response.class);
        Thread.sleep(3000);
        Response dbbRes = cassandraOperation.getRecordsByProperty(contentdbInfo.getKeySpace() , contentdbInfo.getTableName() , JsonKey.CONTENT_ID , contentId);
        List list = (List)dbbRes.getResult().get(JsonKey.RESPONSE);
        Assert.assertEquals(1 , list.size());

    }

    @Test
    public void updateContentTest() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request req = new Request();
        List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
        Map<String , Object> content1 = createContent();
        content1.put(JsonKey.STATUS , new BigInteger("1"));

        contentList.add(content1);
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.CONTENTS, contentList);
        innerMap.put(JsonKey.USER_ID, userId);
        req.setOperation(ActorOperations.ADD_CONTENT.getValue());
        req.setRequest(innerMap);

        subject.tell(req, probe.getRef());
        probe.expectMsgClass(Response.class);
        Thread.sleep(3000);
        Response dbbRes = cassandraOperation.getRecordsByProperty(contentdbInfo.getKeySpace() , contentdbInfo.getTableName() , JsonKey.CONTENT_ID , contentId);
        List list = (List)dbbRes.getResult().get(JsonKey.RESPONSE);
        Assert.assertEquals(1 , list.size());

    }

    @Test
    public void updateContentTest_001() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request req = new Request();
        List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
        Map<String , Object> content1 = createContent();
        content1.put(JsonKey.STATUS , new BigInteger("2"));

        contentList.add(content1);
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.CONTENTS, contentList);
        innerMap.put(JsonKey.USER_ID, userId);
        req.setOperation(ActorOperations.ADD_CONTENT.getValue());
        req.setRequest(innerMap);

        subject.tell(req, probe.getRef());
        probe.expectMsgClass(Response.class);
        Thread.sleep(3000);
        Response dbbRes = cassandraOperation.getRecordsByProperty(contentdbInfo.getKeySpace() , contentdbInfo.getTableName() , JsonKey.CONTENT_ID , contentId);
        List list = (List)dbbRes.getResult().get(JsonKey.RESPONSE);
        Assert.assertEquals(1 , list.size());

    }

    @Test
    public void updateContentTestWithInvalidDateFormat() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request req = new Request();
        List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
        Map<String , Object> content1 = createContent();
        content1.put(JsonKey.STATUS , new BigInteger("2"));
        content1.put(JsonKey.LAST_ACCESS_TIME , "2017-87-20 21:03:29:599+0530");

        contentList.add(content1);
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.CONTENTS, contentList);
        innerMap.put(JsonKey.USER_ID, userId);
        req.setOperation(ActorOperations.ADD_CONTENT.getValue());
        req.setRequest(innerMap);

        subject.tell(req, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }

    @Test
    public void onReceiveUpdateContentWithInvalidOperationTest() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request req = new Request();
        req.setOperation("INVALID_OPERATION");
        subject.tell(req, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }

    @Test
    public void onReceiveUpdateContentWithInvalidRequest() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        subject.tell("INVALID REQUEST", probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }


    private Map<String , Object> createContent(){

        Map<String , Object> content = new HashMap<String , Object>();
        String key = userId + JsonKey.PRIMARY_KEY_DELIMETER + contentId + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId;
        content.put(JsonKey.LAST_ACCESS_TIME , ProjectUtil.getFormattedDate());
        content.put(JsonKey.COMPLETED_COUNT , "0");
        content.put(JsonKey.STATUS , "1");
        content.put(JsonKey.LAST_UPDATED_TIME , ProjectUtil.getFormattedDate());
        content.put(JsonKey.LAST_COMPLETED_TIME , ProjectUtil.getFormattedDate());
        String id = OneWayHashing.encryptVal(key);
        content.put(JsonKey.ID , id);
        content.put(JsonKey.COURSE_ID , courseId);
        content.put(JsonKey.USER_ID , userId);
        content.put(JsonKey.CONTENT_ID , contentId);
        content.put(JsonKey.BATCH_ID , batchId);
        content.put(JsonKey.PROGRESS , new BigInteger("100"));
        return content;
    }

    @AfterClass
    public static void destroy(){

        cassandraOperation.deleteRecord(coursedbInfo.getKeySpace() , coursedbInfo.getTableName(),
            OneWayHashing.encryptVal(userId+JsonKey.PRIMARY_KEY_DELIMETER+courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId));
        String key = userId + JsonKey.PRIMARY_KEY_DELIMETER + contentId + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId;
        String contentid = OneWayHashing.encryptVal(key);
        cassandraOperation.deleteRecord(contentdbInfo.getKeySpace() , contentdbInfo.getTableName(),contentid);

    }

}
