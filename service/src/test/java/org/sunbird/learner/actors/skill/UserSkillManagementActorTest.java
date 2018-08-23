package org.sunbird.learner.actors.skill;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static akka.testkit.JavaTestKit.duration;

public class UserSkillManagementActorTest {

    private static ActorSystem system;
    private static final Props props = Props.create(UserSkillManagementActor.class);
    private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private static Util.DbInfo userSkillDbInfo = Util.dbInfoMap.get(JsonKey.USER_SKILL_DB);
    private static Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    private static final String USER_ID = "vcurc633r89yv";
    private static final String ROOT_ORG_ID = "7838hhucy83yuuy";
    private static final String ENDORSED_USER_ID = "nmnkfiiuvcehuybgu";
    private static final String SKILL_NAME = "Java";
    private static List<String> skillsList = new ArrayList<>();

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        skillsList.add("Java");
        Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
        insertUserDataInCassandraAndEs();
    }

    private static void insertUserDataInCassandraAndEs() {

        Map<String, Object> userMap = new HashMap<>();
        userMap.put(JsonKey.ID, USER_ID);
        userMap.put(JsonKey.ROOT_ORG_ID, ROOT_ORG_ID);
        cassandraOperation.insertRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), userMap);
        userMap.put(JsonKey.USER_ID, USER_ID);
        ElasticSearchUtil.createData(
                ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), USER_ID, userMap);
        Map<String, Object> endorseduserMap = new HashMap<>();
        endorseduserMap.put(JsonKey.ID, ENDORSED_USER_ID);
        endorseduserMap.put(JsonKey.ROOT_ORG_ID, ROOT_ORG_ID);
        cassandraOperation.insertRecord(
                userDbInfo.getKeySpace(), userDbInfo.getTableName(), endorseduserMap);
        endorseduserMap.put(JsonKey.USER_ID, ENDORSED_USER_ID);
        ElasticSearchUtil.createData(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName(),
                ENDORSED_USER_ID,
                endorseduserMap);
    }

    private Response addSkill(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request actorMessage = new Request();
        HashMap<String,Object> innerMap =new HashMap<>();
        innerMap.put(JsonKey.REQUESTED_BY, USER_ID);
        actorMessage.setContext(innerMap);
        actorMessage.put(JsonKey.ENDORSED_USER_ID, ENDORSED_USER_ID);
        actorMessage.put(JsonKey.SKILL_NAME, skillsList);
        actorMessage.setOperation(ActorOperations.ADD_SKILL.getValue());

        subject.tell(actorMessage, probe.getRef());
        return probe.expectMsgClass(duration("100 second"), Response.class);
    }

    @Test
    public void testAddSkill() {
        Response res = addSkill();
        Assert.assertTrue(null != res);
    }

    @Test
    public void testAddSkillAgain() {
        Response res = addSkill();
        Assert.assertTrue(null != res);
    }

    @Test
    public void testUpdateSkill(){
        Response res = updateAndDeleteSkill();
        Assert.assertTrue(null != res);
    }

    @Test
    public void testbAddSkillWithInvalidUserId() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request actorMessage = new Request();
        actorMessage.put(JsonKey.REQUESTED_BY, USER_ID);
        actorMessage.put(JsonKey.ENDORSED_USER_ID, ENDORSED_USER_ID + 1123);
        actorMessage.put(JsonKey.SKILL_NAME, skillsList);
        actorMessage.setOperation(ActorOperations.ADD_SKILL.getValue());

        subject.tell(actorMessage, probe.getRef());
        ProjectCommonException exc =
                probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
        Assert.assertTrue(null != exc);
    }

    @Test
    public void testGetSkill() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request actorMessage = new Request();
        actorMessage.put(JsonKey.REQUESTED_BY, USER_ID);
        actorMessage.put(JsonKey.ENDORSED_USER_ID, ENDORSED_USER_ID);
        actorMessage.setOperation(ActorOperations.GET_SKILL.getValue());

        subject.tell(actorMessage, probe.getRef());
        Response res = probe.expectMsgClass(duration("10 second"), Response.class);
        Assert.assertTrue(null != res);
    }

    @Test
    public void testGetSkillWithInvalidUserId() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request actorMessage = new Request();
        actorMessage.put(JsonKey.REQUESTED_BY, USER_ID);
        actorMessage.put(JsonKey.ENDORSED_USER_ID, ENDORSED_USER_ID + 1123);
        actorMessage.setOperation(ActorOperations.GET_SKILL.getValue());

        subject.tell(actorMessage, probe.getRef());
        ProjectCommonException exc =
                probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
        Assert.assertTrue(null != exc);
    }

    @Test
    public void testSkills() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request actorMessage = new Request();
        actorMessage.setOperation(ActorOperations.GET_SKILLS_LIST.getValue());

        subject.tell(actorMessage, probe.getRef());
        Response res = probe.expectMsgClass(duration("10 second"), Response.class);
        Assert.assertTrue(null != res);
    }

    @Test
    public void testwithinvalidOperationName() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request actorMessage = new Request();
        actorMessage.setOperation(ActorOperations.GET_SKILLS_LIST.getValue() + "Invalid");

        subject.tell(actorMessage, probe.getRef());
        ProjectCommonException exc =
                probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
        Assert.assertTrue(null != exc);
    }

    @Test(expected = ProjectCommonException.class)
    public void testwithUnSupportedMsg() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        subject.tell("UNSUPPORTED OJECT STRING", probe.getRef());

        ProjectCommonException exc =
                probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
        Assert.assertTrue(null != exc);
    }

    @AfterClass
    public static void destroy() {
        cassandraOperation.deleteRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), USER_ID);
        cassandraOperation.deleteRecord(
                userDbInfo.getKeySpace(), userDbInfo.getTableName(), ENDORSED_USER_ID);
        ElasticSearchUtil.removeData(
                ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), USER_ID);
        ElasticSearchUtil.removeData(
                ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), ENDORSED_USER_ID);
        cassandraOperation.deleteRecord(
                userSkillDbInfo.getKeySpace(),
                userSkillDbInfo.getTableName(),
                OneWayHashing.encryptVal(
                        ENDORSED_USER_ID + JsonKey.PRIMARY_KEY_DELIMETER + SKILL_NAME.toLowerCase()));
    }


    private Response updateAndDeleteSkill(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request actorMessage =updateSkillRequest();
        subject.tell(actorMessage, probe.getRef());
        return probe.expectMsgClass(duration("100 second"), Response.class);
    }

    private Request updateSkillRequest(){
        Request actorMessage = new Request();
        actorMessage.put(JsonKey.USER_ID, USER_ID);
        HashMap<String,Object> innerMap =new HashMap<>();
        innerMap.put(JsonKey.REQUESTED_BY, USER_ID);
        actorMessage.setContext(innerMap);
        actorMessage.put(JsonKey.SKILLS, skillsList);
        actorMessage.setOperation(ActorOperations.UPDATE_SKILL.getValue());
        return actorMessage;
    }

}
