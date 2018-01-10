package org.sunbird.learner.actors.skill;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.LearnerStateActor;
import org.sunbird.learner.util.Util;

/**
 * Created by arvind on 24/10/17.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SkillmanagementActorTest {

  static ActorSystem system;
  final static Props props = Props.create(SkillmanagementActor.class);
  static TestActorRef<LearnerStateActor> ref;
  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static Util.DbInfo userSkillDbInfo = Util.dbInfoMap.get(JsonKey.USER_SKILL_DB);
  private static Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private static final String USER_ID = "vcurc633r89yv";
  private static final String ROOT_ORG_ID = "7838hhucy83yuuy";
  private static final String ENDORSED_USER_ID = "nmnkfiiuvcehuybgu";
  private static final String SKILL_NAME="Java";
  private static List<String> skillsList = new ArrayList<>();

  @BeforeClass
  public static void setUp(){
    system = ActorSystem.create("system");

    skillsList.add("Java");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    insertUserDataInCassandraAndEs();

  }

  private static void insertUserDataInCassandraAndEs() {

    Map<String , Object> userMap = new HashMap<>();
    userMap.put(JsonKey.ID , USER_ID);
    userMap.put(JsonKey.ROOT_ORG_ID, ROOT_ORG_ID);
    cassandraOperation.insertRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), userMap);
    userMap.put(JsonKey.USER_ID, USER_ID);
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), USER_ID, userMap);
    Map<String , Object> endorseduserMap = new HashMap<>();
    endorseduserMap.put(JsonKey.ID , ENDORSED_USER_ID);
    endorseduserMap.put(JsonKey.ROOT_ORG_ID, ROOT_ORG_ID);
    cassandraOperation.insertRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), endorseduserMap);
    endorseduserMap.put(JsonKey.USER_ID , ENDORSED_USER_ID);
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), ENDORSED_USER_ID, endorseduserMap);

  }

  @Test
  public void testaAddSkill(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.put(JsonKey.ENDORSED_USER_ID , ENDORSED_USER_ID);
    actorMessage.put(JsonKey.SKILL_NAME, skillsList);
    actorMessage.setOperation(ActorOperations.ADD_SKILL.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res= probe.expectMsgClass(duration("100 second"),Response.class);

  }

  @Test
  public void testabAddSkillAgain(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.put(JsonKey.ENDORSED_USER_ID , ENDORSED_USER_ID);
    actorMessage.put(JsonKey.SKILL_NAME, skillsList);
    actorMessage.setOperation(ActorOperations.ADD_SKILL.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res= probe.expectMsgClass(duration("100 second"),Response.class);

  }

  @Test
  public void testbAddSkillWithInvalidUserId(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.put(JsonKey.ENDORSED_USER_ID , ENDORSED_USER_ID+1123);
    actorMessage.put(JsonKey.SKILL_NAME, skillsList);
    actorMessage.setOperation(ActorOperations.ADD_SKILL.getValue());

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res= probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);

  }

  @Test
  public void testcGetSkill(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.put(JsonKey.ENDORSED_USER_ID , ENDORSED_USER_ID);
    actorMessage.setOperation(ActorOperations.GET_SKILL.getValue());

    subject.tell(actorMessage, probe.getRef());
    probe.expectMsgClass(duration("10 second"),Response.class);

  }

  @Test
  public void testdGetSkillWithInvalidUserId(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY , USER_ID);
    actorMessage.put(JsonKey.ENDORSED_USER_ID , ENDORSED_USER_ID+1123);
    actorMessage.setOperation(ActorOperations.GET_SKILL.getValue());

    subject.tell(actorMessage, probe.getRef());
    probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);

  }

  @Test
  public void testeSkills(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.setOperation(ActorOperations.GET_SKILLS_LIST.getValue());

    subject.tell(actorMessage, probe.getRef());
    probe.expectMsgClass(duration("10 second"),Response.class);

  }

  @Test
  public void testwithinvalidOperationName(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.setOperation(ActorOperations.GET_SKILLS_LIST.getValue()+"Invalid");

    subject.tell(actorMessage, probe.getRef());
    probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);

  }

  @Test
  public void testwithUnSupportedMsg(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    subject.tell("UNSUPPORTED OJECT STRING", probe.getRef());
    probe.expectMsgClass(duration("10 second"),ProjectCommonException.class);

  }

  @AfterClass
  public static void destroy(){
    cassandraOperation.deleteRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(),USER_ID);
    cassandraOperation.deleteRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(),ENDORSED_USER_ID);
    ElasticSearchUtil.removeData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), USER_ID);
    ElasticSearchUtil.removeData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), ENDORSED_USER_ID);
    cassandraOperation.deleteRecord(userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(),
        OneWayHashing.encryptVal(ENDORSED_USER_ID + JsonKey.PRIMARY_KEY_DELIMETER + SKILL_NAME.toLowerCase()));

  }

}
