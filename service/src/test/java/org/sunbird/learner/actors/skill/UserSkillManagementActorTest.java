package org.sunbird.learner.actors.skill;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.sql.Timestamp;
import java.util.*;
import org.junit.*;
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
  private static final String ENDORSE_SKILL_NAME = "C";
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
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        USER_ID,
        userMap);
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
    createEndorseSkillInDb();
    updateES(USER_ID);
  }

  private Request getAddSkillRequest(String userId, String endorseUserId, List<String> skillsList) {

    Request actorMessage = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.ENDORSED_USER_ID, endorseUserId);
    actorMessage.put(JsonKey.SKILL_NAME, skillsList);
    actorMessage.setOperation(ActorOperations.ADD_SKILL.getValue());

    return actorMessage;
  }

  private Request getGetSkillRequest(String userId, String endorseUserId) {

    Request actorMessage = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.ENDORSED_USER_ID, endorseUserId);
    actorMessage.setOperation(ActorOperations.GET_SKILL.getValue());

    return actorMessage;
  }

  private Request getAddSkillEndorsementRequest(
      String userId, String endorseUserId, String skillName) {

    Request actorMessage = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.SKILL_NAME, skillName);
    actorMessage.put(JsonKey.ENDORSED_USER_ID, endorseUserId);
    actorMessage.setOperation(ActorOperations.GET_SKILL.getValue());

    return actorMessage;
  }

  @Test
  public void testAddSkill() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getAddSkillRequest(USER_ID, ENDORSED_USER_ID, skillsList), probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testAddSkillAgain() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getAddSkillRequest(USER_ID, ENDORSED_USER_ID, skillsList), probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testUpdateSkill() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = getUpdateSkillRequest(USER_ID, skillsList);
    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testUpdateSkillWithInvalidUserId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = getUpdateSkillRequest(USER_ID + 1202, skillsList);
    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testAddSkillWithInvalidUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getAddSkillRequest(USER_ID, ENDORSED_USER_ID + 1123, skillsList), probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testAddSkillEndorsement() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(
        getSkillEndorsementResponse(USER_ID, ENDORSED_USER_ID, ENDORSE_SKILL_NAME), probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testAddSkillEndorsementWithInvalidEndorsedUserId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(
        getSkillEndorsementResponse(USER_ID, ENDORSED_USER_ID + 1122, ENDORSE_SKILL_NAME),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testAddSkillEndorsementWithInvalidUserId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(
        getSkillEndorsementResponse(USER_ID + 11221, ENDORSED_USER_ID, ENDORSE_SKILL_NAME),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  private Request getSkillEndorsementResponse(
      String userId, String endorsedUserId, String skillName) {
    Request actorMessage = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.SKILL_NAME, skillName);
    actorMessage.put(JsonKey.ENDORSED_USER_ID, endorsedUserId);
    actorMessage.setOperation(ActorOperations.GET_SKILL.getValue());
    return actorMessage;
  }

  @Test
  public void testGetSkill() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getGetSkillRequest(USER_ID, ENDORSED_USER_ID), probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testGetSkillWithInvalidUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getGetSkillRequest(USER_ID, ENDORSED_USER_ID + 1123), probe.getRef());
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
  public void testWithUnvalidOperationName() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.setOperation(ActorOperations.GET_SKILLS_LIST.getValue() + "Invalid");

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  private Request getUpdateSkillRequest(String userid, List<String> skills) {
    Request actorMessage = new Request();
    actorMessage.put(JsonKey.USER_ID, userid);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userid);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.SKILLS, skills);
    actorMessage.setOperation(ActorOperations.UPDATE_SKILL.getValue());
    return actorMessage;
  }

  @AfterClass
  public static void destroy() {
    cassandraOperation.deleteRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), USER_ID);
    cassandraOperation.deleteRecord(
        userDbInfo.getKeySpace(), userDbInfo.getTableName(), ENDORSED_USER_ID);
    ElasticSearchUtil.removeData(
        ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), USER_ID);
    ElasticSearchUtil.removeData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(),
        ENDORSED_USER_ID);
    cassandraOperation.deleteRecord(
        userSkillDbInfo.getKeySpace(),
        userSkillDbInfo.getTableName(),
        OneWayHashing.encryptVal(
            ENDORSED_USER_ID + JsonKey.PRIMARY_KEY_DELIMETER + SKILL_NAME.toLowerCase()));
    cassandraOperation.deleteRecord(
        userSkillDbInfo.getKeySpace(),
        userSkillDbInfo.getTableName(),
        OneWayHashing.encryptVal(
            ENDORSED_USER_ID + JsonKey.PRIMARY_KEY_DELIMETER + ENDORSE_SKILL_NAME.toLowerCase()));
  }

  private static void createEndorseSkillInDb() {
    String id =
        OneWayHashing.encryptVal(
            USER_ID + JsonKey.PRIMARY_KEY_DELIMETER + ENDORSE_SKILL_NAME.toLowerCase());
    Map<String, Object> userSkillMap = new HashMap<>();
    userSkillMap.put(JsonKey.ID, id);
    userSkillMap.put(JsonKey.USER_ID, USER_ID);
    userSkillMap.put(JsonKey.SKILL_NAME, ENDORSE_SKILL_NAME);
    userSkillMap.put(JsonKey.SKILL_NAME_TO_LOWERCASE, ENDORSE_SKILL_NAME.toLowerCase());
    userSkillMap.put(JsonKey.CREATED_BY, USER_ID);
    userSkillMap.put(JsonKey.CREATED_ON, new Timestamp(Calendar.getInstance().getTime().getTime()));
    userSkillMap.put(JsonKey.LAST_UPDATED_BY, USER_ID);
    userSkillMap.put(
        JsonKey.LAST_UPDATED_ON, new Timestamp(Calendar.getInstance().getTime().getTime()));
    userSkillMap.put(JsonKey.ENDORSEMENT_COUNT, 0);
    cassandraOperation.insertRecord(
        userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), userSkillMap);
  }

  private static void updateES(String userId) {

    // get all records from cassandra as list and add that list to user in
    // ElasticSearch ...
    Response response =
        cassandraOperation.getRecordsByProperty(
            userSkillDbInfo.getKeySpace(), userSkillDbInfo.getTableName(), JsonKey.USER_ID, userId);
    List<Map<String, Object>> responseList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    Map<String, Object> esMap = new HashMap<>();
    esMap.put(JsonKey.SKILLS, responseList);
    Map<String, Object> profile =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId);
    if (null != profile && !profile.isEmpty()) {
      Map<String, String> visibility =
          (Map<String, String>) profile.get(JsonKey.PROFILE_VISIBILITY);
      if ((null != visibility && !visibility.isEmpty()) && visibility.containsKey(JsonKey.SKILLS)) {
        Map<String, Object> visibilityMap =
            ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.userprofilevisibility.getTypeName(),
                userId);
        if (null != visibilityMap && !visibilityMap.isEmpty()) {
          visibilityMap.putAll(esMap);
          ElasticSearchUtil.createData(
              ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.userprofilevisibility.getTypeName(),
              userId,
              visibilityMap);
        }
      } else {
        ElasticSearchUtil.updateData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId,
            esMap);
      }
    }
  }
}
