package org.sunbird.learner.actors.skill;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, ElasticSearchUtil.class})
@PowerMockIgnore("javax.management.*")
public class UserSkillManagementActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(UserSkillManagementActor.class);
  private static CassandraOperation cassandraOperation;
  private static final String USER_ID = "someUserId";
  private static final String ROOT_ORG_ID = "someRootOrgId";
  private static final String ENDORSED_USER_ID = "someEndorsedUserId";
  private static final String ENDORSE_SKILL_NAME = "C";
  private static List<String> skillsList = new ArrayList<>();

  @BeforeClass
  public static void setUp() {
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    system = ActorSystem.create("system");
  }

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Mockito.reset(cassandraOperation);
  }

  @Test
  public void testAddSkill() {
    addSkill();
  }

  @Test
  public void testUpdateSkill() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = createUpdateSkillRequest(USER_ID, skillsList);
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(createGetUserSuccessResponse());
    mockGetSkillResponse(ENDORSED_USER_ID);
    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateSkillWithInvalidUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = createUpdateSkillRequest(USER_ID + 1202, skillsList);
    mockCasandraRequestForGetUserFailure();

    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testAddSkillWithInvalidUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(
        createAddSkillRequest(USER_ID, ENDORSED_USER_ID + 1123, skillsList), probe.getRef());
    mockCasandraRequestForGetUserFailure();
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  public Response createSkillEndorsementResponse() {
    Response response = new Response();
    List<HashMap<String, Object>> result = new ArrayList<>();
    HashMap<String, Object> skill = new HashMap<>();
    skill.put(JsonKey.SKILL_NAME, ENDORSE_SKILL_NAME);
    skill.put(JsonKey.SKILL_NAME_TO_LOWERCASE, ENDORSE_SKILL_NAME.toLowerCase());
    skill.put(JsonKey.ENDORSERS_LIST, new ArrayList<>());
    result.add(skill);
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  @Test
  public void testAddSkillEndorsementWithInvalidEndorsedUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    mockCasandraRequestForGetUserFailure();
    subject.tell(
        getSkillEndorsementRequest(USER_ID, ENDORSED_USER_ID + 1122, ENDORSE_SKILL_NAME),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testAddSkillEndorsementWithInvalidUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockCasandraRequestForGetUserFailure();
    subject.tell(
        getSkillEndorsementRequest(USER_ID + 11221, ENDORSED_USER_ID, ENDORSE_SKILL_NAME),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testGetSkill() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    //        PowerMockito.mockStatic(ElasticSearchUtil.class);
    mockGetSkillResponse(USER_ID);
    subject.tell(createGetSkillRequest(USER_ID, ENDORSED_USER_ID), probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testGetSkillWithInvalidUserId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(createGetSkillRequest(USER_ID, ENDORSED_USER_ID + 1123), probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void testSkills() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Response response = createGetSkillsSuccessResponse();
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    Request actorMessage = new Request();
    actorMessage.setOperation(ActorOperations.GET_SKILLS_LIST.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
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

  private Request createUpdateSkillRequest(String userid, List<String> skills) {
    Request actorMessage = new Request();
    actorMessage.put(JsonKey.USER_ID, userid);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userid);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.SKILLS, skills);
    actorMessage.setOperation(ActorOperations.UPDATE_SKILL.getValue());

    return actorMessage;
  }

  private void mockGetSkillResponse(String userId) {
    HashMap<String, Object> esDtoMap = new HashMap<>();
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.USER_ID, userId);
    esDtoMap.put(JsonKey.FILTERS, filters);
    List<String> fields = new ArrayList<>();
    fields.add(JsonKey.SKILLS);
    esDtoMap.put(JsonKey.FIELDS, fields);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    when(ElasticSearchUtil.complexSearch(
            ElasticSearchUtil.createSearchDTO(esDtoMap),
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName()))
        .thenReturn(createGetSkillResponse());
  }

  private void addSkill() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Response insertResponse = createCassandraInsertSuccessResponse();
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(insertResponse);
    Response getUserResponse = createGetUserSuccessResponse();
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getUserResponse);
    subject.tell(createAddSkillRequest(USER_ID, ENDORSED_USER_ID, skillsList), probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  private Response createCassandraInsertSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private Response createGetUserSuccessResponse() {
    Response response = new Response();
    Map<String, Object> userMap = new HashMap<>();
    userMap.put(JsonKey.ID, USER_ID);
    userMap.put(JsonKey.ROOT_ORG_ID, ROOT_ORG_ID);
    List<Map<String, Object>> result = new ArrayList<>();
    result.add(userMap);
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  private Response createGetUserFailureResponse() {
    Response response = new Response();
    List<Map<String, Object>> result = new ArrayList<>();
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  private Response createGetSkillsSuccessResponse() {
    Response response = new Response();
    Map<String, Object> userMap = new HashMap<>();
    List<Map<String, Object>> result = new ArrayList<>();
    result.add(userMap);
    response.put(JsonKey.RESPONSE, result);
    return response;
  }

  private Request createAddSkillRequest(
      String userId, String endorseUserId, List<String> skillsList) {

    Request actorMessage = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.ENDORSED_USER_ID, endorseUserId);
    actorMessage.put(JsonKey.SKILL_NAME, skillsList);
    actorMessage.setOperation(ActorOperations.ADD_SKILL.getValue());

    return actorMessage;
  }

  private Request createGetSkillRequest(String userId, String endorseUserId) {
    Request actorMessage = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.ENDORSED_USER_ID, endorseUserId);
    actorMessage.setOperation(ActorOperations.GET_SKILL.getValue());
    return actorMessage;
  }

  private Map<String, Object> createGetSkillResponse() {
    HashMap<String, Object> response = new HashMap<>();
    List<Map<String, Object>> content = new ArrayList<>();
    HashMap<String, Object> innerMap = new HashMap<>();
    content.add(innerMap);
    response.put(JsonKey.CONTENT, content);
    return response;
  }

  private void mockCasandraRequestForGetUserFailure() {
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(createGetUserFailureResponse());
  }

  private Request getSkillEndorsementRequest(
      String userId, String endorsedUserId, String skillName) {
    Request actorMessage = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.REQUESTED_BY, userId);
    actorMessage.setContext(innerMap);
    actorMessage.put(JsonKey.SKILL_NAME, skillName);
    actorMessage.put(JsonKey.ENDORSED_USER_ID, endorsedUserId);
    actorMessage.put(JsonKey.USER_ID, userId);
    actorMessage.setOperation(ActorOperations.ADD_USER_SKILL_ENDORSEMENT.getValue());
    return actorMessage;
  }
}
