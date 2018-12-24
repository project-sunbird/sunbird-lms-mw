package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.actorutil.impl.InterServiceCommunicationImpl;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.UserRequestValidator;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.util.ContentStoreUtil;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.EkStepRequestUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.actors.UserManagementActor;
import org.sunbird.user.dao.UserExternalIdentityDao;
import org.sunbird.user.dao.impl.UserExternalIdentityDaoImpl;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.UserUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  EkStepRequestUtil.class,
  Util.class,
  ServiceFactory.class,
  ElasticSearchUtil.class,
  SunbirdMWService.class,
  UserRequestValidator.class,
  UserUtil.class,
  DataCacheHandler.class,
  ProjectUtil.class,
  CassandraOperation.class,
  InterServiceCommunication.class,
  TelemetryUtil.class,
  Util.class,
  InterServiceCommunicationFactory.class,
  InterServiceCommunicationImpl.class,
  SystemSettingClientImpl.class,
  RequestRouter.class,
  UserServiceImpl.class,
  UserService.class,
  UserExternalIdentityDao.class,
  UserExternalIdentityDaoImpl.class,
  SSOServiceFactory.class,
  SSOManager.class,
  KeyCloakServiceImpl.class,
  HttpUtil.class,
  ContentStoreUtil.class
})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class UserManagementActorTestExtended {
  private static ActorSystem system;
  private static final Props props = Props.create(UserManagementActor.class);
  private static String userId = "testUserId";
  private static CassandraOperation cassandraOperation;
  private static UserService userService;
  private static InterServiceCommunication interServiceCommunication;
  private static SystemSettingClientImpl systemSettingClient;
  private static ActorRef actorRef;
  private static UserExternalIdentityDaoImpl userExtDao;
  private static SSOManager ssoManager;

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    PowerMockito.mockStatic(EkStepRequestUtil.class);
  }

  // validate all things in framework related functions
  @Before
  public void beforeEachTest() throws Exception {
    PowerMockito.mockStatic(Util.class);
    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(InterServiceCommunicationFactory.class);
    PowerMockito.mockStatic(UserServiceImpl.class);
    PowerMockito.mockStatic(UserExternalIdentityDao.class);
    PowerMockito.mockStatic(UserExternalIdentityDaoImpl.class);
    PowerMockito.mockStatic(UserRequestValidator.class);
    PowerMockito.mockStatic(SSOServiceFactory.class);
    PowerMockito.mockStatic(SunbirdMWService.class);
    PowerMockito.mockStatic(TelemetryUtil.class);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.mockStatic(DataCacheHandler.class);
    PowerMockito.mockStatic(ContentStoreUtil.class);

    userExtDao = mock(UserExternalIdentityDaoImpl.class);
    PowerMockito.whenNew(UserExternalIdentityDaoImpl.class)
        .withNoArguments()
        .thenReturn(userExtDao);
    ssoManager = mock(KeyCloakServiceImpl.class);
    when(SSOServiceFactory.getInstance()).thenReturn(ssoManager);
    PowerMockito.whenNew(UserExternalIdentityDaoImpl.class)
        .withNoArguments()
        .thenReturn(userExtDao);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Mockito.reset(cassandraOperation);
    PowerMockito.mockStatic(SystemSettingClientImpl.class);
    systemSettingClient = mock(SystemSettingClientImpl.class);
    when(SystemSettingClientImpl.getInstance()).thenReturn(systemSettingClient);
    userService = mock(UserServiceImpl.class);

    actorRef = mock(ActorRef.class);
    PowerMockito.mockStatic(RequestRouter.class);
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);
    interServiceCommunication = mock(InterServiceCommunicationImpl.class);
    when(InterServiceCommunicationFactory.getInstance()).thenReturn(interServiceCommunication);

    mockInterserviceCommunication();

    PowerMockito.mockStatic(HttpUtil.class);
    PowerMockito.doNothing()
        .when(
            TelemetryUtil.class,
            "telemetryProcessingCall",
            Mockito.anyMap(),
            Mockito.anyMap(),
            Mockito.anyList());

    mockUtilsForOrgDetails();
    mockDatacacheHandler();
    mockContentStoreUtil();
  }

  @Ignore
  @Test
  public void testUpdateUserFrameworkSuccess() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = getRequest(true, false, false, false, false);
    mockUserServiceForValidatingUserId(reqObj);
    mockuserExtDao(reqObj);
    mockElasticSearchUtil();
    mockKeycloakUpsertUser();
    mockCassandraforUpdateRecord();
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidGradeLevel() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = getRequest(false, false, true, false, false);
    mockUserServiceForValidatingUserId(reqObj);
    mockuserExtDao(reqObj);
    mockElasticSearchUtil();
    mockKeycloakUpsertUser();
    mockCassandraforUpdateRecord();
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    System.out.print(res.toString());
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidMedium() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = getRequest(false, false, false, true, false);
    mockUserServiceForValidatingUserId(reqObj);
    mockuserExtDao(reqObj);
    mockElasticSearchUtil();
    mockKeycloakUpsertUser();
    mockCassandraforUpdateRecord();
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidBoard() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = getRequest(false, false, false, false, true);
    mockUserServiceForValidatingUserId(reqObj);
    mockuserExtDao(reqObj);
    mockElasticSearchUtil();
    mockKeycloakUpsertUser();
    mockCassandraforUpdateRecord();
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidFrameworkId() throws Exception {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = getRequest(false, true, false, false, false);
    mockUserServiceForValidatingUserId(reqObj);
    mockuserExtDao(reqObj);
    mockElasticSearchUtil();
    mockKeycloakUpsertUser();
    mockCassandraforUpdateRecord();
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(res.getCode().equals(ResponseCode.errorNoFrameworkFound.getErrorCode()));
  }

  private Request getRequest(
      boolean success, boolean id_, boolean grade, boolean medium, boolean board) {
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, userId);
    Map<String, Object> frameworkMap = getFrameworkDetails(success, id_, grade, medium, board);

    innerMap.put(JsonKey.FRAMEWORK, frameworkMap);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    request.put(JsonKey.USER_ID, userId);
    request.put(JsonKey.FRAMEWORK, frameworkMap);
    reqObj.setRequest(request);
    Map<String, Object> context = new HashMap<>();
    context.put(JsonKey.REQUESTED_BY, "someValue");
    context.put(JsonKey.USER_ID, userId);
    reqObj.setContext(context);
    return reqObj;
  }

  private Map<String, Object> getFrameworkDetails(
      boolean success, boolean id_, boolean grade_, boolean medium_, boolean board_) {
    Map<String, Object> frameworkMap = new HashMap<>();
    List<String> medium = new ArrayList<>();
    medium.add("English");
    List<String> gradeLevel = new ArrayList<>();
    gradeLevel.add("Grade 3");
    List<String> board = new ArrayList<>();
    board.add("NCERT");
    if (success) {
      frameworkMap.put(JsonKey.ID, "NCF");
      frameworkMap.put("medium", medium);
      frameworkMap.put("gradeLevel", gradeLevel);
      frameworkMap.put("board", board);
    } else if (id_) {
      frameworkMap.put(JsonKey.ID, "wrongId");
      frameworkMap.put("medium", medium);
      frameworkMap.put("gradeLevel", gradeLevel);
      frameworkMap.put("board", board);
    } else if (grade_) {
      frameworkMap.put(JsonKey.ID, "NCF");
      List<String> gradeLevel2 = new ArrayList<>();
      gradeLevel2.add("SomeWrongGrade");
      frameworkMap.put("medium", medium);
      frameworkMap.put("gradeLevel", gradeLevel2);
      frameworkMap.put("board", board);
    } else if (medium_) {
      frameworkMap.put(JsonKey.ID, "NCF");
      List<String> medium2 = new ArrayList<>();
      medium2.add("glish");
      frameworkMap.put("medium", medium2);
      frameworkMap.put("gradeLevel", gradeLevel);
      frameworkMap.put("board", board);
    } else if (board_) {
      frameworkMap.put(JsonKey.ID, "NCF");
      List<String> board2 = new ArrayList<>();
      board2.add("RRRCERT");
      frameworkMap.put("board", board2);
      frameworkMap.put("gradeLevel", gradeLevel);
      frameworkMap.put("medium", medium);
    }

    return frameworkMap;
  }

  private void mockKeycloakUpsertUser() {
    when(ssoManager.updateUser(Mockito.anyMap())).thenReturn("SUCCESS");
  }

  private void mockElasticSearchUtil() {
    Map<String, Object> userMap = new HashMap<>();
    userMap.put("abc", "abc");
    Mockito.when(
            ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.user.getTypeName(),
                userId))
        .thenReturn(userMap);
  }

  private void mockUserServiceForValidatingUserId(Request req) {
    Mockito.doNothing().when(userService).validateUserId(req);
  }

  private void mockuserExtDao(Request req) {
    when(userExtDao.getUserId(req)).thenReturn(userId);
  }

  private static void mockSystemSettingClientForGetSystemSettingByFieldAndKey() {
    Map<String, List<String>> frameworkFieldsConfigMap = new HashMap<>();
    List<String> frameworkFieldConfig =
        Arrays.asList("id", "medium", "gradeLevel", "board", "subject");
    List<String> frameworkFieldConfigMan = Arrays.asList("id", "medium", "gradeLevel", "board");
    frameworkFieldsConfigMap.put(JsonKey.FIELDS, frameworkFieldConfig);
    frameworkFieldsConfigMap.put(JsonKey.MANDATORY_FIELDS, frameworkFieldConfigMan);
    DataCacheHandler.setFrameworkFieldsConfig(frameworkFieldsConfigMap);
    when(systemSettingClient.getSystemSettingByFieldAndKey(
            Mockito.any(ActorRef.class),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyObject()))
        .thenReturn(frameworkFieldsConfigMap);
  }

  private void mockCassandraforUpdateRecord() {
    // create user map
    Response res = new Response();
    res.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(res);
  }

  private void mockInterserviceCommunication() {
    Response res = new Response();
    Map<String, Object> response = new HashMap<>();
    response.put(JsonKey.ERRORS, null);
    res.getResult().put(JsonKey.RESPONSE, response);
    PowerMockito.when(interServiceCommunication.getResponse(Mockito.any(), Mockito.any()))
        .thenReturn(res);
  }

  private void mockUtilsForOrgDetails() {
    Map<String, Object> rootOrgMap = new HashMap<>();
    String hashTagId = "someHashTagId";
    rootOrgMap.put(JsonKey.HASHTAGID, hashTagId);
    Mockito.when(Util.getOrgDetails(Mockito.anyString())).thenReturn(rootOrgMap);
  }

  private void mockContentStoreUtil() {
    Map<String, Object> contentMap = new HashMap<>();
    contentMap.put(JsonKey.RESPONSE, null);
    PowerMockito.when(ContentStoreUtil.readFramework("wrongId")).thenReturn(contentMap);
  }

  private void mockDatacacheHandler() {
    Map<String, List<String>> frameworkFieldsConfigMap = new HashMap<>();
    List<String> frameworkFieldConfig =
        Arrays.asList("id", "medium", "gradeLevel", "board", "subject");
    List<String> frameworkFieldConfigMan = Arrays.asList("id", "medium", "gradeLevel", "board");
    frameworkFieldsConfigMap.put(JsonKey.FIELDS, frameworkFieldConfig);
    frameworkFieldsConfigMap.put(JsonKey.MANDATORY_FIELDS, frameworkFieldConfigMan);
    DataCacheHandler.setFrameworkFieldsConfig(frameworkFieldsConfigMap);
    Mockito.when(DataCacheHandler.getFrameworkFieldsConfig()).thenReturn(frameworkFieldsConfigMap);
    Map<String, List<Map<String, String>>> frameworkCategoriesMap = new HashMap<>();

    Map<String, String> map2 = new HashMap<>();
    map2.put(JsonKey.NAME, "English");
    List<Map<String, String>> list2 = new ArrayList<>();
    list2.add(map2);
    frameworkCategoriesMap.put("medium", list2);

    List<Map<String, String>> list;
    Map<String, String> map;
    map = new HashMap<>();
    map.put(JsonKey.NAME, "Grade 3");
    list = new ArrayList<>();
    list.add(map);
    frameworkCategoriesMap.put("gradeLevel", list);

    Map<String, String> map3 = new HashMap<>();
    map3.put(JsonKey.NAME, "NCERT");
    List<Map<String, String>> list3 = new ArrayList<>();
    list3.add(map3);
    frameworkCategoriesMap.put("board", list3);
    Map<String, Map<String, List<Map<String, String>>>> x = new HashMap<>();
    x.put("NCF", frameworkCategoriesMap);
    DataCacheHandler.updateFrameworkCategoriesMap("NCF", frameworkCategoriesMap);
    Mockito.when(DataCacheHandler.getFrameworkCategoriesMap()).thenReturn(x);
    Map<String, List<String>> map1 = new HashMap<>();
    List<String> list1 = Arrays.asList("NCF");
    map1.put("someHashTagId", list1);
    Mockito.when(DataCacheHandler.getHashtagIdFrameworkIdMap()).thenReturn(map1);
  }
}
