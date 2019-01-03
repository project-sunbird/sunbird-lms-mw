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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
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
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.util.ContentStoreUtil;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.actors.UserManagementActor;
import org.sunbird.user.dao.impl.UserExternalIdentityDaoImpl;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.UserUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
  Util.class,
  RequestRouter.class,
  SystemSettingClientImpl.class,
  UserServiceImpl.class,
  UserUtil.class,
  InterServiceCommunicationFactory.class,
  DataCacheHandler.class,
  TelemetryUtil.class,
  SSOServiceFactory.class,
  ContentStoreUtil.class
})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class UserManagementActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(UserManagementActor.class);

  private static Map<String, Object> reqMap;

  private static final String userId = "testUserId";
  private static UserService userService;
  private static SystemSettingClientImpl systemSettingClient;
  private static UserExternalIdentityDaoImpl userExtDao;

  @Before
  public void beforeEachTest() {

    ActorRef actorRef = mock(ActorRef.class);
    PowerMockito.mockStatic(RequestRouter.class);
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);

    PowerMockito.mockStatic(ServiceFactory.class);
    CassandraOperationImpl cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());

    PowerMockito.mockStatic(InterServiceCommunicationFactory.class);
    InterServiceCommunication interServiceCommunication = mock(InterServiceCommunicationImpl.class);
    when(InterServiceCommunicationFactory.getInstance()).thenReturn(interServiceCommunication);
    when(interServiceCommunication.getResponse(
            Mockito.any(ActorRef.class), Mockito.any(Request.class)))
        .thenReturn(getEsResponse());

    PowerMockito.mockStatic(SystemSettingClientImpl.class);
    SystemSettingClientImpl systemSettingClient = mock(SystemSettingClientImpl.class);
    when(SystemSettingClientImpl.getInstance()).thenReturn(systemSettingClient);

    when(systemSettingClient.getSystemSettingByFieldAndKey(
            Mockito.any(ActorRef.class),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyObject()))
        .thenReturn(new HashMap<>());

    PowerMockito.mockStatic(UserServiceImpl.class);
    userService = mock(UserServiceImpl.class);
    when(UserServiceImpl.getInstance()).thenReturn(userService);
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("anyId");
    when(userService.getCustodianChannel(Mockito.anyMap(), Mockito.any(ActorRef.class)))
        .thenReturn("anyChannel");
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("rootOrgId");

    PowerMockito.mockStatic(ElasticSearchUtil.class);
    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getEsResponseMap());

    PowerMockito.mockStatic(Util.class);
    Util.getUserProfileConfig(Mockito.any(ActorRef.class));

    PowerMockito.mockStatic(UserUtil.class);
    UserUtil.setUserDefaultValue(Mockito.anyMap(), Mockito.anyString());

    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.TNC_ACCEPTED_ON, 12345678L);
    when(UserUtil.encryptUserData(Mockito.anyMap())).thenReturn(requestMap);

    reqMap = getMapObject();
    userExtDao = mock(UserExternalIdentityDaoImpl.class);
    PowerMockito.mockStatic(SSOServiceFactory.class);
    PowerMockito.mockStatic(TelemetryUtil.class);
    PowerMockito.mockStatic(DataCacheHandler.class);
    PowerMockito.mockStatic(ContentStoreUtil.class);
  }

  @Test
  public void testCreateUserSuccessWithUserCallerId() {

    boolean result =
        testScenario(
            getRequestedObj(
                true, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutUserCallerId() {

    boolean result =
        testScenario(
            getRequestedObj(
                false, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutUserCallerIdChannelAndRootOrgId() {

    boolean result =
        testScenario(
            getRequestedObj(false, false, true, reqMap, ActorOperations.CREATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidChannelAndOrgId() {

    reqMap.put(JsonKey.CHANNEL, "anyReqChannel");
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.parameterMismatch);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutVersion() {

    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidExternalIds() {

    reqMap.put(JsonKey.EXTERNAL_IDS, "anyExternalId");
    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.dataTypeError);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidRoles() {

    reqMap.put(JsonKey.ROLES, "anyRoles");
    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.dataTypeError);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidCountryCode() {

    reqMap.put(JsonKey.COUNTRY_CODE, "anyCode");
    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.invalidCountryCode);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidOrg() {

    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(null);
    boolean result =
        testScenario(
            getRequestedObj(
                false, false, false, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            ResponseCode.invalidOrgData);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccess() {

    boolean result =
        testScenario(
            getRequestedObj(true, true, true, getExternalIdMap(), ActorOperations.UPDATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccessWithoutUserCallerId() {

    boolean result =
        testScenario(
            getRequestedObj(false, true, true, getExternalIdMap(), ActorOperations.UPDATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserFrameworkSuccess() {
    mockForUpdateTest();
    Request reqObj = getRequest(null, null);
    Response res = doUpdateActorCallSuccess(reqObj);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidGradeLevel() {
    mockForUpdateTest();
    Request reqObj = getRequest("gradeLevel", "SomeWrongGrade");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidMedium() {
    mockForUpdateTest();
    Request reqObj = getRequest("medium", "glish");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidBoard() {
    mockForUpdateTest();
    Request reqObj = getRequest("board", "RBCS");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidFrameworkId() {
    mockForUpdateTest();
    Request reqObj = getRequest(JsonKey.ID, "invalidFrameworkId");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.errorNoFrameworkFound.getErrorCode()));
  }

  private Response doUpdateActorCallSuccess(Request reqObj) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockUserServiceForValidatingUserId(reqObj);
    mockuserExtDao(reqObj);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    return res;
  }

  private ProjectCommonException doUpdateActorCallFailure(Request reqObj) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockUserServiceForValidatingUserId(reqObj);
    mockuserExtDao(reqObj);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    return res;
  }

  @SuppressWarnings("unchecked")
  public void mockForUpdateTest() {
    mockInterserviceCommunication();
    mockUtilsForOrgDetails();
    mockDatacacheHandler();
    mockContentStoreUtil();
    mockElasticSearchUtil();
    mockKeycloakUpsertUser();
    mockCassandraforUpdateRecord();

    try {
      PowerMockito.whenNew(UserExternalIdentityDaoImpl.class)
          .withNoArguments()
          .thenReturn(userExtDao);

      PowerMockito.doNothing()
          .when(
              TelemetryUtil.class,
              "telemetryProcessingCall",
              Mockito.anyMap(),
              Mockito.anyMap(),
              Mockito.anyList());
    } catch (Exception e) {
      ProjectLogger.log("Error while mocking Telemetry Util");
    }
  }

  private Request getRequest(String key, String value) {
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, userId);
    Map<String, Object> frameworkMap = getFrameworkDetails(key, value);

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

  private Map<String, Object> getFrameworkDetails(String key, String value) {
    Map<String, Object> frameworkMap = new HashMap<>();
    List<String> medium = new ArrayList<>();
    medium.add("English");
    List<String> gradeLevel = new ArrayList<>();
    gradeLevel.add("Grade 3");
    List<String> board = new ArrayList<>();
    board.add("NCERT");
    frameworkMap.put(JsonKey.ID, "NCF");
    frameworkMap.put("gradeLevel", gradeLevel);
    frameworkMap.put("board", board);
    frameworkMap.put("medium", medium);
    if (key != null) {
      List<String> wrongValue = new ArrayList<>();
      wrongValue.add(value);
      frameworkMap.put(key, wrongValue);
    }
    return frameworkMap;
  }

  private void mockKeycloakUpsertUser() {
    SSOManager ssoManager = mock(KeyCloakServiceImpl.class);
    when(SSOServiceFactory.getInstance()).thenReturn(ssoManager);
    when(ssoManager.updateUser(Mockito.anyMap())).thenReturn("SUCCESS");
  }

  private void mockElasticSearchUtil() {
    Map<String, Object> userMap = new HashMap<>();
    userMap.put("abc", "abc");
    when(ElasticSearchUtil.getDataByIdentifier(
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

  private void mockCassandraforUpdateRecord() {
    CassandraOperation cassandraOperation;
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Response res = new Response();
    res.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(res);
  }

  private void mockInterserviceCommunication() {
    InterServiceCommunication interServiceCommunication = mock(InterServiceCommunicationImpl.class);
    when(InterServiceCommunicationFactory.getInstance()).thenReturn(interServiceCommunication);
    Response res = new Response();
    Map<String, Object> response = new HashMap<>();
    response.put(JsonKey.ERRORS, null);
    res.getResult().put(JsonKey.RESPONSE, response);
    when(interServiceCommunication.getResponse(Mockito.any(), Mockito.any())).thenReturn(res);
  }

  private void mockUtilsForOrgDetails() {
    Map<String, Object> rootOrgMap = new HashMap<>();
    String hashTagId = "someHashTagId";
    rootOrgMap.put(JsonKey.HASHTAGID, hashTagId);
    when(Util.getOrgDetails(Mockito.anyString())).thenReturn(rootOrgMap);
  }

  private void mockContentStoreUtil() {
    Map<String, Object> contentMap = new HashMap<>();
    contentMap.put(JsonKey.RESPONSE, null);
    when(ContentStoreUtil.readFramework("wrongId")).thenReturn(contentMap);
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
    List<Map<String, String>> list2 = getListForCategoryMap("English");
    frameworkCategoriesMap.put("medium", list2);

    List<Map<String, String>> list = getListForCategoryMap("Grade 3");
    frameworkCategoriesMap.put("gradeLevel", list);

    List<Map<String, String>> list3 = getListForCategoryMap("NCERT");
    frameworkCategoriesMap.put("board", list3);

    Map<String, Map<String, List<Map<String, String>>>> x = new HashMap<>();
    x.put("NCF", frameworkCategoriesMap);
    DataCacheHandler.updateFrameworkCategoriesMap("NCF", frameworkCategoriesMap);
    when(DataCacheHandler.getFrameworkCategoriesMap()).thenReturn(x);
    Map<String, List<String>> map1 = new HashMap<>();
    List<String> list1 = Arrays.asList("NCF");
    map1.put("someHashTagId", list1);
    when(DataCacheHandler.getHashtagIdFrameworkIdMap()).thenReturn(map1);
  }

  private List<Map<String, String>> getListForCategoryMap(String value) {
    Map<String, String> map2 = new HashMap<>();
    map2.put(JsonKey.NAME, value);
    List<Map<String, String>> list2 = new ArrayList<>();
    list2.add(map2);
    return list2;
  }

  private Map<String, Object> getAdditionalMapData(Map<String, Object> reqMap) {
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    reqMap.put(JsonKey.CHANNEL, "anyChannel");
    return reqMap;
  }

  private boolean testScenario(Request reqObj, ResponseCode errorCode) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(reqObj, probe.getRef());

    if (errorCode == null) {
      Response res = probe.expectMsgClass(duration("10 second"), Response.class);
      return null != res && res.getResponseCode() == ResponseCode.OK;
    } else {
      ProjectCommonException res =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      return res.getCode().equals(errorCode.getErrorCode())
          || res.getResponseCode() == errorCode.getResponseCode();
    }
  }

  private Map<String, Object> getExternalIdMap() {

    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.EXTERNAL_ID, "externalId");
    reqMap.put(JsonKey.EXTERNAL_ID_PROVIDER, "externalIdProvider");
    reqMap.put(JsonKey.EXTERNAL_ID_TYPE, "externalIdType");
    return reqMap;
  }

  private HashMap<String, Object> getMapObject() {

    HashMap<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.FIRST_NAME, "firstname");
    reqMap.put(JsonKey.USERNAME, "userName");
    reqMap.put(JsonKey.EMAIL, "email@email.com");
    reqMap.put(JsonKey.LANGUAGE, new ArrayList<>());
    reqMap.put(JsonKey.DOB, "1992-12-31");
    reqMap.put(JsonKey.EMAIL_VERIFIED, true);
    reqMap.put(JsonKey.PHONE_VERIFIED, true);
    reqMap.put(JsonKey.ADDRESS, new ArrayList<>());
    return reqMap;
  }

  private Request getRequestedObj(
      boolean isCallerIdReq,
      boolean isRootOrgIdReq,
      boolean isVersionReq,
      Map<String, Object> reqMap,
      ActorOperations actorOperation) {

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    if (isCallerIdReq) innerMap.put(JsonKey.CALLER_ID, "anyCallerId");
    if (isVersionReq) innerMap.put(JsonKey.VERSION, "v2");
    if (isRootOrgIdReq) innerMap.put(JsonKey.ROOT_ORG_ID, "MY_ROOT_ORG_ID");
    innerMap.put(JsonKey.REQUESTED_BY, "requestedBy");
    reqObj.setRequest(reqMap);
    reqObj.setContext(innerMap);
    reqObj.setOperation(actorOperation.getValue());
    return reqObj;
  }

  private static Response getEsResponse() {

    Response response = new Response();
    Map<String, Object> map = new HashMap<>();
    map.put("anyString", new Object());
    response.put(JsonKey.RESPONSE, map);
    return response;
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private static Map<String, Object> getEsResponseMap() {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.IS_ROOT_ORG, true);
    map.put(JsonKey.ID, "rootOrgId");
    map.put(JsonKey.CHANNEL, "anyChannel");
    return map;
  }
}
