package org.sunbird.user;

import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.HashMap;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
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
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.datasecurity.impl.DefaultEncryptionServivceImpl;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.actors.UserManagementActor;

/** @author Amit Kumar */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  SSOServiceFactory.class,
  ElasticSearchUtil.class,
  CassandraOperationImpl.class,
  KeyCloakServiceImpl.class,
  Util.class,
  org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class,
  TelemetryUtil.class,
  DataCacheHandler.class
})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class UserActorTest {

  private static String externalId = String.valueOf(System.currentTimeMillis());
  private static String provider = String.valueOf(System.currentTimeMillis() + 10);
  private static String userName = "userName" + externalId;
  private static String email = externalId + "@gmail.com";
  private static Map<String, Object> user = new HashMap<>();
  private static Map<String, String> responseMap = new HashMap<>();
  private static ActorSystem system;
  private static Props props;
  private static SSOManager ssoManager = null;
  private static CassandraOperation cassandraOperation = null;
  private static Response response = null;
  private static String rootOrgId = "dummyOrgId";
  private static EncryptionService encryptionService = null;

  @BeforeClass
  public static void setUp() throws Exception {
    system = ActorSystem.create("system");
    props = Props.create(UserManagementActor.class);

    response = new Response();
    responseMap.put(JsonKey.USER_ID, "12345");
    response.getResult().putAll(responseMap);

    user.put(JsonKey.FIRST_NAME, "first_name");
    user.put(JsonKey.USERNAME, userName);
    user.put(JsonKey.EXTERNAL_ID, externalId);
    user.put(JsonKey.PROVIDER, provider);
    user.put(JsonKey.EMAIL, email);

    mockClasses();
  }

  private static void mockClasses() throws Exception {
    PowerMockito.mockStatic(SSOServiceFactory.class);
    ssoManager = PowerMockito.mock(KeyCloakServiceImpl.class);
    PowerMockito.when(SSOServiceFactory.getInstance()).thenReturn(ssoManager);
    PowerMockito.when(ssoManager.createUser(user)).thenReturn(responseMap);

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = PowerMockito.mock(CassandraOperationImpl.class);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    PowerMockito.when(cassandraOperation.insertRecord(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(response);
    PowerMockito.when(cassandraOperation.upsertRecord(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(response);
    PowerMockito.when(cassandraOperation.updateRecord(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(response);

    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.mockStatic(Util.class);
    PowerMockito.when(Util.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn(rootOrgId);
    PowerMockito.mockStatic(org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class);
    encryptionService = PowerMockito.mock(DefaultEncryptionServivceImpl.class);
    PowerMockito.when(
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                .getEncryptionServiceInstance(null))
        .thenReturn(encryptionService);
    PowerMockito.when(encryptionService.encryptData(user)).thenReturn(user);
    PowerMockito.when(encryptionService.encryptData(Mockito.anyString())).thenReturn(rootOrgId);
    PowerMockito.mockStatic(TelemetryUtil.class);
    PowerMockito.mockStatic(DataCacheHandler.class);
    PowerMockito.when(
            TelemetryUtil.generateTargetObject(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(user);
    Map<String, String> configSetting = new HashMap<>();
    configSetting.put(JsonKey.EMAIL_UNIQUE, String.valueOf(false));
    configSetting.put(JsonKey.PHONE_UNIQUE, String.valueOf(false));
    PowerMockito.when(DataCacheHandler.getConfigSettings()).thenReturn(configSetting);
    PowerMockito.when(Util.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("rootOrgId");
  }

  /**
   * Unit test to create user.
   *
   * @throws Exception
   */
  // @Test
  public void testCreateUser() throws Exception {
    mockClasses();
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, user);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Exception ex = probe.expectMsgClass(NullPointerException.class);
    assertTrue(null != ex);
  }

  @Ignore
  public void testCreateUserWithInvalidChannel() throws Exception {
    mockClasses();
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    user.put(JsonKey.CHANNEL, "invalidChannelValue@123");
    request.put(JsonKey.USER, user);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException ex = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != ex);
  }

  @Ignore
  public void testInvalidParamRootOrgInRequest() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    Map<String, Object> tempUser = new HashMap<String, Object>(user);
    tempUser.put(JsonKey.ROOT_ORG_ID, "rootOrgId");
    request.put(JsonKey.USER, tempUser);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException ex = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != ex);
    assertTrue(ex.getMessage().equalsIgnoreCase("Invalid parameter rootOrgId in request."));
  }

  @Ignore
  public void testInvalidParamRegOrgIdInRequest() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    Map<String, Object> tempUser = new HashMap<String, Object>(user);
    tempUser.put(JsonKey.REGISTERED_ORG_ID, "regOrgId");
    request.put(JsonKey.USER, tempUser);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException ex = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != ex);
    assertTrue(ex.getMessage().equalsIgnoreCase("Invalid parameter regOrgId in request."));
  }

  @Ignore
  public void testMissingParamProviderInRequest() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    Map<String, Object> tempUser = new HashMap<String, Object>(user);
    tempUser.remove(JsonKey.PROVIDER);
    request.put(JsonKey.USER, tempUser);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException ex = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != ex);
    assertTrue(
        ex.getMessage()
            .equalsIgnoreCase("Missing parameter provider which is dependent on externalId."));
  }

  @Ignore
  public void testMissingParamExternalIdInRequest() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    Map<String, Object> tempUser = new HashMap<String, Object>(user);
    tempUser.remove(JsonKey.EXTERNAL_ID);
    request.put(JsonKey.USER, tempUser);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException ex = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != ex);
    assertTrue(
        ex.getMessage()
            .equalsIgnoreCase("Missing parameter externalId which is dependent on provider."));
  }
}
