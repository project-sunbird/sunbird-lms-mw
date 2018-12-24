package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actor.service.BaseMWService;
import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.actorutil.impl.InterServiceCommunicationImpl;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.actors.UserManagementActor;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.UserUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
  Util.class,
  RequestRouter.class,
  BaseMWService.class,
  SystemSettingClientImpl.class,
  SunbirdMWService.class,
  UserServiceImpl.class,
  Util.class,
  UserUtil.class,
  InterServiceCommunicationFactory.class,
  TelemetryUtil.class
})
@PowerMockIgnore({"javax.management.*"})
public class UserManagementActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private final Props props = Props.create(UserManagementActor.class);
  private static CassandraOperationImpl cassandraOperation = mock(CassandraOperationImpl.class);

  @Before
  public void beforeEachTest() {

    PowerMockito.mockStatic(RequestRouter.class);
    PowerMockito.mockStatic(SystemSettingClientImpl.class);
    PowerMockito.mockStatic(UserServiceImpl.class);

    PowerMockito.mockStatic(ServiceFactory.class);
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

    SystemSettingClientImpl systemSettingClient = mock(SystemSettingClientImpl.class);
    when(SystemSettingClientImpl.getInstance()).thenReturn(systemSettingClient);

    when(systemSettingClient.getSystemSettingByFieldAndKey(
            Mockito.any(ActorRef.class),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyObject()))
        .thenReturn(new HashMap<>());

    UserServiceImpl userService = mock(UserServiceImpl.class);
    when(UserServiceImpl.getInstance()).thenReturn(userService);
    ActorRef actorRef = mock(ActorRef.class);

    PowerMockito.mockStatic(SunbirdMWService.class);
    SunbirdMWService.tellToBGRouter(Mockito.any(Request.class), Mockito.any(ActorRef.class));
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("anyId");
    when(userService.getCustodianChannel(Mockito.anyMap(), Mockito.any(ActorRef.class)))
        .thenReturn("anyChannel");
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("rootOrgId");

    HashMap<String, Object> map = new HashMap<>();
    map.put(JsonKey.IS_ROOT_ORG, true);
    map.put(JsonKey.ID, "rootOrgId");
    map.put(JsonKey.CHANNEL, "anyChannel");
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(map);
    PowerMockito.mockStatic(Util.class);
    Util.getUserProfileConfig(Mockito.any(ActorRef.class));

    PowerMockito.mockStatic(UserUtil.class);
    UserUtil.setUserDefaultValue(Mockito.anyMap(), Mockito.anyString());

    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.TNC_ACCEPTED_ON, 12345678L);
    when(UserUtil.encryptUserData(Mockito.anyMap())).thenReturn(requestMap);
  }

  private static Response getEsResponse() {

    Response response = new Response();
    Map<String, Object> map = new HashMap<>();
    map.put("anyString", new Object());
    response.put(JsonKey.RESPONSE, map);
    return response;
  }

  @Test
  public void testCreateuserSuccessWithCallerId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    reqMap.put(JsonKey.CHANNEL, "anyChannel");
    subject.tell(
        getRequestedObj(true, true, true, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCreateuserSuccessWithoutCallerId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    reqMap.put(JsonKey.CHANNEL, "anyChannel");
    subject.tell(
        getRequestedObj(false, true, true, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCreateuserSuccessWithoutCallerIdAndChannelANdRootOrgId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    subject.tell(
        getRequestedObj(false, false, true, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testCreateuserSuccessWithoutVersion() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    subject.tell(
        getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
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

  @Test
  public void testCreateuserFailureWithInvalidExternalIds() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    reqMap.put(JsonKey.EXTERNAL_IDS, "anyExternalId");
    subject.tell(
        getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
    Assert.assertTrue(res.getCode() == ResponseCode.dataTypeError.getErrorCode());
  }

  @Test
  public void testCreateuserFailureWithInvalidRoles() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    reqMap.put(JsonKey.ROLES, "anyRoles");
    subject.tell(
        getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
    Assert.assertTrue(res.getCode() == ResponseCode.dataTypeError.getErrorCode());
  }

  @Test
  public void testCreateuserFailureWithInvalidCountryCode() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    reqMap.put(JsonKey.COUNTRY_CODE, "anyCode");
    subject.tell(
        getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
    Assert.assertTrue(res.getCode() == ResponseCode.invalidCountryCode.getErrorCode());
  }

  @Test
  public void testCreateuserFailureWithInvalidChannelAndOrganisationId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    reqMap.put(JsonKey.CHANNEL, "anyReqChannel");
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    subject.tell(
        getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
    Assert.assertTrue(res.getCode() == ResponseCode.parameterMismatch.getErrorCode());
  }

  @Test
  public void testCreateuserFailureWithEmptyOrganisationResponse() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = getMapObject();
    reqMap.put(JsonKey.CHANNEL, "anyReqChannel");
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(null);
    subject.tell(
        getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER.getValue()),
        probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
    Assert.assertTrue(res.getCode() == ResponseCode.invalidOrgData.getErrorCode());
  }

  @Test
  public void testUpdateUserSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.EXTERNAL_ID, "externalId");
    reqMap.put(JsonKey.EXTERNAL_ID_PROVIDER, "externalIdProvider");
    reqMap.put(JsonKey.EXTERNAL_ID_TYPE, "externalIdType");
    subject.tell(
        getRequestedObj(true, true, true, reqMap, ActorOperations.UPDATE_USER.getValue()),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateUserSuccessWithoutCallerId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    HashMap<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.EXTERNAL_ID, "externalId");
    reqMap.put(JsonKey.EXTERNAL_ID_PROVIDER, "externalIdProvider");
    reqMap.put(JsonKey.EXTERNAL_ID_TYPE, "externalIdType");
    subject.tell(
        getRequestedObj(false, true, true, reqMap, ActorOperations.UPDATE_USER.getValue()),
        probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  private Request getRequestedObj(
      boolean isCallerIdReq,
      boolean isRootOrgIdReq,
      boolean isVersionReq,
      HashMap<String, Object> reqMap,
      String operation) {

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    if (isCallerIdReq) innerMap.put(JsonKey.CALLER_ID, "anyCallerId");
    if (isVersionReq) innerMap.put(JsonKey.VERSION, "v2");
    if (isRootOrgIdReq) innerMap.put(JsonKey.ROOT_ORG_ID, "MY_ROOT_ORG_ID");
    innerMap.put(JsonKey.REQUESTED_BY, "requestedBy");
    reqObj.setRequest(reqMap);
    reqObj.setContext(innerMap);
    reqObj.setOperation(operation);
    return reqObj;
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }
}
