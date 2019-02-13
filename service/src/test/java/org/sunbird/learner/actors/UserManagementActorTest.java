package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actorutil.location.impl.LocationClientImpl;
import org.sunbird.common.BaseActorTest;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;
import org.sunbird.user.actors.UserManagementActor;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.UserUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  Util.class,
  UserServiceImpl.class,
  UserUtil.class,
  LocationClientImpl.class,
})
@PowerMockIgnore({"javax.management.*"})
public class UserManagementActorTest extends BaseActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(UserManagementActor.class);
  private static Map<String, Object> reqMap;
  private static final HashMap esResponseMap = getEsResponseMap();

  @Before
  public void beforeEachTest() throws Exception {

    PowerMockito.mockStatic(LocationClientImpl.class);
    LocationClientImpl locationClient = mock(LocationClientImpl.class);
    PowerMockito.whenNew(LocationClientImpl.class).withNoArguments().thenReturn(locationClient);
    when(locationClient.getRelatedLocationIds(Mockito.any(ActorRef.class), Mockito.anyList()))
        .thenReturn(getLocationList());

    PowerMockito.mockStatic(UserServiceImpl.class);
    UserServiceImpl userService = mock(UserServiceImpl.class);
    when(UserServiceImpl.getInstance()).thenReturn(userService);
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("anyId");
    when(userService.getCustodianChannel(Mockito.anyMap(), Mockito.any(ActorRef.class)))
        .thenReturn("anyChannel");
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("rootOrgId");

    PowerMockito.mockStatic(Util.class);
    Util.getUserProfileConfig(Mockito.any(ActorRef.class));

    PowerMockito.mockStatic(UserUtil.class);
    UserUtil.setUserDefaultValue(Mockito.anyMap(), Mockito.anyString());
    reqMap = getMapObject();
  }

  @Test
  public void testCreateUserSuccessWithUserCallerId() {

    boolean result =
        testScenario(
            getRequest(true, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutUserCallerId() {

    boolean result =
        testScenario(
            getRequest(
                false, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutUserCallerIdChannelAndRootOrgId() {

    boolean result =
        testScenario(
            getRequest(false, false, true, reqMap, ActorOperations.CREATE_USER),
            null,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidChannelAndOrgId() {

    reqMap.put(JsonKey.CHANNEL, "anyReqChannel");
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.parameterMismatch,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidLocationCodes() {

    reqMap.put(JsonKey.LOCATION_CODES, Arrays.asList("invalidLocationCode"));
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.invalidParameterValue,
            false,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutVersion() {

    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            null,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithLocationCodes() {

    reqMap.put(JsonKey.LOCATION_CODES, Arrays.asList("locationCode"));
    boolean result =
        testScenario(
            getRequest(true, true, true, reqMap, ActorOperations.CREATE_USER),
            null,
            true,
            true,
            true);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidExternalIds() {

    reqMap.put(JsonKey.EXTERNAL_IDS, "anyExternalId");
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.dataTypeError,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidRoles() {

    reqMap.put(JsonKey.ROLES, "anyRoles");
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.dataTypeError,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidCountryCode() {

    reqMap.put(JsonKey.COUNTRY_CODE, "anyCode");
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.invalidCountryCode,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidOrg() {

    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(null);
    boolean result =
        testScenario(
            getRequest(
                false, false, false, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            ResponseCode.invalidOrgData,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserFailureWithLocationCodes() {
    boolean result =
        testScenario(
            getRequest(
                true, true, true, getUpdateRequestWithLocationCodes(), ActorOperations.UPDATE_USER),
            ResponseCode.invalidParameterValue,
            false,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccess() {

    boolean result =
        testScenario(
            getRequest(true, true, true, getExternalIdMap(), ActorOperations.UPDATE_USER),
            null,
            true,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccessWithLocationCodes() {

    boolean result =
        testScenario(
            getRequest(
                true, true, true, getUpdateRequestWithLocationCodes(), ActorOperations.UPDATE_USER),
            null,
            true,
            true,
            true);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccessWithoutUserCallerId() {

    boolean result =
        testScenario(
            getRequest(false, true, true, getExternalIdMap(), ActorOperations.UPDATE_USER),
            null,
            true,
            false,
            false);
    assertTrue(result);
  }

  private Map<String, Object> getAdditionalMapData(Map<String, Object> reqMap) {
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    reqMap.put(JsonKey.CHANNEL, "anyChannel");
    return reqMap;
  }

  private boolean testScenario(
      Request reqObj,
      ResponseCode errorCode,
      boolean isFirstPresent,
      boolean isSecondPresent,
      boolean isLocation) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    mockInterserviceCommunication(isLocation, isFirstPresent, isSecondPresent);
    subject.tell(reqObj, probe.getRef());

    if (errorCode == null) {
      Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
      return null != res && res.getResponseCode() == ResponseCode.OK;
    } else {
      ProjectCommonException res =
          probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
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

  private Request getRequest(
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

  private Map<String, Object> getUpdateRequestWithLocationCodes() {
    Map<String, Object> reqObj = new HashMap();
    reqObj.put(JsonKey.LOCATION_CODES, Arrays.asList("locationCode"));
    reqObj.put(JsonKey.USER_ID, "userId");
    return reqObj;
  }

  private static HashMap getEsResponseMap() {
    HashMap<String, Object> map = new HashMap<>();
    map.put(JsonKey.IS_ROOT_ORG, true);
    map.put(JsonKey.ID, "rootOrgId");
    map.put(JsonKey.CHANNEL, "anyChannel");
    return map;
  }

  private List getLocationList() {

    List<String> lst = new ArrayList<>();
    lst.add("anyCode");
    return lst;
  }

  @Override
  protected Map<String, Object> getDataByIdentifierElasticSearch() {
    return esResponseMap;
  }

  @Override
  protected Response getRecordByIdWithFieldsCassandra() {
    return null;
  }

  @Override
  protected Response getRecordByIdCassandra() {
    return null;
  }

  @Override
  protected List<Response> getResponseList(
      boolean isLocation, boolean isFirstRequired, boolean isSecondRequired) {
    List<Response> listResponse = new ArrayList<>();
    listResponse.add(getEsResponseForLocation(isLocation, isFirstRequired));
    listResponse.add(getEsResponse(isSecondRequired));
    return listResponse;
  }

  private static Response getEsResponseForLocation(boolean isLocation, boolean isFirstRequired) {

    Response response = null;
    if (isFirstRequired) {
      response = new Response();
      if (isLocation) {
        response.put(JsonKey.RESPONSE, Arrays.asList("location"));
        return response;
      }
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.ID, "anyId");
      response.put(JsonKey.RESPONSE, map);
      return response;
    }
    return response;
  }

  protected static Response getEsResponse(boolean isSecondRequired) {

    Response response = null;
    if (isSecondRequired) {
      response = new Response();
      Map<String, Object> map = new HashMap<>();
      map.put("anyString", new Object());
      response.put(JsonKey.RESPONSE, map);
      return response;
    }
    return response;
  }
}
