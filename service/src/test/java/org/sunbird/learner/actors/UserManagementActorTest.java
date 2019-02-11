package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.actorutil.location.impl.LocationClientImpl;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
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
  LocationClientImpl.class
})
@PowerMockIgnore({"javax.management.*"})
public class UserManagementActorTest extends UserManagementActorTestBase {

  @Test
  public void testCreateUserSuccessWithUserCallerId() {

    boolean result =
        testScenario(
            getRequest(true, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutUserCallerId() {

    boolean result =
        testScenario(
            getRequest(
                false, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutUserCallerIdChannelAndRootOrgId() {

    boolean result =
        testScenario(getRequest(false, false, true, reqMap, ActorOperations.CREATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidChannelAndOrgId() {

    reqMap.put(JsonKey.CHANNEL, "anyReqChannel");
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.parameterMismatch);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidLocationCodes() {
    when(InterServiceCommunicationFactory.getInstance())
        .thenReturn(interServiceCommunication)
        .thenReturn(interServiceCommunication);
    when(interServiceCommunication.getResponse(
            Mockito.any(ActorRef.class), Mockito.any(Request.class)))
        .thenReturn(null);
    reqMap.put(JsonKey.LOCATION_CODES, Arrays.asList("invalidLocationCode"));
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.invalidParameterValue);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutVersion() {

    boolean result =
        testScenario(getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithLocationCodes() {
    when(InterServiceCommunicationFactory.getInstance())
        .thenReturn(interServiceCommunication)
        .thenReturn(interServiceCommunication);
    when(interServiceCommunication.getResponse(
            Mockito.any(ActorRef.class), Mockito.any(Request.class)))
        .thenReturn(getEsResponseForLocation())
        .thenReturn(getEsResponse());
    reqMap.put(JsonKey.LOCATION_CODES, Arrays.asList("locationCode"));
    boolean result =
        testScenario(getRequest(true, true, true, reqMap, ActorOperations.CREATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidExternalIds() {

    reqMap.put(JsonKey.EXTERNAL_IDS, "anyExternalId");
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.dataTypeError);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidRoles() {

    reqMap.put(JsonKey.ROLES, "anyRoles");
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.dataTypeError);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidCountryCode() {

    reqMap.put(JsonKey.COUNTRY_CODE, "anyCode");
    boolean result =
        testScenario(
            getRequest(false, false, false, reqMap, ActorOperations.CREATE_USER),
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
            getRequest(
                false, false, false, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            ResponseCode.invalidOrgData);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserFailureWithLocationCodes() {
    when(interServiceCommunication.getResponse(
            Mockito.any(ActorRef.class), Mockito.any(Request.class)))
        .thenReturn(null);
    boolean result =
        testScenario(
            getRequest(
                true, true, true, getUpdateRequestWithLocationCodes(), ActorOperations.UPDATE_USER),
            ResponseCode.invalidParameterValue);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccess() {

    boolean result =
        testScenario(
            getRequest(true, true, true, getExternalIdMap(), ActorOperations.UPDATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccessWithLocationCodes() {
    when(InterServiceCommunicationFactory.getInstance())
        .thenReturn(interServiceCommunication)
        .thenReturn(interServiceCommunication);
    when(interServiceCommunication.getResponse(
            Mockito.any(ActorRef.class), Mockito.any(Request.class)))
        .thenReturn(getEsResponseForLocation())
        .thenReturn(getEsResponse());
    boolean result =
        testScenario(
            getRequest(
                true, true, true, getUpdateRequestWithLocationCodes(), ActorOperations.UPDATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccessWithoutUserCallerId() {

    boolean result =
        testScenario(
            getRequest(false, true, true, getExternalIdMap(), ActorOperations.UPDATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithUserTypeAsTeacher() {
    reqMap.put(JsonKey.USER_TYPE, JsonKey.TEACHER);

    when(userService.getRootOrgIdFromChannel(Mockito.anyString()))
        .thenReturn("rootOrgId")
        .thenReturn("");

    boolean result =
        testScenario(
            getRequest(true, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithUserTypeAsOther() {
    reqMap.put(JsonKey.USER_TYPE, JsonKey.OTHER);

    boolean result =
        testScenario(
            getRequest(true, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithUserTypeAsTeacherAndCustodianOrg() {
    reqMap.put(JsonKey.USER_TYPE, JsonKey.TEACHER);

    boolean result =
        testScenario(
            getRequest(false, false, true, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.errorTeacherCannotBelongToCustodianOrg);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccessWithUserTypeTeacher() {
    Map<String, Object> req = getExternalIdMap();
    req.put(JsonKey.USER_TYPE, JsonKey.TEACHER);
    when(userService.getUserById(Mockito.anyString())).thenReturn(getUser(false));
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("rootOrgId1");
    boolean result =
        testScenario(getRequest(false, true, true, req, ActorOperations.UPDATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserFailureWithUserTypeTeacher() {
    Map<String, Object> req = getExternalIdMap();
    req.put(JsonKey.USER_TYPE, JsonKey.TEACHER);
    when(userService.getUserById(Mockito.anyString())).thenReturn(getUser(false));
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("rootOrgId");
    boolean result =
        testScenario(
            getRequest(false, true, true, req, ActorOperations.UPDATE_USER),
            ResponseCode.errorTeacherCannotBelongToCustodianOrg);
    assertTrue(result);
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
}
