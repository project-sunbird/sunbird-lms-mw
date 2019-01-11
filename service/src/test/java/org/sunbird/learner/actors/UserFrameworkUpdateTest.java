package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.util.ContentStoreUtil;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.user.dao.impl.UserExternalIdentityDaoImpl;
import org.sunbird.user.service.impl.UserServiceImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataCacheHandler.class, ContentStoreUtil.class, UserServiceImpl.class, Util.class})
public class UserFrameworkUpdateTest extends UserManagementActorTest {

  private static UserExternalIdentityDaoImpl userExtDao;
  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = getProps();

  @Before
  public void beforeEachTestn() {
    userExtDao = mock(UserExternalIdentityDaoImpl.class);
    PowerMockito.mockStatic(DataCacheHandler.class);
    PowerMockito.mockStatic(ContentStoreUtil.class);
    PowerMockito.mockStatic(Util.class);
    Mockito.doNothing().when(userService).validateUserId(Mockito.any());
  }

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
  public void testUpdateUserFrameworkSuccess() {
    Request reqObj = getRequest(null, null);
    Response res = doUpdateActorCallSuccess(reqObj);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidGradeLevel() {
    Request reqObj = getRequest("gradeLevel", "SomeWrongGrade");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidMedium() {
    Request reqObj = getRequest("medium", "glish");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidBoard() {
    Request reqObj = getRequest("board", "RBCS");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidFrameworkId() {
    Request reqObj = getRequest(JsonKey.ID, "invalidFrameworkId");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.errorNoFrameworkFound.getErrorCode()));
  }

  private Response doUpdateActorCallSuccess(Request reqObj) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockForUpdateTest();
    mockuserExtDao(reqObj);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    return res;
  }

  private ProjectCommonException doUpdateActorCallFailure(Request reqObj) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockForUpdateTest();
    mockuserExtDao(reqObj);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    return res;
  }

  @SuppressWarnings("unchecked")
  public void mockForUpdateTest() {
    mockUtilsForOrgDetails();
    mockDatacacheHandler();
    mockContentStoreUtil();

    try {
      PowerMockito.whenNew(UserExternalIdentityDaoImpl.class)
          .withNoArguments()
          .thenReturn(userExtDao);

    } catch (Exception e) {
      ProjectLogger.log("Error while mocking UserExternalIdentityDaoImpl");
    }
  }

  private void mockuserExtDao(Request req) {
    when(userExtDao.getUserId(req)).thenReturn(userId);
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
    when(ContentStoreUtil.readFramework("invalidFrameworkId")).thenReturn(contentMap);
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
    frameworkCategoriesMap.put("medium", getListForCategoryMap("English"));
    frameworkCategoriesMap.put("gradeLevel", getListForCategoryMap("Grade 3"));
    frameworkCategoriesMap.put("board", getListForCategoryMap("NCERT"));

    Map<String, Map<String, List<Map<String, String>>>> x = new HashMap<>();
    x.put("NCF", frameworkCategoriesMap);
    DataCacheHandler.updateFrameworkCategoriesMap("NCF", frameworkCategoriesMap);
    when(DataCacheHandler.getFrameworkCategoriesMap()).thenReturn(x);
    Map<String, List<String>> map1 = new HashMap<>();
    List<String> list1 = Arrays.asList("NCF");
    map1.put("someHashTagId", list1);
    when(DataCacheHandler.getHashtagIdFrameworkIdMap()).thenReturn(map1);
  }
}
