package org.sunbird.user;

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
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.actors.UserManagementActor;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
  Util.class,
  RequestRouter.class,
  BaseMWService.class,
  SystemSettingClientImpl.class,
  SunbirdMWService.class
})
@PowerMockIgnore({"javax.management.*"})
public class UserActorTest {

  private ActorSystem system = ActorSystem.create("system");

  private final Props props = Props.create(UserManagementActor.class);
  private static SystemSettingClientImpl systemSettingClient;
  private static final String LATEST_VERSION = "latestVersion";

  @Before
  public void beforeEachTest() throws Exception {

    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(RequestRouter.class);
    PowerMockito.mockStatic(SystemSettingClientImpl.class);
    systemSettingClient = mock(SystemSettingClientImpl.class);
    when(SystemSettingClientImpl.getInstance()).thenReturn(systemSettingClient);
    ActorRef actorRef = mock(ActorRef.class);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.mockStatic(SunbirdMWService.class);
    SunbirdMWService.tellToBGRouter(Mockito.any(), Mockito.any());
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);
  }

  @Test
  public void testGetMediaTypesSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    when(systemSettingClient.getSystemSettingByFieldAndKey(
            Mockito.any(ActorRef.class),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyObject()))
        .thenReturn(new HashMap<>());

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CALLER_ID, "anyCallerId");
    innerMap.put(JsonKey.VERSION, "v2");
    reqObj.setContext(innerMap);
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  /*@Test
  public void testSetProfileVisibilitySuccess() {
    final String userId = "USER-ID";
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId))
            .thenReturn(createGetResponse(true));
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROFILE_VISIBILITY.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testSetProfileVisibilityFailure() {
    final String userId = "USER-ID";
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId))
            .thenReturn(createGetResponse(false));
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROFILE_VISIBILITY.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
            probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(res.getCode() == ResponseCode.userNotFound.getErrorCode());
  }*/

  private Map<String, Object> createGetResponse(boolean isSuccess) {
    HashMap<String, Object> response = new HashMap<>();
    if (isSuccess) response.put(JsonKey.CONTENT, "Any-content");
    return response;
  }

  private Response getSuccessResponse() {
    Response response = new Response();
    List<Map<String, Object>> resMapList = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    resMapList.add(map);
    response.put(JsonKey.RESPONSE, resMapList);
    return response;
  }
}
