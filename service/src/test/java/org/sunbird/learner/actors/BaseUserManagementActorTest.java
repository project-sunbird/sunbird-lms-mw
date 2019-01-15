package org.sunbird.learner.actors;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
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
import org.sunbird.actorutil.location.impl.LocationClientImpl;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.actors.UserManagementActor;
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
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public abstract class BaseUserManagementActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(UserManagementActor.class);
  protected static final String userId = "testUserId";

  public ActorSystem getActorSystem() {
    return system;
  }

  public static Props getProps() {
    return props;
  }

  protected static Map<String, Object> reqMap;
  static InterServiceCommunication interServiceCommunication =
      mock(InterServiceCommunicationImpl.class);;

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

    when(cassandraOperation.getRecordsByCompositeKey(
            Mockito.anyString(), Mockito.anyString(), Mockito.any()))
        .thenReturn(getUserFromExternalId());
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getUserFromExternalId())
        .thenReturn(getUserFromExternalId())
        .thenReturn(getUserFromExternalId());
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    when(ElasticSearchUtil.complexSearch(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getEsContent());

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
  }

  private Map<String, Object> getEsContent() {
    Map<String, Object> esContent = new HashMap<>();
    List<Map<String, Object>> esContentList = new ArrayList<>();
    Map<String, Object> esContent2 = new HashMap<>();
    esContent2.put(JsonKey.STATUS, 1);
    esContent2.put(JsonKey.ID, "rootOrgId");
    esContentList.add(esContent2);
    esContent.put(JsonKey.CONTENT, esContentList);
    return esContent;
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

  private Response getUserFromExternalId() {
    Response response = new Response();
    Map<String, Object> map = new HashMap<>();
    map.put("any", "any");
    map.put("rootOrgId", "anyValue");
    List<Map<String, Object>> userRecordList = new ArrayList<>();
    userRecordList.add(map);
    userRecordList.add(map);
    response.put(JsonKey.RESPONSE, userRecordList);
    return response;
  }

  protected static Response getEsResponse() {
    Response response = new Response();
    Map<String, Object> map = new HashMap<>();
    map.put("anyString", new Object());
    map.put(JsonKey.ERRORS, null);
    response.put(JsonKey.RESPONSE, map);
    return response;
  }
}
