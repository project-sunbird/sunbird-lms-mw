package org.sunbird.systemsettings.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.*;
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
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.systemsettings.model.SystemSetting;
import org.sunbird.systemsettings.service.SystemSettingService;
import org.sunbird.systemsettings.service.impl.SystemSettingServiceImpl;
import scala.concurrent.duration.FiniteDuration;

/**
 * This class implements the test cases for system settings actor methods
 *
 * @author Loganathan
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ElasticSearchUtil.class,
  CassandraOperationImpl.class,
  ServiceFactory.class,
  SystemSettingServiceImpl.class
})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class SystemSettingsActorTest {
  private static final FiniteDuration ACTOR_MAX_WAIT_DURATION = duration("100 second");
  private ObjectMapper mapper = new ObjectMapper();
  private ActorSystem system;
  private Props props;
  private TestKit probe;
  private ActorRef subject;
  private Request actorMessage;
  private CassandraOperation cassandraOperation;
  private SystemSettingService systemSettingService;
  private SystemSetting systemSetting;
  private ProjectCommonException exception;

  @Before
  public void setUp() {
    system = ActorSystem.create("system");
    probe = new TestKit(system);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = PowerMockito.mock(CassandraOperationImpl.class);
    systemSettingService = PowerMockito.mock(SystemSettingServiceImpl.class);
    props = Props.create(SystemSettingsActor.class);
    subject = system.actorOf(props);
    actorMessage = new Request();
    systemSetting = new SystemSetting();
  }

  @Test
  public void TestUpdateSystemSettingById() {
    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> orgData = new HashMap<String, Object>();
    orgData.put(JsonKey.ID, "defaultRootOrgId");
    orgData.put(JsonKey.FIELD, "defaultRootOrgId");
    orgData.put(JsonKey.VALUE, "0125656442955776000");
    boolean thrown = false;
    data.put(JsonKey.SYSTEM_SETTINGS, orgData);
    try {
      PowerMockito.when(systemSettingService.setSetting(Mockito.any())).thenReturn(new Response());
    } catch (Exception e) {
      thrown = true;
    }

    actorMessage.setOperation(ActorOperations.UPDATE_SYSTEM_SETTING_BY_ID.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());

    Object resp = probe.expectMsgAnyClassOf(ACTOR_MAX_WAIT_DURATION, Response.class);
    Assert.assertTrue(null != resp);
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void TestUpdateSystemSettingByIdError() {
    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> orgData = new HashMap<String, Object>();
    orgData.put(JsonKey.ID, "defaultRootOrgId");
    orgData.put(JsonKey.FIELD, "defaultRootOrgId");
    orgData.put(JsonKey.VALUE, "0125656442955776000");
    boolean thrown = false;
    data.put(JsonKey.SYSTEM_SETTINGS, orgData);
    try {
      PowerMockito.when(systemSettingService.setSetting(Mockito.any()))
          .thenThrow(new ProjectCommonException("SERVER_ERROR", "Internal Server Error", 500));
    } catch (Exception e) {
      thrown = true;
    }

    actorMessage.setOperation(ActorOperations.UPDATE_SYSTEM_SETTING_BY_ID.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());

    Object resp = probe.expectMsgAnyClassOf(ACTOR_MAX_WAIT_DURATION, Exception.class);
    Assert.assertTrue(null != resp);
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void TestGetSystemSettingById() {
    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> orgData = new HashMap<String, Object>();
    orgData.put(JsonKey.ID, "defaultRootOrgId");
    boolean thrown = false;
    data.put(JsonKey.SYSTEM_SETTINGS, orgData);
    try {
      Response response = new Response();
      response.put(
          JsonKey.RESPONSE, new ArrayList<Map<String, Object>>(Arrays.asList(new HashMap<>())));
      PowerMockito.when(systemSettingService.readSetting(Mockito.anyString()))
          .thenReturn(
              new SystemSetting("defaultRootOrgId", "defaultRootOrgId", "0125656442955776000"));
    } catch (Exception e) {
      thrown = true;
    }

    actorMessage.setOperation(ActorOperations.GET_SYSTEM_SETTING_BY_ID.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());

    Object resp = probe.expectMsgAnyClassOf(ACTOR_MAX_WAIT_DURATION, Response.class);
    Assert.assertTrue(null != resp);
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void TestGetAllSystemSettings() {
    boolean thrown = false;
    try {
      Response response = new Response();
      response.put(
          JsonKey.RESPONSE, new ArrayList<Map<String, Object>>(Arrays.asList(new HashMap<>())));
      PowerMockito.when(systemSettingService.readAllSettings()).thenReturn(new Response());
    } catch (Exception e) {
      thrown = true;
    }
    actorMessage.setOperation(ActorOperations.GET_ALL_SYSTEM_SETTINGS.getValue());
    subject.tell(actorMessage, probe.getRef());

    Object resp = probe.expectMsgAnyClassOf(ACTOR_MAX_WAIT_DURATION, Response.class);
    Assert.assertTrue(null != resp);
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void TestGetSystemSettingByIdError() {
    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> orgData = new HashMap<String, Object>();
    orgData.put(JsonKey.ID, "defaultRootOrgId");
    boolean thrown = false;
    data.put(JsonKey.SYSTEM_SETTINGS, orgData);
    try {
      PowerMockito.when(systemSettingService.readSetting(Mockito.anyString()))
          .thenThrow(new ProjectCommonException("SERVER_ERROR", "Internal Server Error", 500));
    } catch (Exception e) {
      thrown = true;
    }

    actorMessage.setOperation(ActorOperations.GET_SYSTEM_SETTING_BY_ID.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());

    Object resp = probe.expectMsgAnyClassOf(ACTOR_MAX_WAIT_DURATION, Exception.class);
    Assert.assertTrue(null != resp);
    Assert.assertEquals(false, thrown);
  }

  @Test
  public void TestGetAllSystemSettingsError() {
    boolean thrown = false;
    try {
      Response response = new Response();
      response.put(
          JsonKey.RESPONSE, new ArrayList<Map<String, Object>>(Arrays.asList(new HashMap<>())));
      PowerMockito.when(systemSettingService.readAllSettings())
          .thenThrow(new ProjectCommonException("SERVER_ERROR", "Internal Server Error", 500));
    } catch (Exception e) {
      thrown = true;
    }
    actorMessage.setOperation(ActorOperations.GET_ALL_SYSTEM_SETTINGS.getValue());
    subject.tell(actorMessage, probe.getRef());

    Object resp = probe.expectMsgAnyClassOf(ACTOR_MAX_WAIT_DURATION, Exception.class);
    Assert.assertTrue(null != resp);
    Assert.assertEquals(false, thrown);
  }
}
