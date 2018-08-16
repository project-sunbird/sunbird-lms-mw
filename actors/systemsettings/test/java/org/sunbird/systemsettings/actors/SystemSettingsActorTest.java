package org.sunbird.systemsettings.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.service.SunbirdMWService;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.init.service.SystemSettingService;
import org.sunbird.init.service.impl.SystemSettingServiceImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.models.organization.Organization;
import scala.concurrent.duration.FiniteDuration;

@Ignore
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  SystemSettingServiceImpl.class,
  ElasticSearchUtil.class,
  ProjectUtil.class,
  SunbirdMWService.class,
  ServiceFactory.class
})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class SystemSettingsActorTest {

  private static final FiniteDuration ACTOR_MAX_WAIT_DURATION = duration("100 second");
  private ObjectMapper mapper = new ObjectMapper();
  private static ActorSystem system;
  private static CassandraOperation cassandraImpl;
  private static Props props = null;
  private static Util.DbInfo orgDB = null;
  private String orgId = "";
  private static SystemSettingService systemSettingService;
  private static SystemSettingService systemSettingServiceImpl;
  private static TestKit probe;
  private static ActorRef subject;
  private static Request actorMessage;

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    probe = new TestKit(system);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.mockStatic(ProjectUtil.class);
    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(SunbirdMWService.class);
    systemSettingServiceImpl = PowerMockito.mock(SystemSettingServiceImpl.class);
    cassandraImpl = PowerMockito.mock(CassandraOperationImpl.class);
    props = Props.create(SystemSettingsActor.class);
    subject = system.actorOf(props);
    actorMessage = new Request();
  }

  @Test
  public void testGetSystemSettingById() {   
  }

  @Test
  public void testGetSystemSettingByIdInvalidParam() {   
  }

  @Test
  public void testGetAllSystemSettings() {
   
  }
}
