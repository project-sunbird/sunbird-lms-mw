package org.sunbird.location.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LocationActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.location.dao.LocationDao;
import org.sunbird.location.dao.impl.LocationDaoFactory;
import org.sunbird.location.dao.impl.LocationDaoImpl;
import org.sunbird.location.model.Location;
import scala.concurrent.duration.FiniteDuration;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LocationDaoFactory.class, LocationDaoImpl.class, ElasticSearchUtil.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class LocationActorTest {

  private static final FiniteDuration ACTOR_MAX_WAIT_DURATION = duration("100 second");
  private ObjectMapper mapper = new ObjectMapper();
  private ActorSystem system;
  private Props props;
  private LocationDao locDaoImpl;
  private TestKit probe;
  private ActorRef subject;
  // private LocationDaoFactory locFactory;
  private Request actorMessage;

  @Before
  public void setUp() {
    system = ActorSystem.create("system");
    probe = new TestKit(system);
    PowerMockito.mockStatic(LocationDaoFactory.class);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    locDaoImpl = PowerMockito.mock(LocationDaoImpl.class);
    props = Props.create(LocationActor.class, locDaoImpl);
    subject = system.actorOf(props);
    actorMessage = new Request();
  }

  @Test
  public void testCreateLocation() throws IOException {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    response.put(JsonKey.RESPONSE, response);
    Map<String, Object> data = new HashMap<String, Object>();
    data.put(GeoLocationJsonKey.LOCATION_TYPE, "STATE");
    data.put(GeoLocationJsonKey.CODE, "S01");
    data.put(JsonKey.NAME, "DUMMY_STATE");
    Location location = mapper.convertValue(data, Location.class);
    PowerMockito.when(LocationDaoFactory.getInstance()).thenReturn(locDaoImpl);
    PowerMockito.when(locDaoImpl.create(location)).thenReturn(response);
    actorMessage.setOperation(LocationActorOperation.CREATE_LOCATION.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    Response resp = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, Response.class);
    Assert.assertTrue(null != resp);
  }
}
