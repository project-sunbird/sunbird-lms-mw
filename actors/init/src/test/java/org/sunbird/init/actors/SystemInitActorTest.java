package org.sunbird.init.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.*;
import org.junit.runner.RunWith;
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
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.init.model.SystemSetting;
import org.sunbird.init.service.SystemSettingService;
import org.sunbird.init.service.impl.SystemSettingServiceImpl;
import org.sunbird.learner.util.Util;
import scala.concurrent.duration.FiniteDuration;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ElasticSearchUtil.class,
  CassandraOperationImpl.class,
  ServiceFactory.class,
  SystemSettingServiceImpl.class
})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class SystemInitActorTest {
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

  @Before
  public void setUp() {
    system = ActorSystem.create("system");
    probe = new TestKit(system);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = PowerMockito.mock(CassandraOperationImpl.class);
    systemSettingService = PowerMockito.mock(SystemSettingServiceImpl.class);
    props = Props.create(SystemInitActor.class);
    subject = system.actorOf(props);
    actorMessage = new Request();
    systemSetting = new SystemSetting();
  }

  @Test
  public void systemInitRootOrgTest() throws IOException {
    Response response = new Response();
    Map<String, Object> respData = new HashMap<String, Object>();
    respData.put(JsonKey.ID, "0125571436950241280");
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    response.put(JsonKey.RESPONSE, respData);
    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> orgData = new HashMap<String, Object>();
    orgData.put(JsonKey.ORG_NAME, "orgTest");
    orgData.put(JsonKey.CHANNEL, "orgTestChannel");
    data.put(JsonKey.ORGANISATION, orgData);

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Map<String, Object> queryMap = new HashMap<String, Object>();
    queryMap.put(JsonKey.IS_ROOT_ORG, true);

    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.CHANNEL, orgData.get(JsonKey.CHANNEL));
    filters.put(JsonKey.IS_ROOT_ORG, true);
    PowerMockito.when(systemSettingService.readSetting(JsonKey.IS_ROOT_ORG_INITIALISED))
        .thenReturn(null);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(
                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), queryMap))
        .thenReturn(null);
    PowerMockito.when(
            cassandraOperation.insertRecord(
                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgData))
        .thenReturn(response);
    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    Map<String, Object> esData = new HashMap<String, Object>();
    PowerMockito.when(
            ElasticSearchUtil.complexSearch(
                searchDTO,
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName()))
        .thenReturn(null);
    actorMessage.setOperation(ActorOperations.SYSTEM_INIT_ROOT_ORG.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    Object resp = probe.expectMsgAnyClassOf(ACTOR_MAX_WAIT_DURATION, Response.class);
    Assert.assertTrue(null != resp);
  }

  @Test
  public void systemInitRootOrgAlreadyInitialisedTest() throws IOException {

    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> orgData = new HashMap<String, Object>();
    orgData.put(JsonKey.ORG_NAME, "orgTest");
    orgData.put(JsonKey.CHANNEL, "orgTestChannel");
    data.put(JsonKey.ORGANISATION, orgData);

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Map<String, Object> queryMap = new HashMap<String, Object>();
    queryMap.put(JsonKey.IS_ROOT_ORG, true);

    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.CHANNEL, orgData.get(JsonKey.CHANNEL));
    filters.put(JsonKey.IS_ROOT_ORG, true);
    systemSetting.setId(JsonKey.IS_ROOT_ORG_INITIALISED);
    systemSetting.setField(JsonKey.IS_ROOT_ORG_INITIALISED);
    systemSetting.setValue(JsonKey.COMPLETED);
    PowerMockito.when(systemSettingService.readSetting(JsonKey.IS_ROOT_ORG_INITIALISED))
        .thenReturn(systemSetting);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(
                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), queryMap))
        .thenReturn(null);
    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    Map<String, Object> esData = new HashMap<String, Object>();
    PowerMockito.when(
            ElasticSearchUtil.complexSearch(
                searchDTO,
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName()))
        .thenReturn(null);
    actorMessage.setOperation(ActorOperations.SYSTEM_INIT_ROOT_ORG.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException resp =
        probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, ProjectCommonException.class);
    Assert.assertTrue(null != resp);
  }

  @Test
  public void systemInitRootOrgStatusStartedOrgNotExistsTest() throws IOException {
    Response response = new Response();
    Map<String, Object> respData = new HashMap<String, Object>();
    respData.put(JsonKey.ID, "0125571436950241280");
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    response.put(JsonKey.RESPONSE, respData);
    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> orgData = new HashMap<String, Object>();
    orgData.put(JsonKey.ORG_NAME, "orgTest");
    orgData.put(JsonKey.CHANNEL, "orgTestChannel");
    data.put(JsonKey.ORGANISATION, orgData);

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Map<String, Object> queryMap = new HashMap<String, Object>();
    queryMap.put(JsonKey.IS_ROOT_ORG, true);

    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.CHANNEL, orgData.get(JsonKey.CHANNEL));
    filters.put(JsonKey.IS_ROOT_ORG, true);
    systemSetting.setId(JsonKey.IS_ROOT_ORG_INITIALISED);
    systemSetting.setField(JsonKey.IS_ROOT_ORG_INITIALISED);
    systemSetting.setValue(JsonKey.STARTED);
    PowerMockito.when(systemSettingService.readSetting(JsonKey.IS_ROOT_ORG_INITIALISED))
        .thenReturn(systemSetting);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(
                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), queryMap))
        .thenReturn(null);
    PowerMockito.when(
            cassandraOperation.insertRecord(
                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgData))
        .thenReturn(response);
    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    Map<String, Object> esData = new HashMap<String, Object>();
    PowerMockito.when(
            ElasticSearchUtil.complexSearch(
                searchDTO,
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName()))
        .thenReturn(null);
    actorMessage.setOperation(ActorOperations.SYSTEM_INIT_ROOT_ORG.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    Response resp = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, Response.class);
    Assert.assertTrue(null != resp);
  }

  @Test
  public void systemInitRootOrgStatusStartedOrgExistsTest() throws IOException {
    Response response = new Response();
    Map<String, Object> respData = new HashMap<String, Object>();
    respData.put(JsonKey.ID, "0125571436950241280");
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    response.put(JsonKey.RESPONSE, respData);
    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> orgData = new HashMap<String, Object>();
    orgData.put(JsonKey.ORG_NAME, "orgTest");
    orgData.put(JsonKey.CHANNEL, "orgTestChannel");
    data.put(JsonKey.ORGANISATION, orgData);

    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Map<String, Object> queryMap = new HashMap<String, Object>();
    queryMap.put(JsonKey.IS_ROOT_ORG, true);

    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.CHANNEL, orgData.get(JsonKey.CHANNEL));
    filters.put(JsonKey.IS_ROOT_ORG, true);
    systemSetting.setId(JsonKey.IS_ROOT_ORG_INITIALISED);
    systemSetting.setField(JsonKey.IS_ROOT_ORG_INITIALISED);
    systemSetting.setValue(JsonKey.STARTED);

    Response orgRespData = new Response();
    orgRespData.put(JsonKey.ID, "0125571436950241280");
    orgRespData.put(JsonKey.ORG_NAME, orgData.get(JsonKey.ORG_NAME));
    orgRespData.put(JsonKey.CHANNEL, orgData.get(JsonKey.CHANNEL));
    orgRespData.put(JsonKey.IS_ROOT_ORG, true);

    PowerMockito.when(systemSettingService.readSetting(JsonKey.IS_ROOT_ORG_INITIALISED))
        .thenReturn(systemSetting);
    PowerMockito.when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    PowerMockito.when(
            cassandraOperation.getRecordsByProperties(
                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), queryMap))
        .thenReturn(orgRespData);
    PowerMockito.when(
            cassandraOperation.insertRecord(
                orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgData))
        .thenReturn(response);
    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
    Map<String, Object> esData = new HashMap<String, Object>();
    PowerMockito.when(
            ElasticSearchUtil.complexSearch(
                searchDTO,
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName()))
        .thenReturn(null);
    actorMessage.setOperation(ActorOperations.SYSTEM_INIT_ROOT_ORG.getValue());
    actorMessage.getRequest().putAll(data);
    subject.tell(actorMessage, probe.getRef());
    Response resp = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, Response.class);
    Assert.assertTrue(null != resp);
  }
}
