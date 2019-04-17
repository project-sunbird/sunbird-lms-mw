package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actor.service.BaseMWService;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.role.dao.impl.RoleDaoImpl;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.user.dao.impl.UserOrgDaoImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  RoleDaoImpl.class,
  BaseMWService.class,
  RequestRouter.class,
  InterServiceCommunicationFactory.class,
  ElasticSearchUtil.class,
  Util.class,
  UserOrgDaoImpl.class,
  DecryptionService.class,
  DataCacheHandler.class
})
@PowerMockIgnore({"javax.management.*"})
public class PageMngmntActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(PageManagementActor.class);
  private static final InterServiceCommunication interServiceCommunication =
      Mockito.mock(InterServiceCommunication.class);
  private static final Response response = Mockito.mock(Response.class);
  private static CassandraOperationImpl cassandraOperation;

  private static String sectionId = null;
  private static String sectionId2 = null;
  private static String pageId = "anyID";
  private static String pageIdWithOrg = null;
  private static String pageIdWithOrg2 = null;

  @BeforeClass
  public static void beforeClass() {

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    PowerMockito.mockStatic(DataCacheHandler.class);
    when(DataCacheHandler.getPageMap()).thenReturn(Mockito.anyMap());
    Map map = mock(Map.class);
    when(map.get(Mockito.anyString())).thenReturn(new HashMap<>());
  }

  /* private static Response getRecordByPropertyResponse() {

      Response response = new Response();
      List<Map<String, Object>> list = new ArrayList<>();
      Map<String, Object> orgMap = new HashMap<>();
      orgMap.put(JsonKey.ID, "ORGANISATION_ID");
      list.add(orgMap);
      response.put(JsonKey.RESPONSE, list);
      return response;
  }*/

  @Before
  public void beforeEachTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    /*PowerMockito.mockStatic(BaseMWService.class);
    PowerMockito.mockStatic(RequestRouter.class);
    PowerMockito.mockStatic(InterServiceCommunicationFactory.class);
    PowerMockito.mockStatic(RoleDaoImpl.class);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.mockStatic(Util.class);
    PowerMockito.mockStatic(UserOrgDaoImpl.class);

    when(InterServiceCommunicationFactory.getInstance()).thenReturn(interServiceCommunication);
    RoleDaoImpl roleDao = Mockito.mock(RoleDaoImpl.class);
    when(RoleDaoImpl.getInstance()).thenReturn(roleDao);
    UserOrgDao userOrgDao = Mockito.mock(UserOrgDaoImpl.class);
    when(UserOrgDaoImpl.getInstance()).thenReturn(userOrgDao);
    when(userOrgDao.updateUserOrg(Mockito.anyObject())).thenReturn(getSuccessResponse());
    CompletionStage completionStage = Mockito.mock(CompletionStage.class);
    ActorSelection actorSelection = Mockito.mock(ActorSelection.class);
    when(BaseMWService.getRemoteRouter(Mockito.anyString())).thenReturn(actorSelection);
    when(actorSelection.resolveOneCS(Duration.create(Mockito.anyLong(), "seconds")))
            .thenReturn(completionStage);
    ActorRef actorRef = Mockito.mock(ActorRef.class);
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);
    SearchDTO searchDTO = Mockito.mock(SearchDTO.class);
    when(Util.createSearchDto(Mockito.anyMap())).thenReturn(searchDTO);*/

    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);

    /*when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
            .thenReturn(getCassandraResponse());
    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
            .thenReturn(getRecordByPropertyResponse());*/

    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getUpdateSuccess());
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordById());
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getUpdateSuccess());
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordById());
  }

  private static Response getUpdateSuccess() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private static Response cassandraGetRecordById() {
    Response response = new Response();
    List list = new ArrayList();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.NAME, "anyName");
    map.put(JsonKey.ID, "anyId");
    map.put(JsonKey.SECTIONS, "anySection");
    list.add(map);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  /*@Test
  public void testGetUserRoleSuccess() {
      assertTrue(testScenario(true, true, null));
  }

  @Test
  public void testAssignRolesSuccessWithValidOrgId() {
      assertTrue(testScenario(true, null));
  }

  @Test
  public void testAssignRolesSuccessWithoutOrgId() {
      assertTrue(testScenario(false, null));
  }

  @Test
  public void testAssignRolesFailure() {
      assertTrue(testScenario(false, ResponseCode.CLIENT_ERROR));
  }

  @Test
  public void testAssignRolesFailureWithInvalidOrgId() {
      assertTrue(testScenario(false, ResponseCode.invalidParameterValue));
  }*/

  @Test
  public void testInvalidRequest() {

    Request reqObj = new Request();

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    subject.tell(reqObj, probe.getRef());
    NullPointerException exc =
        probe.expectMsgClass(duration("100 second"), NullPointerException.class);
    assertTrue(null != exc);
  }

  @Test
  public void testInvalidOperationSuccess() {

    Request reqObj = new Request();
    reqObj.setOperation("INVALID_OPERATION");

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void testCreateSuccessPageWithOrgId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();
    List<Map<String, Object>> appMapList = new ArrayList<Map<String, Object>>();
    Map<String, Object> appMap = new HashMap<String, Object>();
    appMap.put(JsonKey.ID, sectionId);
    appMap.put(JsonKey.INDEX, new BigInteger("1"));
    appMap.put(JsonKey.GROUP, new BigInteger("1"));
    appMapList.add(appMap);

    pageMap.put(JsonKey.APP_MAP, appMapList);

    List<Map<String, Object>> portalMapList = new ArrayList<Map<String, Object>>();
    Map<String, Object> portalMap = new HashMap<String, Object>();
    portalMap.put(JsonKey.ID, sectionId);
    portalMap.put(JsonKey.INDEX, new BigInteger("1"));
    portalMap.put(JsonKey.GROUP, new BigInteger("1"));
    portalMapList.add(portalMap);

    pageMap.put(JsonKey.PORTAL_MAP, portalMapList);

    pageMap.put(JsonKey.PAGE_NAME, "Test Page");
    pageMap.put(JsonKey.ORGANISATION_ID, "ORG1");
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getRecordByPropMap(false));
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    pageIdWithOrg = (String) response.get(JsonKey.PAGE_ID);
    assertTrue(null != pageIdWithOrg);
  }

  @Test
  public void testCreatePageSuccessWithoutOrgId() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();
    List<Map<String, Object>> appMapList = new ArrayList<Map<String, Object>>();
    Map<String, Object> appMap = new HashMap<String, Object>();
    appMap.put(JsonKey.ID, sectionId);
    appMap.put(JsonKey.INDEX, new BigInteger("1"));
    appMap.put(JsonKey.GROUP, new BigInteger("1"));
    appMapList.add(appMap);

    pageMap.put(JsonKey.APP_MAP, appMapList);

    List<Map<String, Object>> portalMapList = new ArrayList<Map<String, Object>>();
    Map<String, Object> portalMap = new HashMap<String, Object>();
    portalMap.put(JsonKey.ID, sectionId);
    portalMap.put(JsonKey.INDEX, new BigInteger("1"));
    portalMap.put(JsonKey.GROUP, new BigInteger("1"));
    portalMapList.add(portalMap);

    pageMap.put(JsonKey.PORTAL_MAP, portalMapList);

    pageMap.put(JsonKey.PAGE_NAME, "Test Page3");
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getRecordByPropMap(false));
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    pageIdWithOrg2 = (String) response.get(JsonKey.PAGE_ID);
    assertTrue(null != pageIdWithOrg2);
  }

  @Test
  public void testCreatePageFailureWithPageAlreadyExists() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();
    List<Map<String, Object>> appMapList = new ArrayList<Map<String, Object>>();
    Map<String, Object> appMap = new HashMap<String, Object>();
    appMap.put(JsonKey.ID, sectionId);
    appMap.put(JsonKey.INDEX, new BigInteger("1"));
    appMap.put(JsonKey.GROUP, new BigInteger("1"));
    appMapList.add(appMap);

    pageMap.put(JsonKey.APP_MAP, appMapList);

    List<Map<String, Object>> portalMapList = new ArrayList<Map<String, Object>>();
    Map<String, Object> portalMap = new HashMap<String, Object>();
    portalMap.put(JsonKey.ID, sectionId);
    portalMap.put(JsonKey.INDEX, new BigInteger("1"));
    portalMap.put(JsonKey.GROUP, new BigInteger("1"));
    portalMapList.add(portalMap);

    pageMap.put(JsonKey.PORTAL_MAP, portalMapList);

    pageMap.put(JsonKey.PAGE_NAME, "Test Page");
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(cassandraGetRecordByProperty(""));

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void testGetPageSetting() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    boolean pageName = false;
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_SETTING.getValue());
    reqObj.getRequest().put(JsonKey.ID, "Test Page");

    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordByProperty(""));

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Map<String, Object> result = response.getResult();
    Map<String, Object> page = (Map<String, Object>) result.get(JsonKey.PAGE);
    if (null != page.get(JsonKey.NAME) && ((String) page.get(JsonKey.NAME)).equals("anyName")) {
      pageName = true;
    }
    assertTrue(pageName);
  }

  @Test
  public void testGetPageSettingWithAppMap() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    boolean pageName = false;

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_SETTING.getValue());
    reqObj.getRequest().put(JsonKey.ID, "Test Page");

    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordByProperty(JsonKey.APP_MAP));

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Map<String, Object> result = response.getResult();
    Map<String, Object> page = (Map<String, Object>) result.get(JsonKey.PAGE);
    if (null != page.get(JsonKey.NAME) && ((String) page.get(JsonKey.NAME)).equals("anyName")) {
      pageName = true;
    }
    assertTrue(pageName);
  }

  @Test
  public void testGetPageSettingWithPortalMap() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    boolean pageName = false;
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_SETTING.getValue());
    reqObj.getRequest().put(JsonKey.ID, "Test Page");

    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordByProperty(JsonKey.PORTAL_MAP));

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Map<String, Object> result = response.getResult();
    Map<String, Object> page = (Map<String, Object>) result.get(JsonKey.PAGE);
    if (null != page.get(JsonKey.NAME) && ((String) page.get(JsonKey.NAME)).equals("anyName")) {
      pageName = true;
    }
    assertTrue(pageName);
  }

  @Test
  public void testGetPageSettingsSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    boolean pageName = false;
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_SETTINGS.getValue());
    reqObj.getRequest().put(JsonKey.ID, "Test Page");

    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordByProperty(JsonKey.PORTAL_MAP));

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Map<String, Object> result = response.getResult();

    assertTrue(result != null);
  }

  @Test
  public void testGetAllSectionsSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    boolean section = false;
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_ALL_SECTION.getValue());
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Map<String, Object> result = response.getResult();
    List<Map<String, Object>> sectionList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    for (Map<String, Object> sec : sectionList) {
      if (null != sec.get(JsonKey.SECTIONS)) {
        section = true;
      }
    }
    assertTrue(section);
  }

  @Test
  public void testGetSectionSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    boolean section = false;
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_SECTION.getValue());
    reqObj.getRequest().put(JsonKey.ID, sectionId);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    Map<String, Object> result = response.getResult();
    List<Map<String, Object>> sectionList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);

    for (Map<String, Object> sec : sectionList) {
      if (null != sec.get(JsonKey.SECTIONS)) {
        section = true;
      }
    }
    assertTrue(section);
  }

  @Test
  public void testUpdatePageSuccessWithPageName() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();

    pageMap.put(JsonKey.PAGE_NAME, "Test Page");
    pageMap.put(JsonKey.ID, pageId);
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(cassandraGetRecordByProperty(""));

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdatePageFailureWithPageName() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();

    pageMap.put(JsonKey.PAGE_NAME, "Test Page");
    pageMap.put(JsonKey.ID, "anyId");
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(cassandraGetRecordByProperty(""));

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void testDUpdatePageSuccessWithoutPageAndWithPortalMap() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();

    //        pageMap.put(JsonKey.PAGE_NAME, "Test Page");
    pageMap.put(JsonKey.ID, pageId);
    pageMap.put(JsonKey.PORTAL_MAP, "anyQuery");
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(cassandraGetRecordByProperty(JsonKey.PORTAL_MAP));

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testDUpdatePageSuccessWithoutPageAndWithAppMap() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();

    //        pageMap.put(JsonKey.PAGE_NAME, "Test Page");
    pageMap.put(JsonKey.ID, pageId);
    pageMap.put(JsonKey.APP_MAP, "anyQuery");
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    when(cassandraOperation.getRecordsByProperties(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(cassandraGetRecordByProperty(JsonKey.APP_MAP));

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdatePageSection() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Map<String, Object> filterMap = new HashMap<>();
    Map<String, Object> reqMap = new HashMap<>();
    Map<String, Object> searchQueryMap = new HashMap<>();
    List<String> list = new ArrayList<>();
    list.add("Bengali");
    filterMap.put("language", list);
    reqMap.put(JsonKey.FILTERS, filterMap);
    searchQueryMap.put(JsonKey.REQUEST, reqMap);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_SECTION.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> sectionMap = new HashMap<String, Object>();
    sectionMap.put(JsonKey.ID, sectionId2);
    sectionMap.put(JsonKey.SECTION_DISPLAY, "TOP1");
    sectionMap.put(JsonKey.SECTION_NAME, "Updated Test Section2");
    sectionMap.put(JsonKey.SEARCH_QUERY, searchQueryMap);
    sectionMap.put(JsonKey.SECTION_DATA_TYPE, "course");
    innerMap.put(JsonKey.SECTION, sectionMap);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testGetPageData() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Map<String, Object> header = new HashMap<>();
    header.put("Accept", "application/json");
    header.put("Content-Type", "application/json");
    Map<String, Object> filterMap = new HashMap<>();
    List<String> contentList = new ArrayList<>();
    contentList.add("Story");
    contentList.add("Worksheet");
    contentList.add("Collection");
    contentList.add("Game");
    contentList.add("Collection");
    contentList.add("Course");
    filterMap.put("contentType", contentList);

    List<String> statusList = new ArrayList<>();
    statusList.add("Live");
    filterMap.put("language", statusList);

    List<String> objectTypeList = new ArrayList<>();
    objectTypeList.add("content");
    filterMap.put("objectType", objectTypeList);

    List<String> languageList = new ArrayList<>();
    languageList.add("English");
    languageList.add("Hindi");
    languageList.add("Gujarati");
    languageList.add("Bengali");
    filterMap.put("language", languageList);
    Map<String, Object> compMap = new HashMap<>();
    compMap.put("<=", 2);
    compMap.put(">", 0.5);
    filterMap.put("size", compMap);
    filterMap.put("orthographic_complexity", 1);
    List<String> gradeListist = new ArrayList<>();
    gradeListist.add("Grade 1");
    filterMap.put("gradeLevel", gradeListist);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_DATA.getValue());
    reqObj.getRequest().put(JsonKey.SOURCE, "web");
    reqObj.getRequest().put(JsonKey.PAGE_NAME, "Test Page Name Updated");
    reqObj.getRequest().put(JsonKey.FILTERS, filterMap);
    HashMap<String, Object> map = new HashMap<>();
    map.put(JsonKey.PAGE, reqObj.getRequest());
    map.put(JsonKey.HEADER, header);
    reqObj.setRequest(map);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testGetPageDataWithOrgCode() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Map<String, Object> header = new HashMap<>();
    header.put("Accept", "application/json");
    header.put("Content-Type", "application/json");

    Map<String, Object> filterMap = new HashMap<>();
    List<String> list = new ArrayList<>();
    list.add("English");
    filterMap.put("language", list);
    Map<String, Object> compMap = new HashMap<>();
    compMap.put("<=", 2);
    compMap.put(">", 0.5);
    filterMap.put("size", compMap);
    filterMap.put("orthographic_complexity", 1);
    List<String> gradeListist = new ArrayList<>();
    gradeListist.add("Grade 1");
    filterMap.put("gradeLevel", gradeListist);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_DATA.getValue());
    reqObj.getRequest().put(JsonKey.SOURCE, "web");
    reqObj.getRequest().put(JsonKey.PAGE_NAME, "Test Page Name Updated");
    reqObj.getRequest().put(JsonKey.FILTERS, filterMap);
    reqObj.getRequest().put(JsonKey.ORG_CODE, "ORG1");
    HashMap<String, Object> map = new HashMap<>();
    map.put(JsonKey.PAGE, reqObj.getRequest());
    map.put(JsonKey.HEADER, header);
    reqObj.setRequest(map);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  private static Response getRecordByPropMap(boolean isValid) {
    Response response = new Response();
    List<Map> list = new ArrayList<>();
    Map<String, Object> map = new HashMap();
    map.put(JsonKey.ID, "anyID");
    if (isValid) list.add(map);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  /*private boolean testScenario(
          boolean isGetUserRoles, boolean isOrgIdReq, ResponseCode errorResponse) {

      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      if (isGetUserRoles) {

          Request reqObj = new Request();
          reqObj.setOperation(ActorOperations.GET_ROLES.getValue());
          subject.tell(reqObj, probe.getRef());
      } else {
          DecryptionService decryptionService = Mockito.mock(DecryptionService.class);
          when(decryptionService.decryptData(Mockito.anyMap())).thenReturn(getOrganisationsMap());
          when(interServiceCommunication.getResponse(Mockito.anyObject(), Mockito.anyObject()))
                  .thenReturn(response);
          if (errorResponse == null) {
              when(response.get(Mockito.anyString())).thenReturn(new HashMap<>());
              mockGetOrgResponse(true);
          } else {
              mockGetOrgResponse(false);
          }
          subject.tell(getRequestObj(isOrgIdReq), probe.getRef());
      }
      if (errorResponse == null) {
          Response res = probe.expectMsgClass(duration("100 second"), Response.class);
          return null != res && res.getResponseCode() == ResponseCode.OK;
      } else {
          ProjectCommonException res =
                  probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
          return res.getCode().equals(errorResponse.getErrorCode())
                  || res.getResponseCode() == errorResponse.getResponseCode();
      }
  }*/

  private Map<String, Object> getOrganisationsMap() {

    Map<String, Object> orgMap = new HashMap<>();
    List<Map<String, Object>> list = new ArrayList<>();
    orgMap.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    list.add(orgMap);
    orgMap.put(JsonKey.ORGANISATIONS, list);
    return orgMap;
  }

  private Map<String, Object> createResponseGet(boolean isResponseRequired) {
    HashMap<String, Object> response = new HashMap<>();
    List<Map<String, Object>> content = new ArrayList<>();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CONTACT_DETAILS, "CONTACT_DETAILS");
    innerMap.put(JsonKey.ID, "ORGANISATION_ID");
    innerMap.put(JsonKey.HASHTAGID, "HASHTAGID");
    HashMap<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    List<Map<String, Object>> orgList = new ArrayList<>();
    orgList.add(orgMap);
    innerMap.put(JsonKey.ORGANISATIONS, orgList);
    if (isResponseRequired) {
      content.add(innerMap);
    }
    response.put(JsonKey.CONTENT, content);
    return response;
  }

  private Object getRequestObj(boolean isOrgIdReq) {
    Request reqObj = new Request();
    List roleLst = new ArrayList();
    roleLst.add("anyRole");
    reqObj.put(JsonKey.ROLES, roleLst);
    reqObj.put(JsonKey.EXTERNAL_ID, "EXTERNAL_ID");
    reqObj.put(JsonKey.USER_ID, "USER_ID");
    reqObj.put(JsonKey.HASHTAGID, "HASHTAGID");
    reqObj.put(JsonKey.PROVIDER, "PROVIDER");
    if (isOrgIdReq) {
      reqObj.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    }
    reqObj.setOperation(ActorOperations.ASSIGN_ROLES.getValue());
    return reqObj;
  }

  private void mockGetOrgResponse(boolean isResponseRequired) {

    when(ElasticSearchUtil.complexSearch(
            Mockito.any(SearchDTO.class),
            Mockito.eq(ProjectUtil.EsIndex.sunbird.getIndexName()),
            Mockito.anyVararg()))
        .thenReturn(createResponseGet(isResponseRequired));
  }

  private static Response getCassandraResponse() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, "ORGANISATION_ID");
    list.add(orgMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Response cassandraGetRecordByProperty(String reqMap) {
    Response response = new Response();
    List list = new ArrayList();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.NAME, "anyName");
    map.put(JsonKey.ID, "anyID");
    if (!reqMap.equals("")) {
      map.put(reqMap, "anyQuery");
    }
    list.add(map);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }
}
