package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;
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
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;

// @FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class PageManagementActorTest {

  private static final Props props = Props.create(PageManagementActor.class);
  private static final ActorSystem system = ActorSystem.create("system");

  private static final TestKit probe = new TestKit(system);
  ActorRef subject = system.actorOf(props);

  private static String sectionId = null;
  private static String sectionId2 = null;
  private static String pageId = "anyID";
  private static String pageIdWithOrg = null;
  private static String pageIdWithOrg2 = null;
  //  private static CassandraOperation cassandraOperation;
  private static CassandraOperationImpl cassandraOperation;

  @BeforeClass
  public static void setUp() {

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
  }

  @Before
  public void beforeTest() {
    PowerMockito.mockStatic(ServiceFactory.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);

    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getUpdateSuccess());
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordById());
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getUpdateSuccess());
    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(cassandraGetRecordById());
  }

  @Test
  public void testInvalidRequest() {

    Request reqObj = new Request();
    subject.tell(reqObj, probe.getRef());
    NullPointerException exc =
        probe.expectMsgClass(duration("100 second"), NullPointerException.class);
    assertTrue(null != exc);
  }

  @Test
  public void testInvalidOperationSuccess() {

    Request reqObj = new Request();
    reqObj.setOperation("INVALID_OPERATION");

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void testCreateSuccessPageWithOrgId() {

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

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void testCGetPageSetting() {

    boolean pageName = false;
    TestKit probe = new TestKit(system);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_SETTING.getValue());
    reqObj.getRequest().put(JsonKey.ID, "Test Page");
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    Map<String, Object> result = response.getResult();
    Map<String, Object> page = (Map<String, Object>) result.get(JsonKey.PAGE);
    if (null != page.get(JsonKey.PAGE_NAME)
        && ((String) page.get(JsonKey.PAGE_NAME)).equals("Test Page")) {
      pageName = true;
    }
    assertTrue(pageName);
  }

  @Test
  public void testCGetAllPageSettings() {

    boolean pageName = false;
    TestKit probe = new TestKit(system);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_SETTINGS.getValue());
    reqObj.getRequest().put(JsonKey.ID, "Test Page");
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    Map<String, Object> result = response.getResult();
    List<Map<String, Object>> pageList = (List<Map<String, Object>>) result.get(JsonKey.PAGE);
    for (Map<String, Object> page : pageList) {
      if (null != page.get(JsonKey.PAGE_NAME)
          && ((String) page.get(JsonKey.PAGE_NAME)).equals("Test Page")) {
        pageName = true;
      }
    }
    assertTrue(pageName);
  }

  @Test
  public void testCGetAllSections() {

    boolean section = false;
    TestKit probe = new TestKit(system);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_ALL_SECTION.getValue());
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    Map<String, Object> result = response.getResult();
    List<Map<String, Object>> sectionList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    for (Map<String, Object> sec : sectionList) {
      if (null != sec.get(JsonKey.SECTION_NAME)
          && ((String) sec.get(JsonKey.SECTION_NAME)).equals("Test Section")) {
        section = true;
      }
    }
    assertTrue(section);
  }

  @Test
  public void testCGetSection() {

    boolean section = false;
    TestKit probe = new TestKit(system);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_SECTION.getValue());
    reqObj.getRequest().put(JsonKey.ID, sectionId);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    Map<String, Object> result = response.getResult();
    List<Map<String, Object>> sectionList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    for (Map<String, Object> sec : sectionList) {
      if (null != sec.get(JsonKey.SECTION_NAME)
          && ((String) sec.get(JsonKey.SECTION_NAME)).equals("Test Section")) {
        section = true;
      }
    }
    assertTrue(section);
  }

  @Test
  public void testCUpdatePageWithSameName() {

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();

    pageMap.put(JsonKey.PAGE_NAME, "Test Page");
    pageMap.put(JsonKey.ID, pageId);
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testCUpdatePageWithOrgIdWithSameName() {

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();

    pageMap.put(JsonKey.PAGE_NAME, "Test Page");
    pageMap.put(JsonKey.ID, pageIdWithOrg2);
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void testDUpdatePage() {

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> pageMap = new HashMap<String, Object>();
    List<Map<String, Object>> appMapList = new ArrayList<Map<String, Object>>();
    Map<String, Object> appMap1 = new HashMap<String, Object>();
    appMap1.put(JsonKey.ID, sectionId);
    appMap1.put(JsonKey.INDEX, new BigInteger("1"));
    appMap1.put(JsonKey.GROUP, new BigInteger("1"));
    appMapList.add(appMap1);

    Map<String, Object> appMap2 = new HashMap<String, Object>();
    appMap2.put(JsonKey.ID, sectionId2);
    appMap2.put(JsonKey.INDEX, new BigInteger("1"));
    appMap2.put(JsonKey.GROUP, new BigInteger("1"));
    appMapList.add(appMap2);

    pageMap.put(JsonKey.APP_MAP, appMapList);

    List<Map<String, Object>> portalMapList = new ArrayList<Map<String, Object>>();
    Map<String, Object> portalMap1 = new HashMap<String, Object>();
    portalMap1.put(JsonKey.ID, sectionId);
    portalMap1.put(JsonKey.INDEX, new BigInteger("1"));
    portalMap1.put(JsonKey.GROUP, new BigInteger("1"));
    portalMapList.add(portalMap1);

    Map<String, Object> portalMap2 = new HashMap<String, Object>();
    portalMap2.put(JsonKey.ID, sectionId2);
    portalMap2.put(JsonKey.INDEX, new BigInteger("1"));
    portalMap2.put(JsonKey.GROUP, new BigInteger("1"));
    portalMapList.add(portalMap2);

    pageMap.put(JsonKey.PORTAL_MAP, portalMapList);
    pageMap.put(JsonKey.ID, "anyID");
    pageMap.put(JsonKey.PAGE_NAME, "Test Page Name Updated");
    pageMap.put(JsonKey.ID, pageId);
    innerMap.put(JsonKey.PAGE, pageMap);
    reqObj.setRequest(innerMap);

    //    when(DataCacheHandler.getPageMap()).thenReturn(new HashMap<>());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("100 second"), Response.class);
    assertEquals(res.get(JsonKey.RESPONSE), JsonKey.SUCCESS);
  }

  @Test
  public void testEUpdatedPage() {

    boolean pageName = false;
    TestKit probe = new TestKit(system);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PAGE_SETTING.getValue());
    reqObj.getRequest().put(JsonKey.ID, "Test Page Name Updated");
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    Map<String, Object> result = response.getResult();
    Map<String, Object> page = (Map<String, Object>) result.get(JsonKey.PAGE);
    if (null != page.get(JsonKey.PAGE_NAME)
        && ((String) page.get(JsonKey.PAGE_NAME)).equals("Test Page Name Updated")) {
      List<Map<String, Object>> portalList =
          (List<Map<String, Object>>) page.get(JsonKey.PORTAL_SECTIONS);
      if (portalList.size() == 2) {
        pageName = true;
      }
    }
    assertTrue(pageName);
  }

  @Test
  public void testFUpdatePageSection() {

    Map<String, Object> filterMap = new HashMap<>();
    Map<String, Object> reqMap = new HashMap<>();
    Map<String, Object> searchQueryMap = new HashMap<>();
    List<String> list = new ArrayList<>();
    list.add("Bengali");
    filterMap.put("language", list);
    reqMap.put(JsonKey.FILTERS, filterMap);
    searchQueryMap.put(JsonKey.REQUEST, reqMap);

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
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
  public void testGUpdatedSection() {

    boolean section = false;
    TestKit probe = new TestKit(system);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_SECTION.getValue());
    reqObj.getRequest().put(JsonKey.ID, sectionId2);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(Response.class);
    Map<String, Object> result = response.getResult();
    List<Map<String, Object>> sectionList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    for (Map<String, Object> sec : sectionList) {
      if (null != sec.get(JsonKey.SECTION_NAME)
          && ((String) sec.get(JsonKey.SECTION_NAME)).equals("Updated Test Section2")) {
        section = true;
      }
    }
    assertTrue(section);
  }

  @Test
  public void testHGetPageData() {

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

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
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
  public void testHGetPageDataWithOrgCode() {

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

    TestKit probe = new TestKit(system);
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

  private static Response cassandraGetRecordById() {
    Response response = new Response();
    List list = new ArrayList();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.NAME, "anyName");
    map.put(JsonKey.ID, "anyId");
    list.add(map);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private static Response getUpdateSuccess() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
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
}
