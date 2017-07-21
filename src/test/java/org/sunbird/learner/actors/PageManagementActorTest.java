package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Amit Kumar.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PageManagementActorTest {

    static ActorSystem system;
    static CassandraOperation operation= new CassandraOperationImpl();
    static PropertiesCache cach = PropertiesCache.getInstance();
    final static Props props = Props.create(PageManagementActor.class);
    static Util.DbInfo pageMgmntDbInfo = null;
    static Util.DbInfo pageSectionDbInfo = null;
    static String sectionId = "";
    static String sectionId2 = "";
    static String pageId = "";
    static String pageIdWithOrg = "";
    
    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        pageMgmntDbInfo = Util.dbInfoMap.get(JsonKey.PAGE_MGMT_DB);
        pageSectionDbInfo = Util.dbInfoMap.get(JsonKey.PAGE_SECTION_DB);
    }

    @Test
    public void testAAInvalidOperation(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation("INVALID_OPERATION");

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }

    @Test
    public void testAAInvalidMessageType(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        subject.tell(new String("Invelid Type"), probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }
    
    @Test
    public void testACreatePageSection(){
      //create search query for section
      Map<String,Object> filterMap = new HashMap<>();
      Map<String,Object> reqMap = new HashMap<>();
      Map<String,Object> searchQueryMap = new HashMap<>();
      
      List<String> list = new ArrayList<>();
      list.add("Bengali");
      filterMap.put("language", list);
      Map<String,Object> sizeMap = new HashMap<>();
      sizeMap.put("<=", "1000000");
      filterMap.put("size",sizeMap);
      
      List<String> contentTypeList = new ArrayList<>();
      contentTypeList.add("Course");
      filterMap.put("contentType", contentTypeList);
      
      List<String> objectTypeList = new ArrayList<>();
      objectTypeList.add("content");
      filterMap.put("objectType", objectTypeList);
      
      reqMap.put(JsonKey.FILTERS, filterMap);
      searchQueryMap.put(JsonKey.REQUEST, reqMap);
      
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_SECTION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> sectionMap = new HashMap<String, Object>();
      sectionMap.put(JsonKey.SECTION_DISPLAY , "TOP");
      sectionMap.put(JsonKey.SECTION_NAME, "Test Section");
      sectionMap.put(JsonKey.SEARCH_QUERY, searchQueryMap);
      sectionMap.put(JsonKey.SECTION_DATA_TYPE, "course");
      innerMap.put(JsonKey.SECTION , sectionMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      Response response = probe.expectMsgClass(Response.class);
      sectionId = (String) response.get(JsonKey.SECTION_ID);
    }
    
    @Test
    public void testACreatePageSection2(){
      //create search query for section
      Map<String,Object> filterMap = new HashMap<>();
      Map<String,Object> reqMap = new HashMap<>();
      Map<String,Object> searchQueryMap = new HashMap<>();
      List<String> list = new ArrayList<>();
      list.add("Bengali");
      filterMap.put("language", list);
      Map<String,Object> sizeMap = new HashMap<>();
      sizeMap.put("<=", "1000000");
      //"size": {"<=" : "1000000"}
      filterMap.put("size",sizeMap);
      reqMap.put(JsonKey.FILTERS, filterMap);
      searchQueryMap.put(JsonKey.REQUEST, reqMap);
      
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_SECTION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> sectionMap = new HashMap<String, Object>();
      sectionMap.put(JsonKey.SECTION_DISPLAY , "TOP1");
      sectionMap.put(JsonKey.SECTION_NAME, "Test Section2");
      sectionMap.put(JsonKey.SEARCH_QUERY, searchQueryMap);
      sectionMap.put(JsonKey.SECTION_DATA_TYPE, "course");
      innerMap.put(JsonKey.SECTION , sectionMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      Response response = probe.expectMsgClass(Response.class);
      sectionId2 = (String) response.get(JsonKey.SECTION_ID);
    }

    @Test
    public void testBCreatePageWithOrgId(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.CREATE_PAGE.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> pageMap = new HashMap<String , Object>();
        List<Map<String, Object>> appMapList = new ArrayList<Map<String, Object>>();
        Map<String , Object> appMap = new HashMap<String , Object>();
        appMap.put(JsonKey.ID , sectionId);
        appMap.put(JsonKey.INDEX , new BigInteger("1"));
        appMap.put(JsonKey.GROUP , new BigInteger("1"));
        appMapList.add(appMap);
        
        pageMap.put(JsonKey.APP_MAP , appMapList);

        List<Map<String, Object>> portalMapList = new ArrayList<Map<String, Object>>();
        Map<String , Object> portalMap = new HashMap<String , Object>();
        portalMap.put(JsonKey.ID , sectionId);
        portalMap.put(JsonKey.INDEX , new BigInteger("1"));
        portalMap.put(JsonKey.GROUP , new BigInteger("1"));
        portalMapList.add(portalMap);
        
        pageMap.put(JsonKey.PORTAL_MAP , portalMapList);
        
        pageMap.put(JsonKey.PAGE_NAME, "Test Page");
        pageMap.put(JsonKey.ORGANISATION_ID, "ORG1");
        innerMap.put(JsonKey.PAGE , pageMap);
        reqObj.setRequest(innerMap);

        subject.tell(reqObj, probe.getRef());
        Response response = probe.expectMsgClass(Response.class);
        pageIdWithOrg = (String) response.get(JsonKey.PAGE_ID);
    }

    @Test
    public void testBCreatePage(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.CREATE_PAGE.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> pageMap = new HashMap<String , Object>();
        List<Map<String, Object>> appMapList = new ArrayList<Map<String, Object>>();
        Map<String , Object> appMap = new HashMap<String , Object>();
        appMap.put(JsonKey.ID , sectionId);
        appMap.put(JsonKey.INDEX , new BigInteger("1"));
        appMap.put(JsonKey.GROUP , new BigInteger("1"));
        appMapList.add(appMap);
        
        pageMap.put(JsonKey.APP_MAP , appMapList);

        List<Map<String, Object>> portalMapList = new ArrayList<Map<String, Object>>();
        Map<String , Object> portalMap = new HashMap<String , Object>();
        portalMap.put(JsonKey.ID , sectionId);
        portalMap.put(JsonKey.INDEX , new BigInteger("1"));
        portalMap.put(JsonKey.GROUP , new BigInteger("1"));
        portalMapList.add(portalMap);
        
        pageMap.put(JsonKey.PORTAL_MAP , portalMapList);
        
        pageMap.put(JsonKey.PAGE_NAME, "Test Page");
        innerMap.put(JsonKey.PAGE , pageMap);
        reqObj.setRequest(innerMap);

        subject.tell(reqObj, probe.getRef());
        Response response = probe.expectMsgClass(Response.class);
        pageId = (String) response.get(JsonKey.PAGE_ID);
    }
    
    @Test
    public void testCGetPageSetting(){
        boolean pageName = false;
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_PAGE_SETTING.getValue());
        reqObj.getRequest().put(JsonKey.ID, "Test Page");
        subject.tell(reqObj, probe.getRef());
        Response response = probe.expectMsgClass(Response.class);
        Map<String,Object> result = response.getResult();
        Map<String,Object> page = (Map<String, Object>) result.get(JsonKey.PAGE);
          if(null != page.get(JsonKey.PAGE_NAME) && ((String)page.get(JsonKey.PAGE_NAME)).equals("Test Page")){
            pageName = true;
          }
        assertTrue(pageName);
    }
    
    @Test
    public void testCGetAllPageSettings(){
        boolean pageName = false;
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_PAGE_SETTINGS.getValue());
        reqObj.getRequest().put(JsonKey.ID, "Test Page");
        subject.tell(reqObj, probe.getRef());
        Response response = probe.expectMsgClass(Response.class);
        Map<String,Object> result = response.getResult();
        List<Map<String,Object>> pageList = (List<Map<String, Object>>) result.get(JsonKey.PAGE);
        for(Map<String,Object> page : pageList){
          if(null != page.get(JsonKey.PAGE_NAME) && ((String)page.get(JsonKey.PAGE_NAME)).equals("Test Page")){
            pageName = true;
          }
        }
        assertTrue(pageName);
    }

    @Test
    public void testCGetAllSections(){
        boolean section = false;
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_ALL_SECTION.getValue());
        subject.tell(reqObj, probe.getRef());
        Response response = probe.expectMsgClass(Response.class);
        Map<String,Object> result = response.getResult();
        List<Map<String,Object>> sectionList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        for(Map<String,Object> sec : sectionList){
          if(null != sec.get(JsonKey.SECTION_NAME) && ((String)sec.get(JsonKey.SECTION_NAME)).equals("Test Section")){
            section = true;
          }
        }
        assertTrue(section);
    }

    @Test
    public void testCGetSection(){
      boolean section = false;
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_SECTION.getValue());
        reqObj.getRequest().put(JsonKey.ID, sectionId);
        subject.tell(reqObj, probe.getRef());
        Response response = probe.expectMsgClass(Response.class);
        Map<String,Object> result = response.getResult();
        List<Map<String,Object>> sectionList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        for(Map<String,Object> sec : sectionList){
          if(null != sec.get(JsonKey.SECTION_NAME) && ((String)sec.get(JsonKey.SECTION_NAME)).equals("Test Section")){
            section = true;
          }
        }
        assertTrue(section);
    }

    @Test
    public void testCUpdatePageWithSameName(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> pageMap = new HashMap<String , Object>();
        
        pageMap.put(JsonKey.PAGE_NAME, "Test Page");
        pageMap.put(JsonKey.ID, pageId);
        innerMap.put(JsonKey.PAGE , pageMap);
        reqObj.setRequest(innerMap);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("100 second"),Response.class);
    }
    
    @Test
    public void testDUpdatePage(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_PAGE.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> pageMap = new HashMap<String , Object>();
        List<Map<String, Object>> appMapList = new ArrayList<Map<String, Object>>();
        Map<String , Object> appMap1 = new HashMap<String , Object>();
        appMap1.put(JsonKey.ID , sectionId);
        appMap1.put(JsonKey.INDEX , new BigInteger("1"));
        appMap1.put(JsonKey.GROUP , new BigInteger("1"));
        appMapList.add(appMap1);
        
        Map<String , Object> appMap2 = new HashMap<String , Object>();
        appMap2.put(JsonKey.ID , sectionId2);
        appMap2.put(JsonKey.INDEX , new BigInteger("1"));
        appMap2.put(JsonKey.GROUP , new BigInteger("1"));
        appMapList.add(appMap2);
        
        pageMap.put(JsonKey.APP_MAP , appMapList);

        List<Map<String, Object>> portalMapList = new ArrayList<Map<String, Object>>();
        Map<String , Object> portalMap1 = new HashMap<String , Object>();
        portalMap1.put(JsonKey.ID , sectionId);
        portalMap1.put(JsonKey.INDEX , new BigInteger("1"));
        portalMap1.put(JsonKey.GROUP , new BigInteger("1"));
        portalMapList.add(portalMap1);
        
        Map<String , Object> portalMap2 = new HashMap<String , Object>();
        portalMap2.put(JsonKey.ID , sectionId2);
        portalMap2.put(JsonKey.INDEX , new BigInteger("1"));
        portalMap2.put(JsonKey.GROUP , new BigInteger("1"));
        portalMapList.add(portalMap2);
        
        pageMap.put(JsonKey.PORTAL_MAP , portalMapList);
        
        pageMap.put(JsonKey.PAGE_NAME, "Test Page Name Updated");
        pageMap.put(JsonKey.ID, pageId);
        innerMap.put(JsonKey.PAGE , pageMap);
        reqObj.setRequest(innerMap);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }
    
    @Test
    public void testEUpdatedPage(){
        boolean pageName = false;
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_PAGE_SETTING.getValue());
        reqObj.getRequest().put(JsonKey.ID, "Test Page Name Updated");
        subject.tell(reqObj, probe.getRef());
        Response response = probe.expectMsgClass(Response.class);
        Map<String,Object> result = response.getResult();
        Map<String,Object> page = (Map<String, Object>) result.get(JsonKey.PAGE);
          if(null != page.get(JsonKey.PAGE_NAME) && ((String)page.get(JsonKey.PAGE_NAME)).equals("Test Page Name Updated")){
            List<Map<String,Object>> portalList = (List<Map<String, Object>>) page.get(JsonKey.PORTAL_SECTIONS);
            if(portalList.size() == 2){
              pageName = true;
            }
          }
        assertTrue(pageName);
    }
    
    @Test
    public void testFUpdatePageSection(){
      //create search query for section
      Map<String,Object> filterMap = new HashMap<>();
      Map<String,Object> reqMap = new HashMap<>();
      Map<String,Object> searchQueryMap = new HashMap<>();
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
      Map<String , Object> sectionMap = new HashMap<String, Object>();
      sectionMap.put(JsonKey.ID , sectionId2);
      sectionMap.put(JsonKey.SECTION_DISPLAY , "TOP1");
      sectionMap.put(JsonKey.SECTION_NAME, "Updated Test Section2");
      sectionMap.put(JsonKey.SEARCH_QUERY, searchQueryMap);
      sectionMap.put(JsonKey.SECTION_DATA_TYPE, "course");
      innerMap.put(JsonKey.SECTION , sectionMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(Response.class);
    }

    @Test
    public void testGUpdatedSection(){
      boolean section = false;
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_SECTION.getValue());
        reqObj.getRequest().put(JsonKey.ID, sectionId2);
        subject.tell(reqObj, probe.getRef());
        Response response = probe.expectMsgClass(Response.class);
        Map<String,Object> result = response.getResult();
        List<Map<String,Object>> sectionList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        for(Map<String,Object> sec : sectionList){
          if(null != sec.get(JsonKey.SECTION_NAME) && ((String)sec.get(JsonKey.SECTION_NAME)).equals("Updated Test Section2")){
            section = true;
          }
        }
        assertTrue(section);
    }
    
    @Test
    public void testHGetPageData(){
      Map<String,Object> header = new HashMap<>();
      header.put("Accept", "application/json");
      header.put("Content-Type", "application/json");
      Map<String,Object> filterMap = new HashMap<>();
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
      Map<String,Object> compMap = new HashMap<>();
      compMap.put("<=", 2);
      compMap.put(">", 0.5);
      filterMap.put("size",compMap);
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
      probe.expectMsgClass(duration("100 second"),Response.class);
    }
    
    @Test
    public void testHGetPageDataWithOrgCode(){
      Map<String,Object> header = new HashMap<>();
      header.put("Accept", "application/json");
      header.put("Content-Type", "application/json");
      
      Map<String,Object> filterMap = new HashMap<>();
      List<String> list = new ArrayList<>();
      list.add("English");
      filterMap.put("language", list);
      Map<String,Object> compMap = new HashMap<>();
      compMap.put("<=", 2);
      compMap.put(">", 0.5);
      filterMap.put("size",compMap);
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
      reqObj.getRequest().put(JsonKey.ORG_CODE,"ORG1");
      HashMap<String, Object> map = new HashMap<>();
      map.put(JsonKey.PAGE, reqObj.getRequest());
      map.put(JsonKey.HEADER, header);
      reqObj.setRequest(map);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("100 second"),Response.class);
    }
    
    
    @AfterClass
    public static void deletePageAndSection() {
      Response response=operation.deleteRecord(pageMgmntDbInfo.getKeySpace(), pageMgmntDbInfo.getTableName(), pageId);
      assertEquals("SUCCESS", response.get("response"));
      Response response1=operation.deleteRecord(pageMgmntDbInfo.getKeySpace(), pageMgmntDbInfo.getTableName(), pageIdWithOrg);
      assertEquals("SUCCESS", response1.get("response"));
      Response response2=operation.deleteRecord(pageSectionDbInfo.getKeySpace(), pageSectionDbInfo.getTableName(), sectionId);
      assertEquals("SUCCESS", response2.get("response"));
      Response response3=operation.deleteRecord(pageSectionDbInfo.getKeySpace(), pageSectionDbInfo.getTableName(), sectionId2);
      assertEquals("SUCCESS", response3.get("response"));
    }
}
