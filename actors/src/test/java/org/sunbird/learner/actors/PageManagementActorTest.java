/*package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

*//**
 * @author arvind.
 *//*
public class PageManagementActorTest {

    static ActorSystem system;
    final static Props props = Props.create(PageManagementActor.class);

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
    }

    @Test
    public void testInvalidOperation(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation("INVALID_OPERATION");

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }

    @Test
    public void testInvalidMessageType(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        subject.tell(new String("Invelid Type"), probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }

    @Test
    public void testCreatePage(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.CREATE_PAGE.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> pageMap = new HashMap<String , Object>();
        List<Map<String, Object>> appMapList = new ArrayList<Map<String, Object>>();
        Map<String , Object> appMap1 = new HashMap<String , Object>();
        appMap1.put(JsonKey.ID , "APP_SECTION_1");
        appMap1.put(JsonKey.POSITION , new BigInteger("10"));
        appMapList.add(appMap1);
        pageMap.put(JsonKey.APP_MAP , appMapList);

        List<Map<String, Object>> portalMapList = new ArrayList<Map<String, Object>>();
        Map<String , Object> portalMap1 = new HashMap<String , Object>();
        portalMap1.put(JsonKey.ID , "position_id1");
        portalMap1.put(JsonKey.POSITION , new BigInteger("10"));
        portalMapList.add(appMap1);
        pageMap.put(JsonKey.PORTAL_MAP , portalMapList);
        innerMap.put(JsonKey.PAGE , pageMap);
        reqObj.setRequest(innerMap);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test
    public void testGetSettings(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_PAGE_SETTINGS.getValue());
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test
    public void testCreatePageSection(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.CREATE_SECTION.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> sectionMap = new HashMap<String, Object>();
        sectionMap.put(JsonKey.SECTION_DISPLAY , "TOP");
        innerMap.put(JsonKey.SECTION , sectionMap);
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);

    }

    @Test
    public void testGetAllSections(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_ALL_SECTION.getValue());
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test
    public void testGetSection(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_SECTION.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.ID , "12");
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

    @Test
    public void testGetPageSetting(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_PAGE_SETTING.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.ID , "12");
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }

}
*/