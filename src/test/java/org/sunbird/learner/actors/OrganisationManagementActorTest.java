package org.sunbird.learner.actors;

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

import java.util.HashMap;
import java.util.Map;

/**
 * @author arvind.
 */
public class OrganisationManagementActorTest {


    static ActorSystem system;
    final static Props props = Props.create(OrganisationManagementActor.class);
    public static String orgId = "";
    public static String usrId = "";
    public static String parentOrgId = "";
    //TODO: Complete the test cases based on scenarios
    
    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
    }

    //@Test
    public void testGetOrg() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_ORG_DETAILS.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String, Object> orgMap = new HashMap<String, Object>();
        orgMap.put(JsonKey.ORGANISATION_ID, "CBSE");
        innerMap.put(JsonKey.ORGANISATION, orgMap);

        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

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

        subject.tell(new String("Invalid Type"), probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }
    
    @Test
    public void testCreateOrg(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put("orgCode", "CBSE");
      orgMap.put("isRootOrg", true);
      orgMap.put("channel", "test");
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(Response.class);
    }
    
    @Test
    public void testCreateOrg1Exception(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "AP Board");
      orgMap.put(JsonKey.DESCRIPTION, "AndhraPradesh Board");
      orgMap.put("parentOrgId", "");
      orgMap.put("orgType", "Training");
      orgMap.put("imgUrl", "https://testimgUrl");
      orgMap.put("channel", "Ekstep");
      orgMap.put("preferredLanguage", "English");
      orgMap.put("homeUrl", "https:testUrl");
      orgMap.put("orgCode", "AP");
      orgMap.put("isRootOrg", false);
      Map<String,Object> address = new HashMap<String,Object>();
      address.put("city", "Hyderabad");
      address.put("state", "Andra Pradesh");
      address.put("country", "India");
      address.put("zipCode", "466899");
      orgMap.put("address", address);
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    @Test
    public void testCreateOrg2Exception(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "Tamil Nadu ");
      orgMap.put(JsonKey.DESCRIPTION, "Tamil Nadu Board");
      orgMap.put("parentOrgId", "CBSE");
      orgMap.put("orgType", "Training");
      orgMap.put("imgUrl", "https://testimgUrl");
      orgMap.put("channel", "Ekstep");
      orgMap.put("preferredLanguage", "Tamil");
      orgMap.put("homeUrl", "https:testUrl");
      orgMap.put("orgCode", "TN");
      orgMap.put("isRootOrg", false);
      Map<String,Object> address = new HashMap<String,Object>();
      address.put("city", "Chennai");
      address.put("state", "Tamil Nadu");
      address.put("country", "India");
      address.put("zipCode", "466879");
      orgMap.put("address", address);
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    public void testApproveOrgException(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.APPROVE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.IS_APPROVED, true);
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    public void testUpdateStatus(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.APPROVE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.IS_APPROVED, true);
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    public void testUpdateOrgException(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.IS_APPROVED, true);
      orgMap.put("orgType", "Training");
      orgMap.put("status", "blocked");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    public void testAddMemberToOrgException(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      orgMap.put(JsonKey.ROLE,"");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    public void testRemoveMemberFromOrgException(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
   
}