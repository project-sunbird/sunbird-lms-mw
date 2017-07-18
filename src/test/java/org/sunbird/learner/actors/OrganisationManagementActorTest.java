package org.sunbird.learner.actors;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;

/**
 * @author arvind.
 */
public class OrganisationManagementActorTest {


    static ActorSystem system;
    final static Props props = Props.create(OrganisationManagementActor.class);
    final static Props propsUser = Props.create(UserManagementActor.class);
    public static String orgId = "";
    public static String usrId = "123";//TODO:change while committing
    public static String parentOrgId = "";
    public final static String source = "Test";
    public final static String externalId = "test123";
    
    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        //OrganisationManagementActorTest.createOrgForId();
       //OrganisationManagementActorTest.createUserForId();
    }
    
    public static void createUserForId() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(propsUser);

      Request reqObj = new Request();
      reqObj.setRequest_id("1");
      reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.USERNAME, "test1buser");
      innerMap.put(JsonKey.EMAIL, "test1buser@xyzab.com");
      innerMap.put(JsonKey.PASSWORD , "password");
      Map<String , Object> request = new HashMap<String , Object>();
      request.put(JsonKey.USER , innerMap);
      reqObj.setRequest(request);

      subject.tell(reqObj, probe.getRef());
      Response res =  probe.expectMsgClass(Response.class);
      usrId = (String)res.getResult().get(JsonKey.USER_ID);
    }
    
    public static void createOrgForId(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put(JsonKey.ORG_CODE, "CBSE");
      orgMap.put(JsonKey.SOURCE, source);
      orgMap.put(JsonKey.EXTERNAL_ID, externalId);
      orgMap.put(JsonKey.CHANNEL, "test");
      Map<String,Object> address = new HashMap<String,Object>();
      address.put(JsonKey.CITY, "Hyderabad");
      address.put("state", "Andra Pradesh");
      address.put("country", "India");
      address.put("zipCode", "466899");
      innerMap.put(JsonKey.ADDRESS, address);
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      Response resp = probe.expectMsgClass(Response.class);
      orgId = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
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
    public void testCreateOrgWithoutSourceAndExternalIdSuc(){
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
      Response resp = probe.expectMsgClass(Response.class);
      String id = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
      Assert.assertNotNull(id);
    }
    
    //@Test
    public void testCreateOrgWithSourceAndExternalIdSuc(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put("orgCode", "CBSE");
      orgMap.put("source", "test123");
      orgMap.put("externalId", "123456");
      orgMap.put("channel", "test");
      Map<String,Object> address = new HashMap<String,Object>();
      address.put("city", "Hyderabad");
      address.put("state", "Andra Pradesh");
      address.put("country", "India");
      address.put("zipCode", "466899");
      innerMap.put("address", address);
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      Response resp = probe.expectMsgClass(Response.class);
      String id = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
      Assert.assertNotNull(id);
    }
    
    //@Test
    public void testCreateOrgWithSameSourceAndExternalIdExc(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put("orgCode", "CBSE");
      orgMap.put("source", source);
      orgMap.put("externalId", externalId);
      orgMap.put("channel", "test");
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    @Test
    public void testCreateOrgWithBlankSourceAndExternalIdExc(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put("orgCode", "CBSE");
      orgMap.put("source", null);
      orgMap.put("externalId", null);
      orgMap.put("channel", "test");
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    
    @Test
    public void testCreateOrgRootWithoutChannelExc(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "AP Board");
      orgMap.put(JsonKey.DESCRIPTION, "AndhraPradesh Board");
      orgMap.put(JsonKey.ORG_TYPE, "Training");
      orgMap.put(JsonKey.CHANNEL, null);
      orgMap.put("preferredLanguage", "English");
      orgMap.put("homeUrl", "https:testUrl");
      orgMap.put(JsonKey.ORG_CODE, "AP");
      orgMap.put(JsonKey.IS_ROOT_ORG, true);
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    @Test
    public void testCreateOrgInvalidParentIdExc(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "Tamil Nadu ");
      orgMap.put(JsonKey.DESCRIPTION, "Tamil Nadu Board");
      orgMap.put(JsonKey.PARENT_ORG_ID, "CBSE");
      orgMap.put(JsonKey.ORG_TYPE, "Training");
      orgMap.put("imgUrl", "https://testimgUrl");
      orgMap.put(JsonKey.CHANNEL, "Ekstep");
      orgMap.put("preferredLanguage", "Tamil");
      orgMap.put("homeUrl", "https:testUrl");
      orgMap.put(JsonKey.ORG_CODE, "TN");
      orgMap.put(JsonKey.IS_ROOT_ORG, false);
      Map<String,Object> address = new HashMap<String,Object>();
      address.put(JsonKey.CITY, "Chennai");
      address.put("state", "Tamil Nadu");
      address.put("country", "India");
      address.put("zipCode", "466879");
      orgMap.put(JsonKey.ADDRESS, address);
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
   // @Test
    public void testApproveOrgSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.APPROVE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , orgId);
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(Response.class);
    }

    
    @Test
    public void testApproveOrgExc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.APPROVE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , null);
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testUpdateStatusSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , orgId);
      orgMap.put(JsonKey.STATUS, "retired");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(Response.class);
    }
    
    @Test
    public void testUpdateStatusEx(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.SOURCE , source);
      orgMap.put(JsonKey.EXTERNAL_ID, externalId);
      orgMap.put(JsonKey.STATUS, "Test");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    @Test
    public void testUpdateOrgExc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put("imgUrl", "test");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testUpdateOrgSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , orgId);
      orgMap.put("imgUrl", "test");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(Response.class);
    }
    
    @Test
    public void testGetOrgSuc() {
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_ORG_DETAILS.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String, Object> orgMap = new HashMap<String, Object>();
        orgMap.put(JsonKey.ORGANISATION_ID, orgId);
        innerMap.put(JsonKey.ORGANISATION, orgMap);

        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);
    }
    
    @Test
    public void testGetOrgExc() {
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_ORG_DETAILS.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String, Object> orgMap = new HashMap<String, Object>();
        orgMap.put(JsonKey.ORGANISATION_ID, null);
        innerMap.put(JsonKey.ORGANISATION, orgMap);
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testAddMemberToOrgExc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
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
    
    //@Test
    public void testAddMemberToOrgSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.ADD_MEMBER_ORGANISATION.getValue());
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
    
    //@Test
    public void testRemoveMemberFromOrgSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testRemoveMemberFromOrgExc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
   
    //@Test
    public void testJoinMemberOrgSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testJoinMemberOrgExc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testApproveMemberOrgSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testApproveMemberFromOrgExc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testRejectMemberOrgSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    //@Test
    public void testRejectMemberOrgExc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , "");
      orgMap.put(JsonKey.USER_ID, "");
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
    
    public void deleteOrgBySourceAndExternalId(){
      
    }
}