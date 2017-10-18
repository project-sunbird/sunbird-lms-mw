package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.OrgStatus;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;

/**
 * @author arvind.
 */
@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrganisationManagementActorTest {


    static ActorSystem system;
    static CassandraOperation operation = ServiceFactory.getInstance();
    final static Props props = Props.create(OrganisationManagementActor.class);
    final static Props propsUser = Props.create(UserManagementActor.class);
    static Util.DbInfo orgTypeDbInfo = null;
    static Util.DbInfo userManagementDB = null;
    static Util.DbInfo addressDB = null;
    static Util.DbInfo orgDB = null;
    private static String orgTypeId1 = "";
    private static String orgTypeId2 = "";
    public static String orgId = "";
    public static String addressId = "";
    public static String usrId = "123";//TODO:change while committing
    public static String OrgIDWithoutSourceAndExternalId = "";
    public static String OrgIdWithSourceAndExternalId = "";
    public static String parentOrgId = "";
    public final static String source = "Test";
    public final static String externalId = "test123";
    
    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        userManagementDB = Util.dbInfoMap.get(JsonKey.USER_DB);
        addressDB = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        orgTypeDbInfo = Util.dbInfoMap.get(JsonKey.ORG_TYPE_DB);
        orgDB = Util.dbInfoMap.get(JsonKey.ORG_DB);
    }
    //@Test
    public void test10createUserForId() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(propsUser);

      Request reqObj = new Request();
      reqObj.setRequest_id("1");
      reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.USERNAME, "test04buser");
      innerMap.put(JsonKey.EMAIL, "test04buser@xyzab.com");
      innerMap.put(JsonKey.PASSWORD , "password");
      Map<String , Object> request = new HashMap<String , Object>();
      request.put(JsonKey.USER , innerMap);
      reqObj.setRequest(request);

      subject.tell(reqObj, probe.getRef());
      Response res =  probe.expectMsgClass(Response.class);
      usrId = (String)res.get(JsonKey.USER_ID);
    }
    @Test
    public void test11createOrgForId(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put(JsonKey.ORG_CODE, "CBSE");
      orgMap.put(JsonKey.PROVIDER, source);
      orgMap.put(JsonKey.EXTERNAL_ID, externalId);
      //orgMap.put(JsonKey.CHANNEL, "test");
      Map<String,Object> address = new HashMap<String,Object>();
      address.put(JsonKey.CITY, "Hyderabad");
      address.put("state", "Andra Pradesh");
      address.put("country", "India");
      address.put("zipCode", "466899");
      innerMap.put(JsonKey.ADDRESS, address);
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      Response resp = probe.expectMsgClass(duration("200 second"),Response.class);
      orgId = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
      System.out.println("orgId : "+orgId);
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
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
    public void test12testCreateOrgWithoutSourceAndExternalIdSuc(){
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
      orgMap.put("channel", "test1");
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      Response resp = probe.expectMsgClass(duration("200 second"),Response.class);
      OrgIDWithoutSourceAndExternalId = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
      System.out.println("OrgIDWithoutSourceAndExternalId : "+OrgIDWithoutSourceAndExternalId);
      Assert.assertNotNull(OrgIDWithoutSourceAndExternalId);
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    @Test
    public void test13CreateOrgWithSourceAndExternalIdSuc(){
      
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put("orgCode", "CBSE");
      orgMap.put(JsonKey.PROVIDER, "pr100000001");
      orgMap.put(JsonKey.EXTERNAL_ID, "ex100000001");
     // orgMap.put("channel", "test1");
      Map<String,Object> address = new HashMap<String,Object>();
      address.put("city", "Hyderabad");
      address.put("state", "Andra Pradesh");
      address.put("country", "India");
      address.put("zipCode", "466899");
      innerMap.put("address", address);
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      Response resp = probe.expectMsgClass(duration("200 second"),Response.class);
      OrgIdWithSourceAndExternalId = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
      System.out.println("OrgIdWithSourceAndExternalId : "+OrgIdWithSourceAndExternalId);
      Assert.assertNotNull(OrgIdWithSourceAndExternalId);
    }
    
    @Test
    public void test14CreateOrgWithSameSourceAndExternalIdExc(){
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put("orgCode", "CBSE");
      orgMap.put(JsonKey.PROVIDER, "pr10001");
      orgMap.put(JsonKey.EXTERNAL_ID, "ex10001");
      orgMap.put("channel", "test");
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test15CreateOrgWithBlankSourceAndExternalIdExc(){
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_NAME , "CBSE");
      orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
      orgMap.put("orgCode", "CBSE");
      orgMap.put(JsonKey.PROVIDER, null);
      orgMap.put("externalId", null);
      orgMap.put("channel", "test");
      innerMap.put(JsonKey.ORGANISATION , orgMap);

      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    
    @Test
    public void test16CreateOrgRootWithoutChannelExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test17CreateOrgInvalidParentIdExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test18ApproveOrgSuc(){
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
      probe.expectMsgClass(duration("200 second"),Response.class);
    }

    
    @Test
    public void test19ApproveOrgExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test20UpdateStatusSuc(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.ORGANISATION_ID , orgId);
      orgMap.put(JsonKey.STATUS, new BigInteger(String.valueOf(OrgStatus.RETIRED.getValue())));
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("200 second"),Response.class);
    }
    
    @Test
    public void test21UpdateStatusEx(){
      TestKit probe = new TestKit(system);
      
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG_STATUS.getValue());
      HashMap<String, Object> innerMap = new HashMap<>();
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.PROVIDER , source);
      orgMap.put(JsonKey.EXTERNAL_ID, externalId);
      orgMap.put(JsonKey.STATUS, new BigInteger("10"));
      innerMap.put(JsonKey.ORGANISATION , orgMap);
      reqObj.setRequest(innerMap);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test22UpdateOrgExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test23UpdateOrgSuc(){
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
      probe.expectMsgClass(duration("200 second"),Response.class);
    }
    
    @Test
    public void test24GetOrgSuc() {
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
        Response resp =probe.expectMsgClass(duration("200 second"),Response.class);
       // addressId = (String) ((Map<String,Object>)resp.getResult().get(JsonKey.ADDRESS)).get(JsonKey.ID);
    }
    
    @Test
    public void test25GetOrgExc() {
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
        probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test26AddMemberToOrgExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test27AddMemberToOrgSuc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test28RemoveMemberFromOrgSuc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test29RemoveMemberFromOrgExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
   
    @Test
    public void test30JoinMemberOrgSuc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test31JoinMemberOrgExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test32ApproveMemberOrgSuc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test33ApproveMemberFromOrgExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test34RejectMemberOrgSuc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    @Test
    public void test35RejectMemberOrgExc(){
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
      probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
    public void deleteOrgBySourceAndExternalId(){
      
    }
    
   // @Test
    public void test36CreateOrgType() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG_TYPE.getValue());
      reqObj.setRequest_id(ExecutionContext.getRequestId());
      reqObj.setEnv(1);
      reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_001");
      subject.tell(reqObj, probe.getRef());
      Response response = probe.expectMsgClass(Response.class);
      assertEquals("SUCCESS", response.getResult().get(JsonKey.RESPONSE));
      
      Request req = new Request();
      req.setOperation(ActorOperations.GET_ORG_TYPE_LIST.getValue());
      req.setRequest_id(ExecutionContext.getRequestId());
      req.setEnv(1);
      subject.tell(req, probe.getRef());
      Response res = probe.expectMsgClass(duration("200 second"), Response.class);
      List<Map<String,Object>> resMapList = (List<Map<String, Object>>) res.getResult().get(JsonKey.RESPONSE);
      if(null != resMapList && !resMapList.isEmpty()){
        for(Map<String,Object> map : resMapList){
          String name = (String) map.get(JsonKey.NAME);
          if(null != name && "ORG_TYPE_001".equalsIgnoreCase(name)){
            orgTypeId1 = (String) map.get(JsonKey.ID);
          }
        }
      }
    }
    
   // @Test
    public void test37CreateOrgType() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG_TYPE.getValue());
      reqObj.setRequest_id(ExecutionContext.getRequestId());
      reqObj.setEnv(1);
      reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_002");
      subject.tell(reqObj, probe.getRef());
      Response response = probe.expectMsgClass(Response.class);
      assertEquals("SUCCESS", response.getResult().get(JsonKey.RESPONSE));
      
      Request req = new Request();
      req.setOperation(ActorOperations.GET_ORG_TYPE_LIST.getValue());
      req.setRequest_id(ExecutionContext.getRequestId());
      req.setEnv(1);
      subject.tell(req, probe.getRef());
      Response res = probe.expectMsgClass(duration("200 second"), Response.class);
      List<Map<String,Object>> resMapList = (List<Map<String, Object>>) res.getResult().get(JsonKey.RESPONSE);
      if(null != resMapList && !resMapList.isEmpty()){
        for(Map<String,Object> map : resMapList){
          String name = (String) map.get(JsonKey.NAME);
          if(null != name && "ORG_TYPE_002".equalsIgnoreCase(name)){
            orgTypeId2 = (String) map.get(JsonKey.ID);
          }
        }
      }
    }
   // @Test
    public void test38OrgTypeList() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.GET_ORG_TYPE_LIST.getValue());
      reqObj.setRequest_id(ExecutionContext.getRequestId());
      reqObj.setEnv(1);
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("200 second"), Response.class);
    }
    
   // @Test
    public void test39CreateOrgTypeWithSameName() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_ORG_TYPE.getValue());
      reqObj.setRequest_id(ExecutionContext.getRequestId());
      reqObj.setEnv(1);
      reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_001");
      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(ProjectCommonException.class);
    }
   // @Test
    public void test40UpdateOrgType() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG_TYPE.getValue());
      reqObj.setRequest_id(ExecutionContext.getRequestId());
      reqObj.setEnv(1);
      reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_203");
      reqObj.getRequest().put(JsonKey.ID, orgTypeId1);
      subject.tell(reqObj, probe.getRef());
      Response response = probe.expectMsgClass(Response.class);
      assertEquals("SUCCESS", response.getResult().get(JsonKey.RESPONSE));
    }
    
   // @Test
    public void test41UpdateOrgTypeWithExistingName() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG_TYPE.getValue());
      reqObj.setRequest_id(ExecutionContext.getRequestId());
      reqObj.setEnv(1);
      reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_001");
      reqObj.getRequest().put(JsonKey.ID, orgTypeId2);
      subject.tell(reqObj, probe.getRef());
      ProjectCommonException response = probe.expectMsgClass(duration("200 second"),ProjectCommonException.class);
    }
    
  //  @Test
    public void test42UpdateOrgTypeWithWrongId() {
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);
      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.UPDATE_ORG_TYPE.getValue());
      reqObj.setRequest_id(ExecutionContext.getRequestId());
      reqObj.setEnv(1);
      reqObj.getRequest().put(JsonKey.NAME, "ORG_TYPE_12");
      String id = orgTypeId2+"1";
      reqObj.getRequest().put(JsonKey.ID, id);
      subject.tell(reqObj, probe.getRef());
      Response response = probe.expectMsgClass(duration("200 second"),Response.class);
      assertEquals("SUCCESS", response.getResult().get(JsonKey.RESPONSE));
    }
    
    
    @AfterClass
    public static void delete() {
      System.out.println("After class");
      SSOManager ssoManager = SSOServiceFactory.getInstance();
      Map<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.USER_ID, usrId);
      ssoManager.removeUser(innerMap);
      try{
      CassandraOperation operation = ServiceFactory.getInstance();
      //operation.deleteRecord(orgTypeDbInfo.getKeySpace(), orgTypeDbInfo.getTableName(), orgTypeId1);
      //operation.deleteRecord(orgTypeDbInfo.getKeySpace(), orgTypeDbInfo.getTableName(), orgTypeId2);
      //operation.deleteRecord(userManagementDB.getKeySpace(), userManagementDB.getTableName(), usrId);
      //operation.deleteRecord(addressDB.getKeySpace(), addressDB.getTableName(), addressId);
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), orgId);
      System.out.println("1 "+ orgId);
      
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), OrgIDWithoutSourceAndExternalId);
      System.out.println("2 "+ OrgIDWithoutSourceAndExternalId);
      
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), OrgIdWithSourceAndExternalId);
      System.out.println("3 "+ OrgIdWithSourceAndExternalId);
      
      Map<String , Object> orgMap = new HashMap<String , Object>();
      orgMap.put(JsonKey.PROVIDER, "pr100000001");
      orgMap.put(JsonKey.EXTERNAL_ID, "ex100000001");
      Response response = operation.getRecordsByProperties(orgDB.getKeySpace(), orgDB.getTableName(), orgMap);
      response.getResult().get(JsonKey.ID);
      System.out.println("Id frm response "+response.getResult().get(JsonKey.ID));
      }catch(Throwable th){
        th.printStackTrace();
      }
      ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.user.getTypeName(), usrId);
      
      ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.organisation.getTypeName(), orgId);
      ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.organisation.getTypeName(), OrgIDWithoutSourceAndExternalId);
      ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.organisation.getTypeName(), OrgIdWithSourceAndExternalId);
    }
}