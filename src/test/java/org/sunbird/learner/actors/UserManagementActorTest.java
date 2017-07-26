package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.impl.KeyCloakServiceImpl;

/**
 * @author Amit Kumar
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserManagementActorTest {

    static ActorSystem system;
    static CassandraOperation operation= new CassandraOperationImpl();
    static PropertiesCache cach = PropertiesCache.getInstance();
    final static Props props = Props.create(UserManagementActor.class);
    static Util.DbInfo userManagementDB = null;
    static Util.DbInfo addressDB = null;
    static Util.DbInfo jobDB = null;
    static Util.DbInfo eduDB = null;
    static Util.DbInfo orgDB = null;
    private static String userId = "";
    private static String addressId = "";
    private static String eduId = "";
    private static String jobId = "";
    private static String orgId = "";
    //private static 

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        userManagementDB = Util.dbInfoMap.get(JsonKey.USER_DB);
        addressDB = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
        jobDB = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
        eduDB = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
        orgDB  = Util.dbInfoMap.get(JsonKey.ORG_DB);
        
        /*Map<String,Object> orgMap =  new HashMap<>();
        orgMap.put(JsonKey.ORG_NAME, "ORG Name");
        orgId = ProjectUtil.getUniqueIdFromTimestamp(1);
        orgMap.put(JsonKey.ID, orgId);
        operation.insertRecord(orgDB.getKeySpace(), orgDB.getTableName(), orgMap);*/
    }

    @Test
    public void TestACreateUser(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_2017");
        innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_2017@gmail.com");
        innerMap.put(JsonKey.PASSWORD , "password");
        innerMap.put(JsonKey.PROVIDER , "BLR");
        innerMap.put(JsonKey.REGISTERED_ORG_ID, orgId);
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        Response response =  probe.expectMsgClass(duration("100 second"),Response.class);
        userId = (String) response.get(JsonKey.USER_ID);
        innerMap.put(JsonKey.ID, userId);
        System.out.println(userId);
    }
    
    @Test
    public void TestBUpdateUserInfo(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.LAST_NAME, "user_last_name_updated");
        innerMap.put(JsonKey.ID, userId);
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("100 second"),Response.class);
    }
    
    @Test
    public void TestCAddUserAddressInfo(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.LAST_NAME, "user_last_name_twice");
        List<Map<String , Object>> list = new ArrayList<Map<String,Object>>();
        Map<String , Object> address = new HashMap<String , Object>();
        address.put(JsonKey.ADDRESS_LINE1, "addr line1");
        innerMap.put(JsonKey.USER_ID, userId);
        address.put(JsonKey.CITY, "city");
        list.add(address);
        innerMap.put(JsonKey.ADDRESS, list);
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("100 second"),Response.class);
        
        
    }
    
    @Test
    public void TestDUpdatedUserAddressInfo(){
      Response response=operation.getRecordsByProperty(addressDB.getKeySpace(), addressDB.getTableName(),JsonKey.USER_ID, userId);
      Map<String,Object> result =  (Map<String,Object>)(response.getResult());
      //Map<String,Object> res = (Map<String, Object>) result.get(JsonKey.RESPONSE);
      String name = (String) ((Map<String,Object>)((((List<Map<String,Object>>)result.get(JsonKey.RESPONSE)).get(0)))).get(JsonKey.ADDRESS_LINE1);
      assertEquals("addr line1", name);
    }
    
    @Test
    public void TestEAddUserEducationInfo(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.LAST_NAME, "user_last_name_thrice");
        List<Map<String , Object>> list = new ArrayList<Map<String,Object>>();
        Map<String , Object> education = new HashMap<String , Object>();
        education.put(JsonKey.DEGREE, "degree");
        education.put(JsonKey.NAME, "College Name");
        innerMap.put(JsonKey.USER_ID, userId);
        list.add(education);
        innerMap.put(JsonKey.EDUCATION, list);
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("100 second"),Response.class);
    }
    
    @Test
    public void TestEUpdatedUserEducationInfo(){
      Response response=operation.getRecordsByProperty(eduDB.getKeySpace(), eduDB.getTableName(),JsonKey.USER_ID, userId);
      Map<String,Object> result =  (Map<String,Object>)(response.getResult());
      //Map<String,Object> res = (Map<String, Object>) result.get(JsonKey.RESPONSE);
      String name = (String) ((Map<String,Object>)((((List<Map<String,Object>>)result.get(JsonKey.RESPONSE)).get(0)))).get(JsonKey.NAME);
      assertEquals("College Name", name);
    }
    
    @Test
    public void TestFAddUserJobProfileInfo(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.LAST_NAME, "user_last_name_frice");
        List<Map<String , Object>> list = new ArrayList<Map<String,Object>>();
        Map<String , Object> jobProfile = new HashMap<String , Object>();
        jobProfile.put(JsonKey.JOB_NAME, "job title");
        jobProfile.put(JsonKey.ORG_NAME, "KA Org");
        list.add(jobProfile);
        innerMap.put(JsonKey.JOB_PROFILE, list);
        innerMap.put(JsonKey.USER_ID, userId);
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("100 second"),Response.class);
        
    }
    
    @Test
    public void TestFUpdatedUserJobProfileInfo(){
      Response response=operation.getRecordsByProperty(jobDB.getKeySpace(), jobDB.getTableName(),JsonKey.USER_ID, userId);
      Map<String,Object> result =  (Map<String,Object>)(response.getResult());
      //Map<String,Object> res = (Map<String, Object>) result.get(JsonKey.RESPONSE);
      String jobName = (String) ((Map<String,Object>)((((List<Map<String,Object>>)result.get(JsonKey.RESPONSE)).get(0)))).get(JsonKey.JOB_NAME);
      System.out.println(jobName);
      assertEquals("job title", jobName);
    }
    
    @Test
    public void TestGGetUserInfo(){
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.GET_PROFILE.getValue());
      Map<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.USER_ID, userId);
      Map<String , Object> request = new HashMap<String , Object>();
      request.put(JsonKey.USER , innerMap);
      reqObj.setRequest(request);

      subject.tell(reqObj, probe.getRef());
      Response userResponse = probe.expectMsgClass(duration("100 second"),Response.class);
      Map<String,Object> result =  (Map<String,Object>)(userResponse.getResult());
      Map<String,Object> response = (Map<String, Object>) result.get(JsonKey.RESPONSE);
      assertEquals("user_last_name_frice", response.get("lastName"));
      addressId = (String) (((List<Map<String,Object>>)response.get(JsonKey.ADDRESS)).get(0)).get(JsonKey.ID);
      jobId = (String) (((List<Map<String,Object>>)response.get(JsonKey.JOB_PROFILE)).get(0)).get(JsonKey.ID);
      eduId = (String) (((List<Map<String,Object>>)response.get(JsonKey.EDUCATION)).get(0)).get(JsonKey.ID);
      
    }
    
    @Test
    public void TestGUpdateUserAddressInfo(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.LAST_NAME, "user_last_name_twice");
        List<Map<String , Object>> list = new ArrayList<Map<String,Object>>();
        Map<String , Object> address = new HashMap<String , Object>();
        address.put(JsonKey.ADDRESS_LINE1, "addr line1");
        innerMap.put(JsonKey.USER_ID, userId);
        address.put(JsonKey.CITY, "city1");
        address.put(JsonKey.ID, addressId);
        list.add(address);
        innerMap.put(JsonKey.ADDRESS, list);
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("100 second"),Response.class);
        
    }
    
    @Test
    public void TestHUpdateUserEducationInfo(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.LAST_NAME, "user_last_name_thrice");
        List<Map<String , Object>> list = new ArrayList<Map<String,Object>>();
        Map<String , Object> education = new HashMap<String , Object>();
        education.put(JsonKey.DEGREE, "degree1");
        education.put(JsonKey.NAME, "College Name");
        education.put(JsonKey.ID, eduId);
        innerMap.put(JsonKey.USER_ID, userId);
        list.add(education);
        innerMap.put(JsonKey.EDUCATION, list);
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("100 second"),Response.class);
        
        
    }
    
    @Test
    public void TestIUpdateUserJobProfileInfo(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.LAST_NAME, "user_last_name_frice");
        List<Map<String , Object>> list = new ArrayList<Map<String,Object>>();
        Map<String , Object> jobProfile = new HashMap<String , Object>();
        jobProfile.put(JsonKey.JOB_NAME, "job title");
        jobProfile.put(JsonKey.ORG_NAME, "KA Org");
        jobProfile.put(JsonKey.ID, jobId);
        list.add(jobProfile);
        innerMap.put(JsonKey.JOB_PROFILE, list);
        innerMap.put(JsonKey.USER_ID, userId);
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(duration("100 second"),Response.class);
        
    }
    
    @Test
    public void TestJGetUserInfoByLoginId(){
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue());
      Map<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.LOGIN_ID, "sunbird_dummy_user_2017@BLR");
      Map<String , Object> request = new HashMap<String , Object>();
      request.put(JsonKey.USER , innerMap);
      reqObj.setRequest(request);

      subject.tell(reqObj, probe.getRef());
      Response userResponse = probe.expectMsgClass(duration("100 second"),Response.class);
      Map<String,Object> result =  (Map<String,Object>)(userResponse.getResult());
      Map<String,Object> response = (Map<String, Object>) result.get(JsonKey.RESPONSE);
      assertEquals("city1", ((List<Map<String, Object>>)response.get(JsonKey.ADDRESS)).get(0).get(JsonKey.CITY));
     
    }
    
    //@Test
    public void TestJUserOrgInfo(){
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue());
      Map<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.LOGIN_ID, "sunbird_dummy_user_2017@BLR");
      Map<String , Object> request = new HashMap<String , Object>();
      request.put(JsonKey.USER , innerMap);
      reqObj.setRequest(request);

      subject.tell(reqObj, probe.getRef());
      Response userResponse = probe.expectMsgClass(duration("100 second"),Response.class);
      Map<String,Object> result =  (Map<String,Object>)(userResponse.getResult());
      Map<String,Object> response = (Map<String, Object>) result.get(JsonKey.RESPONSE);
     // assertEquals("city1", ((Map<String, Object>)response.get(JsonKey.REGISTERED_ORG)).get(JsonKey.CITY));
     System.out.println();
    }
    
    @Test
    public void TestKcreateUserTestWithDuplicateEmail(){

      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
      Map<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_201");
      innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_2017@gmail.com");
      innerMap.put(JsonKey.PASSWORD , "password");
      innerMap.put(JsonKey.PROVIDER , "BLR");
      Map<String , Object> request = new HashMap<String , Object>();
      request.put(JsonKey.USER , innerMap);
      reqObj.setRequest(request);

      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    }

    @Test
    public void TestLcreateUserTestWithDuplicateUserName(){

      TestKit probe = new TestKit(system);
      ActorRef subject = system.actorOf(props);

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
      Map<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_2017");
      innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_2017@gmail.com");
      innerMap.put(JsonKey.PASSWORD , "password");
      innerMap.put(JsonKey.PROVIDER , "BLR");
      Map<String , Object> request = new HashMap<String , Object>();
      request.put(JsonKey.USER , innerMap);
      reqObj.setRequest(request);

      subject.tell(reqObj, probe.getRef());
      probe.expectMsgClass(duration("100 second"),ProjectCommonException.class);
    }

    @AfterClass
    public static void deleteUser() {
      SSOManager ssoManager = new KeyCloakServiceImpl();
      Map<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.USER_ID, userId);
      ssoManager.removeUser(innerMap);
      
      Response response=operation.deleteRecord(userManagementDB.getKeySpace(), userManagementDB.getTableName(), userId);
      assertEquals("SUCCESS", response.get(JsonKey.RESPONSE));
      
      Response response1=operation.deleteRecord(addressDB.getKeySpace(), addressDB.getTableName(), addressId);
      assertEquals("SUCCESS", response1.get(JsonKey.RESPONSE));
      Response response2=operation.deleteRecord(jobDB.getKeySpace(), jobDB.getTableName(), jobId);
      assertEquals("SUCCESS", response2.get(JsonKey.RESPONSE));
      Response response3=operation.deleteRecord(eduDB.getKeySpace(), eduDB.getTableName(), eduId);
      assertEquals("SUCCESS", response3.get(JsonKey.RESPONSE));
      ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
          ProjectUtil.EsType.user.getTypeName(),
          userId);
      
    }

}
