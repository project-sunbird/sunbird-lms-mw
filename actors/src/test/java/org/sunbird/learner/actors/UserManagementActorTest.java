package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.Application;
import org.sunbird.learner.actors.badges.BadgesActor;
import org.sunbird.learner.actors.notificationservice.EmailServiceActor;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;

/**
 * @author Amit Kumar
 */
// @Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserManagementActorTest {

  private static ActorSystem system;
  private static CassandraOperation operation = ServiceFactory.getInstance();
  private SSOManager ssoManager = SSOServiceFactory.getInstance();
  private final static Props props = Props.create(UserManagementActor.class);
  private final static Props orgProps = Props.create(OrganisationManagementActor.class);
  private final static Props emailServiceProps = Props.create(EmailServiceActor.class);
  private final static Props badgeProps = Props.create(BadgesActor.class);
  private static EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
          .getEncryptionServiceInstance(null);
  private static Util.DbInfo userManagementDB = null;
  private static Util.DbInfo addressDB = null;
  private static Util.DbInfo jobDB = null;
  private static Util.DbInfo eduDB = null;
  private static Util.DbInfo orgDB = null;
  private static Util.DbInfo userOrgDB = null;
  private static String userId = "";
  private static String userId2 = "";
  private static String addressId = "";
  private static String eduId = "";
  private static String jobId = "";
  private static String orgId = "";
  private static String orgId2 = "";
  private static String userOrgId = "";
  private static String userAddrIdToDelete = "";
  private static String userJobIdWithAddress = "";
  private static String userEduIdWithAddress = "";
  private static String encryption = "";
  private static String userIdnew = "";
  private static String authToken = "";

  @BeforeClass
  public static void setUp() {
    Application.startLocalActorSystem();
    encryption = PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_ENCRYPTION);
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    userManagementDB = Util.dbInfoMap.get(JsonKey.USER_DB);
    addressDB = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);
    jobDB = Util.dbInfoMap.get(JsonKey.JOB_PROFILE_DB);
    eduDB = Util.dbInfoMap.get(JsonKey.EDUCATION_DB);
    orgDB = Util.dbInfoMap.get(JsonKey.ORG_DB);
    userOrgDB = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
    new DataCacheHandler().run();
  }

  @Test
  public void testAAAInvalidOperation() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("INVALID_OPERATION");

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestAAcreateOrgForId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(orgProps);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "DUMMY_ORG");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education");
    orgMap.put(JsonKey.ORG_CODE, "DUMMY_ORG");
    orgMap.put(JsonKey.EXTERNAL_ID, "EXT_ID_DUMMY");
    orgMap.put(JsonKey.PROVIDER, "BLR");
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response resp = probe.expectMsgClass(duration("200 second"), Response.class);
    orgId = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
    assertTrue(null != orgId);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void TestAAcreateOrgForId1() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(orgProps);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_ORG.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> orgMap = new HashMap<String, Object>();
    orgMap.put(JsonKey.ORGANISATION_NAME, "DUMMY_ORG1");
    orgMap.put(JsonKey.DESCRIPTION, "Central Board of Secondary Education1");
    orgMap.put(JsonKey.ORG_CODE, "DUMMY_ORG1");
    orgMap.put(JsonKey.EXTERNAL_ID, "EXT_ID_DUMMY1");
    orgMap.put(JsonKey.PROVIDER, "BLR");
    innerMap.put(JsonKey.ORGANISATION, orgMap);

    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response resp = probe.expectMsgClass(duration("200 second"), Response.class);
    orgId2 = (String) resp.getResult().get(JsonKey.ORGANISATION_ID);
    assertTrue(null != orgId2);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


  @Test
  public void TestACreateUser() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_313131");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_313131@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.PROVIDER, "BLR");
    innerMap.put(JsonKey.PHONE, "9874561231");
    innerMap.put(JsonKey.PHONE_VERIFIED, true);
    innerMap.put(JsonKey.EMAIL_VERIFIED, true);
    innerMap.put(JsonKey.REGISTERED_ORG_ID, orgId);
    // Add Roles
    List<String> roleList = new ArrayList<>();
    roleList.add("CONTENT_CURATION");
    roleList.add("CONTENT_CREATION");
    roleList.add("MEMBERSHIP_MANAGEMENT");
    innerMap.put(JsonKey.ROLES, roleList);
    // Add Address
    List<Map<String, Object>> addrList = new ArrayList<Map<String, Object>>();
    Map<String, Object> address = new HashMap<String, Object>();
    address.put(JsonKey.ADDRESS_LINE1, "addr line1");
    address.put(JsonKey.CITY, "city");
    Map<String, Object> address2 = new HashMap<String, Object>();
    address2.put(JsonKey.ADDRESS_LINE1, "addr line1");
    address2.put(JsonKey.CITY, "city");
    addrList.add(address);
    addrList.add(address2);
    innerMap.put(JsonKey.ADDRESS, addrList);
    // Add Job Profile
    List<Map<String, Object>> jobProfileList = new ArrayList<Map<String, Object>>();
    Map<String, Object> jobProfile = new HashMap<String, Object>();
    jobProfile.put(JsonKey.JOB_NAME, "job title");
    jobProfile.put(JsonKey.ORG_NAME, "KA Org");
    jobProfileList.add(jobProfile);
    innerMap.put(JsonKey.JOB_PROFILE, jobProfileList);
    // Add Education
    List<Map<String, Object>> eduList = new ArrayList<Map<String, Object>>();
    Map<String, Object> education = new HashMap<String, Object>();
    education.put(JsonKey.DEGREE, "degree");
    education.put(JsonKey.NAME, "College Name");
    innerMap.put(JsonKey.USER_ID, userId);
    eduList.add(education);
    innerMap.put(JsonKey.EDUCATION, eduList);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("200 second"), Response.class);
    userId = (String) response.get(JsonKey.USER_ID);
    System.out.println("userId " + userId);
    innerMap.put(JsonKey.ID, userId);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue(null != userId);
  }

  @Test
  public void TestACreateUser2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_3131311");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.PROVIDER, "BLR");
    innerMap.put(JsonKey.PHONE, "9874561233");
    innerMap.put(JsonKey.PHONE_VERIFIED, true);
    innerMap.put(JsonKey.EMAIL_VERIFIED, true);
    innerMap.put(JsonKey.REGISTERED_ORG_ID, orgId);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("200 second"), Response.class);
    userId2 = (String) response.get(JsonKey.USER_ID);
    System.out.println(userId2);
    innerMap.put(JsonKey.ID, userId2);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue(null != userId2);
  }

  @Test
  public void TestACreateUser3() {

    DataCacheHandler.getConfigSettings().put(JsonKey.EMAIL_UNIQUE, "TRUE");
    DataCacheHandler.getConfigSettings().put(JsonKey.PHONE_UNIQUE, "TRUE");

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_31313112");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.PROVIDER, "BLR");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_313131@gmail.com");
    innerMap.put(JsonKey.PHONE_VERIFIED, true);
    innerMap.put(JsonKey.EMAIL_VERIFIED, true);
    innerMap.put(JsonKey.REGISTERED_ORG_ID, orgId);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);

  }

  @Test
  public void TestACreateUser4() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    DataCacheHandler.getConfigSettings().put(JsonKey.EMAIL_UNIQUE, "TRUE");
    DataCacheHandler.getConfigSettings().put(JsonKey.PHONE_UNIQUE, "TRUE");
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_31313113");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.PROVIDER, "BLR");
    innerMap.put(JsonKey.PHONE, "9874561231");
    innerMap.put(JsonKey.PHONE_VERIFIED, true);
    innerMap.put(JsonKey.EMAIL_VERIFIED, true);
    innerMap.put(JsonKey.REGISTERED_ORG_ID, orgId);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestACreateUser5() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, userId2);
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_313131@gmail.com");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestACreateUser6() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, userId2);
    innerMap.put(JsonKey.PHONE, "9874561231");

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);

    DataCacheHandler.getConfigSettings().put(JsonKey.EMAIL_UNIQUE, "FALSE");
    DataCacheHandler.getConfigSettings().put(JsonKey.PHONE_UNIQUE, "FALSE");
  }

  @Test
  public void TestACreateUserWithInvalidOrgId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_313131");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_313131@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.PROVIDER, "BLR");
    innerMap.put(JsonKey.PHONE, "9874561230");
    innerMap.put(JsonKey.PHONE_VERIFIED, true);
    innerMap.put(JsonKey.EMAIL_VERIFIED, true);
    innerMap.put(JsonKey.REGISTERED_ORG_ID, (orgId + "13215665"));

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestBUpdateUserInfo() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.LAST_NAME, "user_last_name_updated");
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_313131");
    innerMap.put(JsonKey.ID, userId);
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_313131@gmail.com");
    // Add Roles
    List<String> roleList = new ArrayList<>();
    roleList.add("CONTENT_CURATION");
    roleList.add("CONTENT_CREATION");
    roleList.add("MEMBERSHIP_MANAGEMENT");
    innerMap.put(JsonKey.ROLES, roleList);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void TestBUpdateUserInfoWithInvalidRole() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.LAST_NAME, "user_last_name_updated");
    innerMap.put(JsonKey.ID, userId);
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_313131@gmail.com");
    // Add Roles
    List<String> roleList = new ArrayList<>();
    roleList.add("CONTENT_CURATION_1");
    roleList.add("CONTENT_CREATION_2");
    roleList.add("MEMBERSHIP_MANAGEMENT_3");
    innerMap.put(JsonKey.ROLES, roleList);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestBCreateUserInfoWithInvalidRole() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_3131311");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_3131311@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    // Add Roles
    List<String> roleList = new ArrayList<>();
    roleList.add("CONTENT_CURATION_1");
    roleList.add("CONTENT_CREATION_2");
    roleList.add("MEMBERSHIP_MANAGEMENT_3");
    innerMap.put(JsonKey.ROLES, roleList);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestCgetUserAddressInfo() {
    String encUserId = userId;
    if ("ON".equalsIgnoreCase(encryption)) {
      try {
        encUserId = encryptionService.encryptData(encUserId);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    Response response = operation.getRecordsByProperty(addressDB.getKeySpace(),
        addressDB.getTableName(), JsonKey.USER_ID, encUserId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    List<Map<String, Object>> addressList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    userAddrIdToDelete = (String) ((Map<String, Object>) addressList.get(0)).get(JsonKey.ID);
    assertEquals(addressList.size(), 2);
  }

  @Test
  public void TestCgetUserAddressInfoAndDelete() {

    String encUserId = userId;
    if ("ON".equalsIgnoreCase(encryption)) {
      try {
        encUserId = encryptionService.encryptData(encUserId);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    Map<String, Object> address = new HashMap<String, Object>();
    address.put(JsonKey.IS_DELETED, true);
    address.put(JsonKey.ID, userAddrIdToDelete);
    list.add(address);
    innerMap.put(JsonKey.ADDRESS, list);
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("200 second"), Response.class);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Response response = operation.getRecordsByProperty(addressDB.getKeySpace(),
        addressDB.getTableName(), JsonKey.USER_ID, encUserId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    List<Map<String, Object>> addressList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    assertEquals(addressList.size(), 1);
  }

  @Test
  public void TestDUpdatedUserAddressInfo() {
    String addrLine1 = "addr line1";
    String encUserId = userId;
    if ("ON".equalsIgnoreCase(encryption)) {
      try {
        addrLine1 = encryptionService.encryptData("addr line1");
        encUserId = encryptionService.encryptData(userId);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    Response response = operation.getRecordsByProperty(addressDB.getKeySpace(),
        addressDB.getTableName(), JsonKey.USER_ID, encUserId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    String addrLine =
        (String) ((Map<String, Object>) ((((List<Map<String, Object>>) result.get(JsonKey.RESPONSE))
            .get(0)))).get(JsonKey.ADDRESS_LINE1);
    assertEquals(addrLine1, addrLine);
  }

  @Test
  public void TestEUpdatedUserEducationInfo() {
    Response response = operation.getRecordsByProperty(eduDB.getKeySpace(), eduDB.getTableName(),
        JsonKey.USER_ID, userId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    String name =
        (String) ((Map<String, Object>) ((((List<Map<String, Object>>) result.get(JsonKey.RESPONSE))
            .get(0)))).get(JsonKey.NAME);
    assertEquals("College Name", name);
  }

  @Test
  public void TestFUpdatedUserJobProfileInfo() {
    Response response = operation.getRecordsByProperty(jobDB.getKeySpace(), jobDB.getTableName(),
        JsonKey.USER_ID, userId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    String jobName =
        (String) ((Map<String, Object>) ((((List<Map<String, Object>>) result.get(JsonKey.RESPONSE))
            .get(0)))).get(JsonKey.JOB_NAME);
    assertEquals("job title", jobName);
  }


  @Test
  public void TestGGetUserInfo() {
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PROFILE.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response userResponse = probe.expectMsgClass(duration("200 second"), Response.class);
    Map<String, Object> result = (Map<String, Object>) (userResponse.getResult());
    Map<String, Object> response = (Map<String, Object>) result.get(JsonKey.RESPONSE);
    assertEquals("user_last_name_updated", response.get("lastName"));
    addressId = (String) (((List<Map<String, Object>>) response.get(JsonKey.ADDRESS)).get(0))
        .get(JsonKey.ID);
    jobId = (String) (((List<Map<String, Object>>) response.get(JsonKey.JOB_PROFILE)).get(0))
        .get(JsonKey.ID);
    eduId = (String) (((List<Map<String, Object>>) response.get(JsonKey.EDUCATION)).get(0))
        .get(JsonKey.ID);

  }

  @Test
  public void TestGGetUserInfoWithInvalidId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PROFILE.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, (userId + "12345"));
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);

  }

  @Test
  public void TestGUpdateUserAddressInfo() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.LAST_NAME, "user_last_name_twice");
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    Map<String, Object> address = new HashMap<String, Object>();
    address.put(JsonKey.ADDRESS_LINE1, "addr line1");
    innerMap.put(JsonKey.USER_ID, userId);
    address.put(JsonKey.CITY, "new city");
    address.put(JsonKey.ID, addressId);
    list.add(address);
    innerMap.put(JsonKey.ADDRESS, list);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void TestHUpdateUserEducationInfo() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.LAST_NAME, "user_last_name_thrice");
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    Map<String, Object> education = new HashMap<String, Object>();
    education.put(JsonKey.DEGREE, "degree1");
    education.put(JsonKey.NAME, "College Name");
    education.put(JsonKey.ID, eduId);
    innerMap.put(JsonKey.USER_ID, userId);
    list.add(education);
    innerMap.put(JsonKey.EDUCATION, list);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


  }

  @Test
  public void TestIUpdateUserJobProfileInfo() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.LAST_NAME, "user_last_name_frice");
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    Map<String, Object> jobProfile = new HashMap<String, Object>();
    jobProfile.put(JsonKey.JOB_NAME, "job title");
    jobProfile.put(JsonKey.ORG_NAME, "KA Org");
    jobProfile.put(JsonKey.ID, jobId);
    list.add(jobProfile);
    innerMap.put(JsonKey.JOB_PROFILE, list);
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void TestJGetUserInfoByLoginId() {

    String encLoginId = "sunbird_dummy_user_313131@BLR";
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.LOGIN_ID, encLoginId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response userResponse = probe.expectMsgClass(duration("200 second"), Response.class);
    Map<String, Object> result = (Map<String, Object>) (userResponse.getResult());
    Map<String, Object> response = (Map<String, Object>) result.get(JsonKey.RESPONSE);
    System.out.println("user Response : " + response.get(JsonKey.USER_ID));
    assertEquals((String) response.get(JsonKey.ID), userId);
    assertEquals(userResponse.getResponseCode().getResponseCode(),
        ResponseCode.OK.getResponseCode());
  }

  @Test
  public void TestJGetUserInfoByInvalidLoginId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.LOGIN_ID, "sunbird_dummy_user_313131@BLR1324564");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestJUserOrgInfo() {
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
    innerMap.put(JsonKey.LOGIN_ID, "sunbird_dummy_user_313131@BLR");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response userResponse = probe.expectMsgClass(duration("200 second"), Response.class);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Map<String, Object> result = (Map<String, Object>) (userResponse.getResult());
    Map<String, Object> response = (Map<String, Object>) result.get(JsonKey.RESPONSE);
    assertEquals("DUMMY_ORG", (String) ((Map<String, Object>) response.get(JsonKey.REGISTERED_ORG))
        .get(JsonKey.ORGANISATION_NAME));
  }

  @Test
  public void TestKUserOrgTableInfo() {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.USER_ID, userId);
    map.put(JsonKey.ORGANISATION_ID, orgId);
    Response response =
        operation.getRecordsByProperties(userOrgDB.getKeySpace(), userOrgDB.getTableName(), map);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    List<String> roles =
        (List) ((Map<String, Object>) ((((List<Map<String, Object>>) result.get(JsonKey.RESPONSE))
            .get(0)))).get(JsonKey.ROLES);
    assertTrue(!roles.contains(ProjectUtil.UserRole.CONTENT_CREATOR.getValue()));
    userOrgId =
        (String) ((Map<String, Object>) ((((List<Map<String, Object>>) result.get(JsonKey.RESPONSE))
            .get(0)))).get(JsonKey.ID);
  }

  @Test
  public void TestKcreateUserTestWithDuplicateEmail() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_181");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_313131@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.PROVIDER, "BLR");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestLcreateUserTestWithDuplicateUserName() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_313131");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_313131@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.PROVIDER, "BLR");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void TestMBlockUser() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BLOCK_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void TestNGetUserInfoAfterBlocking() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PROFILE.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException response =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertEquals("User account has been blocked .", response.getMessage());

  }

  @Test
  public void TestOUnBlockUser() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UNBLOCK_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void TestPGetUserInfoAfterUnBlocking() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PROFILE.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response userResponse = probe.expectMsgClass(duration("200 second"), Response.class);
    Map<String, Object> result = (Map<String, Object>) (userResponse.getResult());
    Map<String, Object> response = (Map<String, Object>) result.get(JsonKey.RESPONSE);
    assertEquals("user_last_name_frice", response.get("lastName"));

  }

  @Test
  public void TestQAddEducationDetailsWithAddress() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    // Add Education
    List<Map<String, Object>> eduList = new ArrayList<Map<String, Object>>();
    Map<String, Object> education = new HashMap<String, Object>();
    education.put(JsonKey.DEGREE, "degree");
    education.put(JsonKey.PERCENTAGE, "70");
    education.put(JsonKey.YEAR_OF_PASSING, new BigInteger("1970"));
    education.put(JsonKey.NAME, "College Name");
    eduList.add(education);

    Map<String, Object> address = new HashMap<String, Object>();
    address.put(JsonKey.ADDRESS_LINE1, "addr line1");
    address.put(JsonKey.CITY, "city");
    education.put(JsonKey.ADDRESS, address);
    innerMap.put(JsonKey.EDUCATION, eduList);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("200 second"), Response.class);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Response response = operation.getRecordsByProperty(eduDB.getKeySpace(), eduDB.getTableName(),
        JsonKey.USER_ID, userId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    List<Map<String, Object>> educationList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    assertEquals(educationList.size(), 2);
    for (Map<String, Object> map : educationList) {
      if (!(eduId.equalsIgnoreCase((String) map.get(JsonKey.ID)))) {
        userEduIdWithAddress = (String) map.get(JsonKey.ID);
      }
    }
  }

  @Test
  public void TestQDeleteEducationDetailsWithAddress() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    // Add Education
    List<Map<String, Object>> eduList = new ArrayList<Map<String, Object>>();
    Map<String, Object> education = new HashMap<String, Object>();
    education.put(JsonKey.ID, userEduIdWithAddress);
    education.put(JsonKey.IS_DELETED, true);
    eduList.add(education);
    innerMap.put(JsonKey.EDUCATION, eduList);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("200 second"), Response.class);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Response response = operation.getRecordsByProperty(eduDB.getKeySpace(), eduDB.getTableName(),
        JsonKey.USER_ID, userId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    List<Map<String, Object>> educationList =
        (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    assertEquals(educationList.size(), 1);
  }

  @Test
  public void TestRAddJobDetailsWithAddress() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    // Add Job Profile
    List<Map<String, Object>> jobProfileList = new ArrayList<Map<String, Object>>();
    Map<String, Object> jobProfile = new HashMap<String, Object>();
    jobProfile.put(JsonKey.JOB_NAME, "job title");
    jobProfile.put(JsonKey.ORG_NAME, "KA Org");
    jobProfileList.add(jobProfile);

    Map<String, Object> address = new HashMap<String, Object>();
    address.put(JsonKey.ADDRESS_LINE1, "addr line1");
    address.put(JsonKey.CITY, "city");
    jobProfile.put(JsonKey.ADDRESS, address);
    innerMap.put(JsonKey.JOB_PROFILE, jobProfileList);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("200 second"), Response.class);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Response response = operation.getRecordsByProperty(jobDB.getKeySpace(), jobDB.getTableName(),
        JsonKey.USER_ID, userId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    List<Map<String, Object>> jobList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    assertEquals(jobList.size(), 2);
    for (Map<String, Object> map : jobList) {
      if (!(jobId.equalsIgnoreCase((String) map.get(JsonKey.ID)))) {
        userJobIdWithAddress = (String) map.get(JsonKey.ID);
      }
    }
  }


  @Test
  public void TestRDeleteJobDetailsWithAddress() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    // Add Job Profile
    List<Map<String, Object>> jobProfileList = new ArrayList<Map<String, Object>>();
    Map<String, Object> jobProfile = new HashMap<String, Object>();
    jobProfile.put(JsonKey.ID, userJobIdWithAddress);
    jobProfile.put(JsonKey.IS_DELETED, true);
    jobProfileList.add(jobProfile);
    innerMap.put(JsonKey.JOB_PROFILE, jobProfileList);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("200 second"), Response.class);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Response response = operation.getRecordsByProperty(jobDB.getKeySpace(), jobDB.getTableName(),
        JsonKey.USER_ID, userId);
    Map<String, Object> result = (Map<String, Object>) (response.getResult());
    List<Map<String, Object>> jobList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    assertEquals(jobList.size(), 1);
  }

  @Test
  public void TestZGetRoles() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_ROLES.getValue());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res);

  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestZACreateUserWithValidWebPage() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_31313100");
    innerMap.put(JsonKey.PHONE, "9742501111");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_31313100@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.ID, userId);
    List<Map<String, String>> webPage = new ArrayList<>();
    Map<String, String> webPageData = new HashMap<>();
    webPageData.put(JsonKey.TYPE, "fb");
    webPageData.put(JsonKey.URL, "https://www.facebook.com/facebook/");
    webPage.add(webPageData);
    innerMap.put(JsonKey.WEB_PAGES, webPage);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("2000 second"), Response.class);
    userIdnew = (String) response.get(JsonKey.USER_ID);
    assertTrue(null != userIdnew);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestZBCreateUserWithInValidWebPageType() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_3131310");
    innerMap.put(JsonKey.PASSWORD, "password");
    List<Map<String, String>> webPage = new ArrayList<>();
    Map<String, String> webPageData = new HashMap<>();
    webPageData.put(JsonKey.TYPE, "test");
    webPageData.put(JsonKey.URL, "https://www.facebook.com/facebook/");
    webPage.add(webPageData);
    innerMap.put(JsonKey.WEB_PAGES, webPage);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException response =
        probe.expectMsgClass(duration("2000 second"), ProjectCommonException.class);
    if (null != response) {
      Assert.assertEquals(ResponseMessage.Message.INVALID_MEDIA_TYPE, response.getMessage());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestZCCreateUserWithInValidWebPageURL() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_3131310");
    innerMap.put(JsonKey.PASSWORD, "password");
    List<Map<String, String>> webPage = new ArrayList<>();
    Map<String, String> webPageData = new HashMap<>();
    webPageData.put(JsonKey.TYPE, "fb");
    webPageData.put(JsonKey.URL, "https://test.com/test/");
    webPage.add(webPageData);
    innerMap.put(JsonKey.WEB_PAGES, webPage);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException response =
        probe.expectMsgClass(duration("2000 second"), ProjectCommonException.class);
    if (null != response) {
      Assert.assertEquals(ResponseMessage.Key.INVALID_WEBPAGE_URL, response.getCode());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestZDUpdateUserWithValidWebPage() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, userIdnew);
    List<Map<String, String>> webPage = new ArrayList<>();
    Map<String, String> webPageData = new HashMap<>();
    webPageData.put(JsonKey.TYPE, "in");
    webPageData.put(JsonKey.URL, "https://www.linkedin.com/in/linkedin");
    webPage.add(webPageData);
    innerMap.put(JsonKey.WEB_PAGES, webPage);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("2000 second"), Response.class);
    assertTrue(ResponseCode.OK == response.getResponseCode());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestZEUpdateUserWithInValidWebPageType() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_1919");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_1919@gmail.com");
    innerMap.put(JsonKey.ID, userIdnew);
    List<Map<String, String>> webPage = new ArrayList<>();
    Map<String, String> webPageData = new HashMap<>();
    webPageData.put(JsonKey.TYPE, "test");
    webPageData.put(JsonKey.URL, "https://www.facebook.com/facebook/");
    webPage.add(webPageData);
    innerMap.put(JsonKey.WEB_PAGES, webPage);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException response =
        probe.expectMsgClass(duration("2000 second"), ProjectCommonException.class);
    if (null != response) {
      Assert.assertEquals(ResponseMessage.Message.INVALID_MEDIA_TYPE, response.getMessage());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestZFUpdateUserWithInValidWebPageURL() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_1919");
    innerMap.put(JsonKey.EMAIL, "sunbird_dummy_user_1919@gmail.com");
    innerMap.put(JsonKey.ID, userIdnew);
    List<Map<String, String>> webPage = new ArrayList<>();
    Map<String, String> webPageData = new HashMap<>();
    webPageData.put(JsonKey.TYPE, "fb");
    webPageData.put(JsonKey.URL, "https://test.com/test/");
    webPage.add(webPageData);
    innerMap.put(JsonKey.WEB_PAGES, webPage);

    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException response =
        probe.expectMsgClass(duration("2000 second"), ProjectCommonException.class);
    if (null != response) {
      Assert.assertEquals("Invalid URL for facebook", response.getMessage());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void userLoginWithInvalidLoginId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.LOGIN.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.SOURCE, "web");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException response =
        probe.expectMsgClass(duration("2000 second"), ProjectCommonException.class);
    if (null != response) {
      Assert.assertEquals(ResponseCode.CLIENT_ERROR.getResponseCode(), response.getResponseCode());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void userLoginWithInvalidEmail() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.LOGIN.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "test123@ntp");
    innerMap.put(JsonKey.PASSWORD, "pass");
    innerMap.put(JsonKey.SOURCE, "web");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException response =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    if (null != response) {
      Assert.assertEquals(ResponseCode.CLIENT_ERROR.getResponseCode(), response.getResponseCode());
    }
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void Z14TestuserLoginWithValidEmail() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.LOGIN.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_313131@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.SOURCE, "web");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("200 second"), Response.class);
    authToken =
        (String) ((Map<String, Object>) response.get(JsonKey.RESPONSE)).get(JsonKey.ACCESSTOKEN);
    System.out.println("Auth token :: " + authToken);
    if (null != response) {
      Assert.assertEquals(response.getResponseCode().getResponseCode(),
          ResponseCode.OK.getResponseCode());
    }
  }

  @SuppressWarnings({"deprecation"})
  @Test
  public void Z14TestuserLoginWithValidPhone() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.LOGIN.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "9874561233");
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.SOURCE, "web");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void userchangePasswordFailure() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CHANGE_PASSWORD.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, "sunbird_dummy_user_313131@gmail.com");
    innerMap.put(JsonKey.PASSWORD, "password2");
    innerMap.put(JsonKey.NEW_PASSWORD, "password1");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void userchangePasswordSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.CHANGE_PASSWORD.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.PASSWORD, "password");
    innerMap.put(JsonKey.NEW_PASSWORD, "password1");
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void userchangePasswordSuccess2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.FORGOT_PASSWORD.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, ("sunbird_dummy_user_313131@gmail.com"));
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void userchangePasswordSuccess3() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.FORGOT_PASSWORD.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, ("sunbird_dummy_user_313131"));
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void userchangePasswordSuccess4WithInvalidUserName() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.FORGOT_PASSWORD.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USERNAME, ("sunbird_dummy_user_3131312"));
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z10TestUpdateUserLoginTime() {
    // setting emailVerified to true for testing
    String respo = ssoManager.setEmailVerifiedTrue(userId);
    System.out.println("respo " + respo);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.USER_CURRENT_LOGIN.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("200 second"), Response.class);
    assertEquals(JsonKey.SUCCESS, (String) response.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z11TestgetMediaTypes() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_MEDIA_TYPES.getValue());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z12TestprofileVisibility() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROFILE_VISIBILITY.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    List<String> privateFields = new ArrayList<>();
    privateFields.add(JsonKey.ADDRESS + "." + JsonKey.CITY);
    privateFields.add(JsonKey.EDUCATION + "." + JsonKey.YEAR_OF_PASSING);
    privateFields.add(JsonKey.JOB_PROFILE + "." + JsonKey.JOB_NAME);
    privateFields.add(JsonKey.SKILLS + "." + JsonKey.SKILL_NAME);
    List<String> publicFields = new ArrayList<>();
    publicFields.add(JsonKey.EDUCATION);
    innerMap.put(JsonKey.PRIVATE, privateFields);
    innerMap.put(JsonKey.PUBLIC, publicFields);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }


  @Test
  public void Z12TestprofileVisibilityWithGetDetailsByLoginId() {

    String encLoginId = "sunbird_dummy_user_313131@BLR";

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.LOGIN_ID, encLoginId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    List<String> fields = new ArrayList<>();
    // fields.add(JsonKey.COMPLETENESS);
    // fields.add(JsonKey.MISSING_FIELDS);
    fields.add(JsonKey.LAST_LOGIN_TIME);
    fields.add(JsonKey.TOPIC);
    request.put(JsonKey.FIELDS, fields);
    reqObj.setRequest(request);
    request.put(JsonKey.REQUESTED_BY, userId);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z12TestprofileVisibilityWithGetUserInfo() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_PROFILE.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    request.put(JsonKey.REQUESTED_BY, userId);
    String fields = JsonKey.LAST_LOGIN_TIME + "," + JsonKey.TOPIC;
    // fields.add(JsonKey.COMPLETENESS);
    // fields.add(JsonKey.MISSING_FIELDS);
    // fields.add(JsonKey.LAST_LOGIN_TIME);
    // fields.add(JsonKey.TOPIC);
    request.put(JsonKey.FIELDS, fields);
    reqObj.setRequest(request);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z12TestprofileVisibility2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROFILE_VISIBILITY.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    List<String> privateFields = new ArrayList<>();
    privateFields.add(JsonKey.ADDRESS);
    List<String> publicFields = new ArrayList<>();
    publicFields.add(JsonKey.EDUCATION);
    publicFields.add(JsonKey.ADDRESS);
    innerMap.put(JsonKey.PRIVATE, privateFields);
    innerMap.put(JsonKey.PUBLIC, publicFields);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z12TestprofileVisibilityForException() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROFILE_VISIBILITY.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, (userId + "456"));
    List<String> privateFields = new ArrayList<>();
    privateFields.add(JsonKey.ADDRESS);
    List<String> publicFields = new ArrayList<>();
    publicFields.add(JsonKey.EDUCATION);
    innerMap.put(JsonKey.PRIVATE, privateFields);
    innerMap.put(JsonKey.PUBLIC, publicFields);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z13TestgetUserDetails() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.getRequest().put(JsonKey.REGISTERED_ORG_ID, "as");
    reqObj.getRequest().put(JsonKey.ROOT_ORG_ID, "as");
    reqObj.setOperation(ActorOperations.DOWNLOAD_USERS.getValue());
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z15TestLogout() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.LOGOUT.getValue());
    reqObj.getRequest().put(JsonKey.AUTH_TOKEN, authToken);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z16TestAssignRoles() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ASSIGN_ROLES.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ID, (userId));
    request.put(JsonKey.ORGANISATION_ID, orgId);
    List<String> roles = new ArrayList<>();
    roles.add("CONTENT_REVIEWER");
    request.put(JsonKey.ROLES, roles);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z16TestAssignRolesWithoutUserId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ASSIGN_ROLES.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USERNAME, "sunbird_dummy_user_313131");
    request.put(JsonKey.PROVIDER, "BLR");
    request.put(JsonKey.ORGANISATION_ID, orgId);
    List<String> roles = new ArrayList<>();
    roles.add("CONTENT_REVIEWER");
    request.put(JsonKey.ROLES, roles);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z17TestAssignRolesWithoutOrgId() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ASSIGN_ROLES.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USERNAME, "sunbird_dummy_user_313131");
    request.put(JsonKey.EXTERNAL_ID, "EXT_ID_DUMMY");
    request.put(JsonKey.PROVIDER, "BLR");
    List<String> roles = new ArrayList<>();
    roles.add("CONTENT_REVIEWER");
    request.put(JsonKey.ROLES, roles);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    probe.expectMsgClass(duration("200 second"), Response.class);
    Map<String, Object> map = ElasticSearchUtil.getDataByIdentifier(
        ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), userId);
    System.out.println("Login Id " + map.get(JsonKey.LOGIN_ID));
    List<Map<String, Object>> usrOrgList =
        (List<Map<String, Object>>) map.get(JsonKey.ORGANISATIONS);
    for (Map<String, Object> usrOrg : usrOrgList) {
      if (orgId.equalsIgnoreCase((String) usrOrg.get(JsonKey.ID))) {
        assertTrue(((List<String>) map.get(JsonKey.ROLES)).contains("CONTENT_REVIEWER"));
      }
    }
  }

  @Test
  public void Z17TestAssignRolesWithoutOrgId2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ASSIGN_ROLES.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ID, userId);
    List<String> roles = new ArrayList<>();
    roles.add("CONTENT_REVIEWER");
    request.put(JsonKey.ROLES, roles);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z18TestJoinUserOrganisation() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.JOIN_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, (userId));
    innerMap.put(JsonKey.ORGANISATION_ID, orgId2);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z18TestJoinUserOrganisation2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.JOIN_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, (userId));
    innerMap.put(JsonKey.ORGANISATION_ID, orgId);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response response = probe.expectMsgClass(duration("200 second"), Response.class);
    assertEquals(((String) response.getResult().get(JsonKey.RESPONSE)),
        "User already joined the organisation");

  }

  @Test
  public void Z18TestJoinUserOrganisation3() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.JOIN_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.ORGANISATION_ID, (orgId2 + "456as"));
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z18TestJoinUserOrganisation4() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.JOIN_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, null);
    innerMap.put(JsonKey.ORGANISATION_ID, null);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z19TestapproveUserOrg() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.APPROVE_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.ORGANISATION_ID, orgId2);
    List<String> roles = new ArrayList<>();
    roles.add("PUBLIC");
    innerMap.put(JsonKey.ROLES, roles);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z19TestapproveUserOrg2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.APPROVE_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, null);
    innerMap.put(JsonKey.ORGANISATION_ID, orgId2);
    List<String> roles = new ArrayList<>();
    roles.add("PUBLIC");
    innerMap.put(JsonKey.ROLES, roles);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z19TestapproveUserOrg3() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.APPROVE_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.ORGANISATION_ID, (orgId2 + "123sd"));
    List<String> roles = new ArrayList<>();
    roles.add("PUBLIC");
    innerMap.put(JsonKey.ROLES, roles);
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z20TestRejectUserOrg() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REJECT_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.ORGANISATION_ID, (orgId2 + "123sd"));
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z20TestRejectUserOrg2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REJECT_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.ORGANISATION_ID, (orgId2));
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z20TestRejectUserOrg3() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REJECT_USER_ORGANISATION.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, null);
    innerMap.put(JsonKey.ORGANISATION_ID, (orgId2));
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, innerMap);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z20TestRejectUserOrg4() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.REJECT_USER_ORGANISATION.getValue());
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER_ORG, null);
    reqObj.setRequest(request);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException exc =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z21TestAddUserBadge() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(badgeProps);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_USER_BADGE.getValue());
    reqObj.getRequest().put(JsonKey.RECEIVER_ID, userId);
    reqObj.getRequest().put(JsonKey.BADGE_TYPE_ID, "0123206539020943360");
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    subject.tell(reqObj, probe.getRef());

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void Z21TestAddUserBadge2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(badgeProps);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_USER_BADGE.getValue());
    reqObj.getRequest().put(JsonKey.RECEIVER_ID, "46789123");
    reqObj.getRequest().put(JsonKey.BADGE_TYPE_ID, "0123206539020943360");
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    subject.tell(reqObj, probe.getRef());

    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z21TestAddUserBadge3() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(badgeProps);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.ADD_USER_BADGE.getValue());
    reqObj.getRequest().put(JsonKey.RECEIVER_ID, userId);
    reqObj.getRequest().put(JsonKey.BADGE_TYPE_ID, "87915");
    reqObj.getRequest().put(JsonKey.REQUESTED_BY, userIdnew);
    subject.tell(reqObj, probe.getRef());

    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    assertTrue(null != exc);
  }

  @Test
  public void Z22TestgetUserBadge() {
    CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    Util.DbInfo userBadgesDbInfo = Util.dbInfoMap.get(JsonKey.USER_BADGES_DB);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    Map<String, Object> esResult = ElasticSearchUtil.getDataByIdentifier(
        ProjectUtil.EsIndex.sunbird.getIndexName(), ProjectUtil.EsType.user.getTypeName(), userId);
    List<Map<String, Object>> result = (List<Map<String, Object>>) esResult.get(JsonKey.BADGES);
    Map<String, Object> badge = result.get(0);
    assertTrue(((String) badge.get(JsonKey.BADGE_TYPE_ID)).equalsIgnoreCase("0123206539020943360"));
    if (((String) badge.get(JsonKey.BADGE_TYPE_ID)).equalsIgnoreCase("0123206539020943360")) {
      cassandraOperation.deleteRecord(userBadgesDbInfo.getKeySpace(),
          userBadgesDbInfo.getTableName(), ((String) badge.get(JsonKey.ID)));
    }

  }

  @Test
  public void z33TestsendEmail() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(emailServiceProps);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.EMAIL_SERVICE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> map = new HashMap<String, Object>();
    List<String> userids = new ArrayList<>();
    userids.add(userId);
    userids.add(userIdnew);
    userids.add("47867");
    map.put(JsonKey.RECIPIENT_USERIDS, userids);
    List<String> emails = new ArrayList<>();
    emails.add("sunbird_dummy_user_313131@gmail.com");
    emails.add("sunbird_dummy_user_31313100@gmail.com");
    map.put(JsonKey.RECIPIENT_EMAILS, emails);
    innerMap.put(JsonKey.EMAIL_REQUEST, map);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }


  @Test
  public void z33TestsendEmail2() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(emailServiceProps);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.EMAIL_SERVICE.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    Map<String, Object> map = new HashMap<String, Object>();
    List<String> userids = new ArrayList<>();
    userids.add(userId);
    userids.add(userIdnew);
    map.put(JsonKey.RECIPIENT_USERIDS, userids);
    List<String> emails = new ArrayList<>();
    emails.add("sunbird_dummy_user_313131@gmail.com");
    emails.add("sunbird_dummy_user_31313100@gmail.com");
    map.put(JsonKey.RECIPIENT_EMAILS, emails);
    innerMap.put(JsonKey.EMAIL_REQUEST, map);
    reqObj.setRequest(innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @AfterClass
  public static void deleteUser() {
    try {
      if ("ON".equalsIgnoreCase(encryption)) {
        Response res = operation.getRecordsByProperty(userManagementDB.getKeySpace(),
            userManagementDB.getTableName(), JsonKey.USERNAME,
            encryptionService.encryptData("sunbird_dummy_user_313131"));
        Map<String, Object> usrMap = ((Map<String, Object>) res.getResult().get(JsonKey.RESPONSE));
        if (null != usrMap.get(JsonKey.ID)) {
          userId = (String) usrMap.get(JsonKey.ID);
          System.out.println(userId);
        }
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    SSOManager ssoManager = SSOServiceFactory.getInstance();
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.USER_ID, userId);
    ssoManager.removeUser(innerMap);

    innerMap.put(JsonKey.USER_ID, userId2);
    ssoManager.removeUser(innerMap);

    operation.deleteRecord(userManagementDB.getKeySpace(), userManagementDB.getTableName(), userId);

    operation.deleteRecord(userManagementDB.getKeySpace(), userManagementDB.getTableName(),
        userId2);

    operation.deleteRecord(addressDB.getKeySpace(), addressDB.getTableName(), addressId);

    operation.deleteRecord(jobDB.getKeySpace(), jobDB.getTableName(), jobId);

    operation.deleteRecord(eduDB.getKeySpace(), eduDB.getTableName(), eduId);

    operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), orgId);

    operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), orgId2);

    operation.deleteRecord(userOrgDB.getKeySpace(), userOrgDB.getTableName(), userOrgId);

    ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(), userId);

    ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(), userId2);

    ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.organisation.getTypeName(), orgId);

    ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.organisation.getTypeName(), orgId2);
    try {
      Util.DbInfo usrExtIdDb = Util.dbInfoMap.get(JsonKey.USR_EXT_ID_DB);
      Response response = operation.getRecordsByProperty(usrExtIdDb.getKeySpace(),
          usrExtIdDb.getTableName(), JsonKey.USER_ID, userId);
      List<Map<String, Object>> mapList =
          (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      for (Map<String, Object> map : mapList) {
        operation.deleteRecord(usrExtIdDb.getKeySpace(), usrExtIdDb.getTableName(),
            (String) map.get(JsonKey.ID));
      }

      response = operation.getRecordsByProperty(usrExtIdDb.getKeySpace(), usrExtIdDb.getTableName(),
          JsonKey.USER_ID, userIdnew);
      mapList = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      for (Map<String, Object> map : mapList) {
        operation.deleteRecord(usrExtIdDb.getKeySpace(), usrExtIdDb.getTableName(),
            (String) map.get(JsonKey.ID));
      }

      response = operation.getRecordsByProperty(usrExtIdDb.getKeySpace(), usrExtIdDb.getTableName(),
          JsonKey.USER_ID, userId2);
      mapList = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
      for (Map<String, Object> map : mapList) {
        operation.deleteRecord(usrExtIdDb.getKeySpace(), usrExtIdDb.getTableName(),
            (String) map.get(JsonKey.ID));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    // To delete user data with webPage Data
    Map<String, Object> userMap = new HashMap<>();
    userMap.put(JsonKey.USER_ID, userIdnew);
    ssoManager.removeUser(userMap);
    operation.deleteRecord(userManagementDB.getKeySpace(), userManagementDB.getTableName(),
        userIdnew);

    ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.user.getTypeName(), userIdnew);
  }

}
