package org.sunbird.learner.actors.bulkupload;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.ProgressStatus;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.Application;
import org.sunbird.learner.util.Util;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;

/**
 * Created by arvind on 22/11/17.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BulkUploadManagementActorTest {

  private static ActorSystem system;
  private static CassandraOperation operation = ServiceFactory.getInstance();
  private static final Props props = Props.create(BulkUploadManagementActor.class);
  private static Util.DbInfo userDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
  private static final String USER_ID = "bcic783gfu239";
  private static String orgUploadProcessId = null;
  private static String userUploadProcessId = null;
  private static String orgUploadProcessId1 = null;
  private static String orgUploadProcessId2 = null;
  private static String userUploadProcessId1 = null;
  private static String orgId;
  private static String orgId1;
  private static String userId;
  private static Util.DbInfo orgDB = Util.dbInfoMap.get(JsonKey.ORG_DB);
  private static Util.DbInfo bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
  private static final String refOrgId = "id34fy";
  private static final String batchId = "batch78575ir8478";
  private static Util.DbInfo batchDbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
  private static SSOManager ssoManager = SSOServiceFactory.getInstance();
  private static Util.DbInfo userOrgdbInfo = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);


  @BeforeClass
  public static void setUp() {
    CassandraOperation operation = ServiceFactory.getInstance();
    Application.startLocalActorSystem();
    system = ActorSystem.create("system");
    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    // userManagementDB = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> userMap = new HashMap<>();
    userMap.put(JsonKey.ID, USER_ID);
    userMap.put(JsonKey.USER_ID, USER_ID);
    // userMap.put(JsonKey.ROOT_ORG_ID, ROOT_ORG_ID);
    operation.insertRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), userMap);
    userMap.put(JsonKey.USER_ID, USER_ID);
    userMap.put(JsonKey.ORGANISATION_ID, refOrgId);
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(), EsType.user.getTypeName(), USER_ID,
        userMap);

    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, refOrgId);
    operation.insertRecord(orgDB.getKeySpace(), orgDB.getTableName(), orgMap);

    Map<String, Object> batchMap = new HashMap<>();
    batchMap.put(JsonKey.ENROLLMENT_TYPE, JsonKey.INVITE_ONLY);
    batchMap.put(JsonKey.ID, batchId);
    List<String> courseCreatedFor = new ArrayList<>();
    courseCreatedFor.add(refOrgId);
    batchMap.put(JsonKey.COURSE_CREATED_FOR, courseCreatedFor);
    batchMap.put(JsonKey.COURSE_ID, "do_212282810555342848180");
    batchMap.put(JsonKey.STATUS, ProgressStatus.STARTED.getValue());
    operation.insertRecord(batchDbInfo.getKeySpace(), batchDbInfo.getTableName(), batchMap);

    batchMap.put(JsonKey.BATCH_ID, batchId);
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(), EsType.course.getTypeName(),
        USER_ID, batchMap);

    Map<String, Object> userOrgMap = new HashMap<>();
    userOrgMap.put(JsonKey.USER_ID, USER_ID);
    userOrgMap.put(JsonKey.ORGANISATION_ID, refOrgId);
    userOrgMap.put(JsonKey.ID, USER_ID);

    operation.insertRecord(userOrgdbInfo.getKeySpace(), userOrgdbInfo.getTableName(), userOrgMap);

  }

  @Test
  public void test11orgbulkUpload() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    File file = null;
    byte[] bytes = null;
    boolean orgProcessFlag = true;

    try {
      file = new File(BulkUploadManagementActorTest.class.getClassLoader()
          .getResource("BulkOrgUploadSample.csv").getFile());
      Path path = Paths.get(file.getPath());
      bytes = Files.readAllBytes(path);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(),e);
      orgProcessFlag = false;
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);
      orgProcessFlag = false;
    }

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.ORGANISATION);

    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);

    if (orgProcessFlag) {

      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(Response.class);
      orgUploadProcessId = (String) res.get(JsonKey.PROCESS_ID);
      System.out.println("PROCESS ID IS " + orgUploadProcessId);
    }
    Assert.assertTrue(null != orgUploadProcessId);
  }

  @Test
  public void test12orgbulkUploadWithRootOrg() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    File file = null;
    byte[] bytes = null;
    boolean orgProcessFlag = true;

    try {
      file = new File(BulkUploadManagementActorTest.class.getClassLoader()
          .getResource("BulkOrgUploadSampleWithRootOrgTrue.csv").getFile());
      Path path = Paths.get(file.getPath());
      bytes = Files.readAllBytes(path);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(),e);;
      orgProcessFlag = false;
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);;
      orgProcessFlag = false;
    }

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.ORGANISATION);

    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);

    if (orgProcessFlag) {

      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(Response.class);
      orgUploadProcessId2 = (String) res.get(JsonKey.PROCESS_ID);
      System.out.println("ROOT ORG PROCESS ID IS " + orgUploadProcessId2);

    }
    Assert.assertTrue(null != orgUploadProcessId2);
  }

  @Test
  public void test12orgbulkUploadWithRootOrgUpdate() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    File file = null;
    byte[] bytes = null;
    boolean orgProcessFlag = true;

    try {
      file = new File(BulkUploadManagementActorTest.class.getClassLoader()
          .getResource("BulkOrgUploadSampleWithRootOrgTrue.csv").getFile());
      Path path = Paths.get(file.getPath());
      bytes = Files.readAllBytes(path);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(),e);;
      orgProcessFlag = false;
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);;
      orgProcessFlag = false;
    }

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.ORGANISATION);

    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);

    if (orgProcessFlag) {

      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(Response.class);
      Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
    }

  }

  @Test
  public void test13orgbulkUploadforUpdate() {

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(),e);;
    }

    boolean orgProcessFlag = true;

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    File file = null;
    byte[] bytes = null;

    try {
      file = new File(BulkUploadManagementActorTest.class.getClassLoader()
          .getResource("BulkOrgUploadSample.csv").getFile());
      Path path = Paths.get(file.getPath());
      bytes = Files.readAllBytes(path);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(),e);;
      orgProcessFlag = false;
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);;
      orgProcessFlag = false;
    }

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.ORGANISATION);

    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);

    if (orgProcessFlag) {

      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(Response.class);
      orgUploadProcessId1 = (String) res.get(JsonKey.PROCESS_ID);
      System.out.println("PROCESS ID IS " + orgUploadProcessId);
      Assert.assertTrue(null != orgUploadProcessId1);
    }
  }

  @Test
  public void test12aaorgbulkUploadGetStatus() {

    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(),e);;
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_BULK_OP_STATUS.getValue());

    reqObj.getRequest().put(JsonKey.PROCESS_ID, orgUploadProcessId);

    if (null != orgUploadProcessId) {
      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(duration("200 second"), Response.class);
      List<Map<String, Object>> list = (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);
      if (!list.isEmpty()) {
        Map<String, Object> map = list.get(0);
        Object[] list1 = (Object[]) map.get(JsonKey.SUCCESS_RESULT);
        map = (Map<String, Object>) list1[0];

        orgId = (String) map.get(JsonKey.ID);
        Assert.assertTrue(null != orgId);
      }
    }

  }

  @Test
  public void ztest13aaorgbulkUploadGetStatusForRootOrg() {

    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(),e);;
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_BULK_OP_STATUS.getValue());

    reqObj.getRequest().put(JsonKey.PROCESS_ID, orgUploadProcessId2);

    if (null != orgUploadProcessId2) {
      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(duration("200 second"), Response.class);
      List<Map<String, Object>> list = (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);
      if (!list.isEmpty()) {
        Map<String, Object> map = list.get(0);
        Object[] list1 = (Object[]) map.get(JsonKey.SUCCESS_RESULT);
        map = (Map<String, Object>) list1[0];

        orgId1 = (String) map.get(JsonKey.ID);
        System.out.println("ROOT ORG ID " + orgId1);
        Assert.assertTrue(null != orgId1);
      }
    }

  }

  @Test
  public void test12userbulkUpload001() {

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(),e);;
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    File file = null;
    byte[] bytes = null;
    boolean userProcessFlag = true;

    try {
      file = new File(BulkUploadManagementActorTest.class.getClassLoader()
          .getResource("BulkUploadUserSample.csv").getFile());
      Path path = Paths.get(file.getPath());
      bytes = Files.readAllBytes(path);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(),e);;
      userProcessFlag = false;
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);;
      userProcessFlag = false;
    }

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    innerMap.put(JsonKey.ORGANISATION_ID, refOrgId);

    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);
    // reqObj.getRequest().put(JsonKey.ORGANISATION_ID , orgId);

    if (userProcessFlag) {

      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(Response.class);
      userUploadProcessId = (String) res.get(JsonKey.PROCESS_ID);
      System.out.println("PROCESS ID IS " + userUploadProcessId);
      Assert.assertTrue(null != userUploadProcessId);

    }
  }

  @Test
  public void test14userbulkUpload001Update() {

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(),e);;
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    File file = null;
    byte[] bytes = null;
    boolean userProcessFlag = true;

    try {
      file = new File(BulkUploadManagementActorTest.class.getClassLoader()
          .getResource("BulkUploadUserSample.csv").getFile());
      Path path = Paths.get(file.getPath());
      bytes = Files.readAllBytes(path);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(),e);;
      userProcessFlag = false;
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);;
      userProcessFlag = false;
    }

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    innerMap.put(JsonKey.ORGANISATION_ID, refOrgId);

    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);

    if (userProcessFlag) {

      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(Response.class);
      userUploadProcessId1 = (String) res.get(JsonKey.PROCESS_ID);
      System.out.println("PROCESS ID IS " + userUploadProcessId);
      Assert.assertTrue(null != userUploadProcessId1);

    }
  }

  @Test
  public void test16userubulkUpload001wGetStatus() {

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(),e);;
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_BULK_OP_STATUS.getValue());

    reqObj.getRequest().put(JsonKey.PROCESS_ID, userUploadProcessId);
    System.out.println("USER UPLOAD PROCESS ID " + userUploadProcessId);

    if (null != userUploadProcessId) {
      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(duration("200 second"), Response.class);
      /*
       * String respon = (String)res.get(JsonKey.RESPONSE); System.out.println(respon);
       */
      List<Map<String, Object>> list = (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);
      if (!list.isEmpty()) {
        Map<String, Object> map = list.get(0);
        Object[] list1 = (Object[]) map.get(JsonKey.SUCCESS_RESULT);
        map = (Map<String, Object>) list1[0];

        userId = (String) map.get(JsonKey.USER_ID);
        System.out.println("USER ID " + userId);
        Assert.assertTrue(null != userId);

      }
    }

  }

  @Test
  public void test16uploadBatch() {

    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      ProjectLogger.log(e.getMessage(),e);;
    }

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    File file = null;
    byte[] bytes = null;
    boolean battchProcessFlag = true;

    try {
      file = new File(BulkUploadManagementActorTest.class.getClassLoader()
          .getResource("BulkUploadBatchSample.csv").getFile());
      Path path = Paths.get(file.getPath());
      bytes = Files.readAllBytes(path);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(),e);;
      battchProcessFlag = false;
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(),e);;
      battchProcessFlag = false;
    }

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.BATCH);
    // innerMap.put(JsonKey.ORGANISATION_ID , refOrgId);

    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);
    // reqObj.getRequest().put(JsonKey.ORGANISATION_ID , orgId);

    if (battchProcessFlag) {

      subject.tell(reqObj, probe.getRef());
      Response res = probe.expectMsgClass(duration("3000 second"), Response.class);
      Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
    }
  }

  @AfterClass
  public static void destroy() {

    Map<String, Object> dbMap = new HashMap<>();
    dbMap.put(JsonKey.ID, USER_ID);
    Response result = operation.getRecordsByProperties(userDbInfo.getKeySpace(),
        userDbInfo.getTableName(), dbMap);
    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!(list.isEmpty())) {
      for (Map<String, Object> res : list) {
        String id = (String) res.get(JsonKey.ID);
        System.out.println("ID is " + id);
        operation.deleteRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), id);
        ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
            EsType.user.getTypeName(), id);
      }
    }

    try {
      if (null != userId) {
        dbMap.put(JsonKey.ID, userId);
        Map<String, Object> ssoMap = new HashMap<>();
        ssoMap.put(JsonKey.USER_ID, userId);
        ssoManager.removeUser(ssoMap);
        result = operation.getRecordsByProperties(userDbInfo.getKeySpace(),
            userDbInfo.getTableName(), dbMap);
        list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (!(list.isEmpty())) {
          for (Map<String, Object> res : list) {
            String id = (String) res.get(JsonKey.ID);
            System.out.println("ID is " + id);
            operation.deleteRecord(userDbInfo.getKeySpace(), userDbInfo.getTableName(), id);
            ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
                EsType.user.getTypeName(), id);
          }
        }
      }
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }

    if (null != orgId) {
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), orgId);
      ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
          EsType.organisation.getTypeName(), orgId);
    }

    if (null != orgId1) {
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), orgId1);
      ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
          EsType.organisation.getTypeName(), orgId1);
    }

    if (null != batchId) {
      operation.deleteRecord(batchDbInfo.getKeySpace(), batchDbInfo.getTableName(), batchId);
      ElasticSearchUtil.removeData(ProjectUtil.EsIndex.sunbird.getIndexName(),
          EsType.course.getTypeName(), batchId);
    }

    if (null != refOrgId) {
      operation.deleteRecord(orgDB.getKeySpace(), orgDB.getTableName(), refOrgId);
    }
    if (null != orgUploadProcessId) {
      operation.deleteRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), orgUploadProcessId);
    }
    if (null != orgUploadProcessId1) {
      operation.deleteRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), orgUploadProcessId1);
    }
    if (null != userUploadProcessId) {
      operation.deleteRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), userUploadProcessId);
    }
    if (null != userUploadProcessId1) {
      operation.deleteRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), userUploadProcessId1);
    }
  }
}
