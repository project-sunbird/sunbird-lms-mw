package org.sunbird.learner.actors.bulkupload;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;

/** @author arvind. Junit test cases for bulk upload - user, org, batch. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class BulkUploadManagementActorTest {

  private TestKit probe;
  private ActorRef subject;
  private static final String USER_ID = "bcic783gfu239";
  private static final String refOrgId = "id34fy";
  private static final String PROCESS_ID = "process-13647-fuzzy";
  private static final String rootOrgID = "0125883816782069760";
  private static CassandraOperationImpl mockCassandraOperation;

  @Before
  public void setUp() {
    mockCassandraOperation = mock(CassandraOperationImpl.class);

    ActorSystem system = ActorSystem.create("system");
    probe = new TestKit(system);

    Props props = Props.create(BulkUploadManagementActor.class);
    subject = system.actorOf(props);

    PowerMockito.mockStatic(ServiceFactory.class);
    when(ServiceFactory.getInstance()).thenReturn(mockCassandraOperation);
  }

  @Test
  public void testOrgBulkUploadCreateOrgSuccess() {

    byte[] bytes = getFileAsBytes("BulkOrgUploadSample.csv");

    Response response = createCassandraInsertSuccessResponse();
    when(mockCassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(response);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.ORGANISATION);
    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10000 second"), Response.class);
    String uploadProcessId = (String) res.get(JsonKey.PROCESS_ID);
    Assert.assertTrue(null != uploadProcessId);
  }

  @Test
  public void testOrgBulkUploadCreateOrgWithInvalidHeaders() {

    String headerLine = "batchId,orgName,isRootOrg,channel";
    String firstLine = "batch78575ir8478,hello001,false,,1119";
    StringBuilder builder = new StringBuilder();
    builder.append(headerLine).append("\n").append(firstLine);

    Response response = createCassandraInsertSuccessResponse();
    when(mockCassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(response);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.ORGANISATION);
    innerMap.put(JsonKey.FILE, builder.toString().getBytes());
    reqObj.getRequest().put(JsonKey.DATA, innerMap);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testBulkUploadGetStatus() {
    Response response = getCassandraRecordByIdForBulkUploadResponse();
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenReturn(response);
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_BULK_OP_STATUS.getValue());
    reqObj.getRequest().put(JsonKey.PROCESS_ID, PROCESS_ID);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    List<Map<String, Object>> list = (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);
    if (!list.isEmpty()) {
      Map<String, Object> map = list.get(0);
      String processId = (String) map.get(JsonKey.PROCESS_ID);
      Assert.assertTrue(null != processId);
    }
  }

  @Test
  public void testUserBulkUploadCreateUserWithInvalidHeaders() {

    String headerLine = "batchId,firstName,lastName,phone";
    String firstLine = "batch78575ir8478,xyz1234516,Kumar15,9000000011";
    StringBuilder builder = new StringBuilder();
    builder.append(headerLine).append("\n").append(firstLine);

    Response response = getCassandraRecordByIdForOrgResponse();
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);
    Response insertResponse = createCassandraInsertSuccessResponse();
    when(mockCassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(insertResponse);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    innerMap.put(JsonKey.ORGANISATION_ID, refOrgId);

    innerMap.put(JsonKey.FILE, builder.toString().getBytes());
    reqObj.getRequest().put(JsonKey.DATA, innerMap);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testBatchBulkUploadCreateBatchSuccess() {

    String headerLine = "batchId,userIds";
    String firstLine = "batch78575ir8478,\"bcic783gfu239,nhhuc37i5t8,h7884f7t8\"";
    StringBuilder builder = new StringBuilder();
    builder.append(headerLine).append("\n").append(firstLine);

    Response insertResponse = createCassandraInsertSuccessResponse();
    when(mockCassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(insertResponse);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.BATCH);

    innerMap.put(JsonKey.FILE, builder.toString().getBytes());
    reqObj.getRequest().put(JsonKey.DATA, innerMap);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("300 second"), Response.class);
    String processId = (String) res.get(JsonKey.PROCESS_ID);
    Assert.assertTrue(null != processId);
  }

  @Test
  public void testBatchBulkUploadWithInvalidFileHeaders() {

    String headerLine = "batchId,userIds,orgId";
    String firstLine = "batch78575ir8478,\"bcic783gfu239,nhhuc37i5t8,h7884f7t8\",org123";
    StringBuilder builder = new StringBuilder();
    builder.append(headerLine).append("\n").append(firstLine);

    Response insertResponse = createCassandraInsertSuccessResponse();
    when(mockCassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(insertResponse);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();

    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.BATCH);

    innerMap.put(JsonKey.FILE, builder.toString().getBytes());
    reqObj.getRequest().put(JsonKey.DATA, innerMap);

    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testUserBulkUploadFailureWithSamePhoneNumbers() {

    byte[] bytes = getFileAsBytes("BulkUploadUserSameNum.csv");
    Response response = createCassandraInsertSuccessResponse();
    when(mockCassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(response);

    Response mockGetFailureResponse = getMockFailureResponse();
    Response mockGetOrganisationResponse = getMockOrganisationResponse();

    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(mockGetOrganisationResponse);

    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenReturn(mockGetFailureResponse);

    Response failureResponse = getCassandraRecordBulkUploadResponseUserFailure();
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenReturn(failureResponse);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.ORGANISATION_ID, refOrgId);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    String uploadProcessId = (String) res.get(JsonKey.PROCESS_ID);

    ActorSystem system = ActorSystem.create("system");
    probe = new TestKit(system);

    Request reqGetObj = new Request();
    reqGetObj.setOperation(ActorOperations.GET_BULK_OP_STATUS.getValue());
    reqGetObj.getRequest().put(JsonKey.PROCESS_ID, uploadProcessId);
    subject.tell(reqGetObj, probe.getRef());
    Response resp = probe.expectMsgClass(duration("10 second"), Response.class);
    List<Map<String, Object>> list = (List<Map<String, Object>>) resp.get(JsonKey.RESPONSE);
    if (!list.isEmpty()) {
      Map<String, Object> map = list.get(0);
      String failureMsg = (String) map.get(JsonKey.FAILURE_RESULT);
      Assert.assertTrue(null != failureMsg);
    }
  }

  private Response getMockOrganisationResponse() {

    Response response = new Response();
    List<Map<String, Object>> responseList = new ArrayList();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.DESCRIPTION, "description");
    map.put(JsonKey.CHANNEL, "abc");
    map.put(JsonKey.IS_ROOT_ORG, true);
    map.put(JsonKey.ROOT_ORG_ID, rootOrgID);
    responseList.add(map);
    response.put(JsonKey.RESPONSE, responseList);
    return response;
  }

  @Test
  public void testUserBulkUploadSuccessWithDifferentPhoneNumbers() {

    byte[] bytes = getFileAsBytes("BulkUploadUserDiffNum.csv");
    Response response = createCassandraInsertSuccessResponse();
    when(mockCassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(response);

    Response mockGetFailureResponse = getMockFailureResponse();
    Response mockGetOrganisationResponse = getMockOrganisationResponse();

    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(mockGetOrganisationResponse);

    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenReturn(mockGetFailureResponse);

    Response successResponse = getCassandraRecordBulkUploadResponseUserSuccess();
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenReturn(successResponse);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.BULK_UPLOAD.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.ORGANISATION_ID, refOrgId);
    innerMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    innerMap.put(JsonKey.FILE, bytes);
    reqObj.getRequest().put(JsonKey.DATA, innerMap);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    String uploadProcessId = (String) res.get(JsonKey.PROCESS_ID);

    ActorSystem system = ActorSystem.create("system");
    probe = new TestKit(system);

    Request reqGetObj = new Request();
    reqGetObj.setOperation(ActorOperations.GET_BULK_OP_STATUS.getValue());
    reqGetObj.getRequest().put(JsonKey.PROCESS_ID, uploadProcessId);
    subject.tell(reqGetObj, probe.getRef());
    Response resp = probe.expectMsgClass(duration("10 second"), Response.class);
    List<Map<String, Object>> list = (List<Map<String, Object>>) resp.get(JsonKey.RESPONSE);
    if (!list.isEmpty()) {
      Map<String, Object> map = list.get(0);
      String failureMsg = (String) map.get(JsonKey.SUCCESS_RESULT);
      Assert.assertTrue(null != failureMsg);
    }
  }

  private Response getMockFailureResponse() {

    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> mainMap = new HashMap<>();
    mainMap.put(JsonKey.FAILURE_RESULT, "any thing");
    mainMap.put(JsonKey.SUCCESS_RESULT, null);
    mainMap.put(JsonKey.ID, PROCESS_ID);
    mainMap.put(JsonKey.OBJECT_TYPE, "user");
    mainMap.put(JsonKey.STATUS, "3");
    list.add(mainMap);
    response.put(JsonKey.RESPONSE, list);

    return response;
  }

  private Response createCassandraInsertSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private Response getCassandraRecordByIdForBulkUploadResponse() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> bulkUploadProcessMap = new HashMap<>();
    bulkUploadProcessMap.put(JsonKey.ID, "123");
    bulkUploadProcessMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadProcessMap.put(JsonKey.OBJECT_TYPE, JsonKey.ORGANISATION);
    list.add(bulkUploadProcessMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Response getCassandraRecordBulkUploadResponseUserFailure() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> bulkUploadProcessMap = new HashMap<>();

    bulkUploadProcessMap.put(JsonKey.ID, "123");
    bulkUploadProcessMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadProcessMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    bulkUploadProcessMap.put(JsonKey.FAILURE_RESULT, "3f1ed5c2-0e44-47cb-b80c-a1e778c8b842");
    list.add(bulkUploadProcessMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Response getCassandraRecordBulkUploadResponseUserSuccess() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> bulkUploadProcessMap = new HashMap<>();

    bulkUploadProcessMap.put(JsonKey.ID, "123");
    bulkUploadProcessMap.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    bulkUploadProcessMap.put(JsonKey.OBJECT_TYPE, JsonKey.USER);
    bulkUploadProcessMap.put(JsonKey.SUCCESS_RESULT, "3f1ed5c2-0e44-47cb-b80c-a1e778c8b842");
    list.add(bulkUploadProcessMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Response getCassandraRecordByIdForOrgResponse() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, "org123");
    list.add(orgMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private byte[] getFileAsBytes(String fileName) {
    File file = null;
    byte[] bytes = null;
    try {
      file =
          new File(
              BulkUploadManagementActorTest.class.getClassLoader().getResource(fileName).getFile());
      Path path = Paths.get(file.getPath());
      bytes = Files.readAllBytes(path);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(), e);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return bytes;
  }
}
