package org.sunbird.learner.actors.bulkupload;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;

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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.BaseActorTest;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class BulkUploadManagementActorTest extends BaseActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(BulkUploadManagementActor.class);
  private static final String USER_ID = "bcic783gfu239";
  private static final String refOrgId = "id34fy";
  private static final String PROCESS_ID = "process-13647-fuzzy";

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
  }

  @Test
  public void testOrgBulkUploadCreateOrgSuccess() {

    boolean result =
        testScenario(
            getRequest(ActorOperations.BULK_UPLOAD, true, false, JsonKey.ORGANISATION, true),
            true,
            false);
    assertTrue(result);
  }

  @Test
  public void testOrgBulkUploadCreateOrgWithInvalidHeaders() {

    boolean result =
        testScenario(
            getRequest(ActorOperations.BULK_UPLOAD, false, false, JsonKey.ORGANISATION, false),
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testBulkUploadGetStatus() {

    boolean result =
        testScenario(
            getRequest(ActorOperations.GET_BULK_OP_STATUS, false, true, JsonKey.ORGANISATION, true),
            true,
            true);
    assertTrue(result);
  }

  @Test
  public void testUserBulkUploadCreateUserSuccess() {

    boolean result =
        testScenario(
            getRequest(ActorOperations.BULK_UPLOAD, false, false, JsonKey.USER, true), true, false);
    assertTrue(result);
  }

  @Test
  public void testUserBulkUploadCreateUserWithInvalidHeaders() {

    boolean result =
        testScenario(
            getRequest(ActorOperations.BULK_UPLOAD, false, false, JsonKey.USER, false),
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testBatchBulkUploadCreateBatchSuccess() {

    boolean result =
        testScenario(
            getRequest(ActorOperations.BULK_UPLOAD, false, false, JsonKey.BATCH, true),
            true,
            false);
    assertTrue(result);
  }

  @Test
  public void testBatchBulkUploadWithInvalidFileHeaders() {

    boolean result =
        testScenario(
            getRequest(ActorOperations.BULK_UPLOAD, false, false, JsonKey.BATCH, false),
            false,
            false);
    assertTrue(result);
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

  private byte[] getBytes(String objectType, boolean isSuccess) {

    String headerLine = null;
    String firstLine = null;

    if (!isSuccess) {
      headerLine = "batchId,firstName,lastName,phone";
      firstLine = "batch123,xyz1234516,Kumar15,9000000011";
    } else if (objectType.equals(JsonKey.ORGANISATION)) {
      headerLine = "batchId,orgName,isRootOrg,channel";
      firstLine = "batch78575ir8478,hello001,false,,1119";
    } else if (objectType.equals(JsonKey.USER)) {
      headerLine = "firstName,lastName,phone";
      firstLine = "xyz1234516,Kumar15,9000000011";
    } else if (objectType.equals(JsonKey.BATCH)) {
      headerLine = "batchId,userIds";
      firstLine = "batch78575ir8478,\"bcic783gfu239,nhhuc37i5t8,h7884f7t8\"";
    }

    StringBuilder builder = new StringBuilder();
    builder.append(headerLine).append("\n").append(firstLine);
    return builder.toString().getBytes();
  }

  private boolean testScenario(Request reqObj, boolean isSuccess, boolean isGetRequest) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(reqObj, probe.getRef());

    if (isSuccess) {
      Response res = probe.expectMsgClass(duration("1000 second"), Response.class);

      if (isGetRequest) {
        List<Map<String, Object>> list = (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);
        if (!list.isEmpty()) {
          Map<String, Object> map = list.get(0);
          String processId = (String) map.get(JsonKey.PROCESS_ID);
          return null != processId;
        }
      } else {
        return null != res && res.getResponseCode() == ResponseCode.OK;
      }
    } else {
      ProjectCommonException res =
          probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
      return null != res;
    }
    return false;
  }

  private Request getRequest(
      ActorOperations actorOperation,
      boolean fromFile,
      boolean isGetRequest,
      String objectType,
      boolean isSuccess) {

    byte[] bytes = null;
    if (!isGetRequest) {
      if (fromFile) {
        bytes = getFileAsBytes("BulkOrgUploadSample.csv");
      } else {
        bytes = getBytes(objectType, isSuccess);
      }
    }

    Request reqObj = new Request();

    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CREATED_BY, USER_ID);
    innerMap.put(JsonKey.OBJECT_TYPE, objectType);
    innerMap.put(JsonKey.ORGANISATION_ID, refOrgId);

    if (!isGetRequest) {
      innerMap.put(JsonKey.FILE, bytes);
      reqObj.getRequest().put(JsonKey.PROCESS_ID, PROCESS_ID);
    }

    reqObj.getRequest().put(JsonKey.DATA, innerMap);
    reqObj.setOperation(actorOperation.getValue());
    return reqObj;
  }

  @Override
  public Response getRecordByIdCassandra() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, "org123");
    orgMap.put(JsonKey.IS_ROOT_ORG, true);
    list.add(orgMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  @Override
  protected List<Response> getResponseList(
      boolean isLocation, boolean isFirstRequired, boolean isSecondRequired) {
    return null;
  }

  @Override
  protected Map<String, Object> getDataByIdentifierElasticSearch() {
    return null;
  }

  @Override
  public Response getRecordByIdWithFieldsCassandra() {
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
}
