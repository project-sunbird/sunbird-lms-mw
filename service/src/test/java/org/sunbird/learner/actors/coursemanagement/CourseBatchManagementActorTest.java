package org.sunbird.learner.actors.coursemanagement;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.text.SimpleDateFormat;
import java.util.*;
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
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.CourseBatchManagementActor;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class CourseBatchManagementActorTest {

  private TestKit probe;
  private ActorRef subject;

  private static CassandraOperationImpl mockCassandraOperation;
  private static final String BATCH_ID = "0125728227995648004";
  private static final String BATCH_NAME = "kirtitest3333";
  SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

  @Before
  public void setUp() {
    mockCassandraOperation = mock(CassandraOperationImpl.class);

    ActorSystem system = ActorSystem.create("system");
    probe = new TestKit(system);

    Props props = Props.create(CourseBatchManagementActor.class, mockCassandraOperation);
    subject = system.actorOf(props);

    PowerMockito.mockStatic(ServiceFactory.class);
    when(ServiceFactory.getInstance()).thenReturn(mockCassandraOperation);
  }

  String startDate_String = "";
  String endDate_String = "";
  String pastStartDate_String = "";
  String pastEndDate_String = "";
  static String globalExistingStartDate_String = "";
  static String globalExistingEndDate_String = "";

  Calendar calendar1 = Calendar.getInstance();
  Calendar calendar2 = Calendar.getInstance();
  Calendar calendar6 = Calendar.getInstance();

  public void defineStartAndEndDate() {

    calendar6.add(Calendar.DAY_OF_MONTH, -2);
    calendar1.add(Calendar.DAY_OF_MONTH, 2);
    calendar2.add(Calendar.DAY_OF_MONTH, 6);

    Date startDate = calendar1.getTime();
    startDate_String = format.format(startDate);
    Date endDate = calendar2.getTime();
    endDate_String = format.format(endDate);
    Date pastStartDate = calendar6.getTime();
    pastStartDate_String = format.format(pastStartDate);
    Date pastEndDate = pastStartDate;
    pastEndDate_String = format.format(pastEndDate);
  }

  private ProjectCommonException updateCourseBatchFailure(
      String startDate, String endDate, Response mockCassandraResponse) {
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(mockCassandraResponse);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, BATCH_ID);
    innerMap.put(JsonKey.NAME, BATCH_NAME);
    innerMap.put(JsonKey.START_DATE, startDate);
    innerMap.put(JsonKey.END_DATE, endDate);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);

    subject.tell(reqObj, probe.getRef());

    ProjectCommonException exc =
        probe.expectMsgClass(duration("10000 second"), ProjectCommonException.class);
    return exc;
  }

  private Response updateCourseBatch(
      String startDate,
      String endDate,
      Response mockCassandraResponse,
      Response mockCassandraUpdateResult) {

    // we are mocking response from cassandra
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(mockCassandraResponse);

    // mocking result
    when(mockCassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(mockCassandraUpdateResult);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, BATCH_ID);
    innerMap.put(JsonKey.NAME, BATCH_NAME);

    if (startDate != null) {
      innerMap.put(JsonKey.START_DATE, startDate);
    }
    if (endDate != null) {
      innerMap.put(JsonKey.END_DATE, endDate);
    }
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);

    subject.tell(reqObj, probe.getRef());

    Response res = probe.expectMsgClass(duration("10000 second"), Response.class);
    return res;
  }

  @Test(expected = ProjectCommonException.class)
  public void testUpdateStartedCourseBatchFailureWithStartDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    ProjectCommonException ex =
        updateCourseBatchFailure(startDate_String, null, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  // kirti sagar
  @Test
  public void testUpdateStartedCourseBatchFailureWithEndDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    Response mockCassandraUpdateResult = getMockCassandraResult();
    ProjectCommonException ex =
        updateCourseBatchFailure(null, endDate_String, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateStartedCourseBatchFailureWithDifferentStartDateAndEndDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    Response mockCassandraUpdateResult = getMockCassandraResult();

    ProjectCommonException ex =
        updateCourseBatchFailure(startDate_String, endDate_String, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateStartedCourseBatchSuccessWithSameStartDateAndEndDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    Response mockCassandraUpdateResult = getMockCassandraResult();
    Response res =
        updateCourseBatch(
            globalExistingStartDate_String,
            globalExistingEndDate_String,
            mockCassandraResponse,
            mockCassandraUpdateResult);
    Assert.assertTrue(null != res);
  }

  // kirti
  // course has not started yet
  @Test
  public void testUpdateNotStartedCourseBatchSuccessWithFutureStartDateAndEndDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    Response mockCassandraUpdateResult = getMockCassandraResult();
    Response res =
        updateCourseBatch(
            startDate_String, endDate_String, mockCassandraResponse, mockCassandraUpdateResult);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testUpdateNotStartedCourseBatchSuccessWithFutureStartDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    Response mockCassandraUpdateResult = getMockCassandraResult();
    Response res =
        updateCourseBatch(startDate_String, null, mockCassandraResponse, mockCassandraUpdateResult);
    Assert.assertTrue(null != res);
  }

  @Test
  public void testUpdateNotStartedCourseBatchSuccessWithFutureEndDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    Response mockCassandraUpdateResult = getMockCassandraResult();
    Response res =
        updateCourseBatch(null, endDate_String, mockCassandraResponse, mockCassandraUpdateResult);
    Assert.assertTrue(null != res);
  }

  // new scenarios
  @Test
  public void testUpdateNotStartedCourseBatchFailureWithPastStartDate() {

    int courseStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);

    ProjectCommonException ex =
        updateCourseBatchFailure(pastStartDate_String, null, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateNotStartedCourseBatchFailureWithPastEndDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    ProjectCommonException ex =
        updateCourseBatchFailure(null, pastEndDate_String, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.courseBatchEndDateError.getErrorCode()));
  }

  @Test
  public void testUpdateNotStartedCourseBatchFailureWithEndDateBeforeFutureStartDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    ProjectCommonException ex =
        updateCourseBatchFailure(startDate_String, pastEndDate_String, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  // course Ended
  // check for postman exception status
  @Test
  // start date required
  public void testUpdateCompletedCourseBatchFailureWithEndDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.COMPLETED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    ProjectCommonException ex =
        updateCourseBatchFailure(null, endDate_String, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.courseBatchEndDateError.getErrorCode()));
  }

  @Test
  public void testUpdateCompletedCourseBatchFailureWithStartDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.COMPLETED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    ProjectCommonException ex =
        updateCourseBatchFailure(startDate_String, null, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateCompletedCourseBatchFailureWithStartDateAndEndDate() {

    defineStartAndEndDate();
    int courseStatus = ProjectUtil.ProgressStatus.COMPLETED.getValue();
    Response mockCassandraResponse = getMockCassandraRecordByIdResponse(courseStatus);
    ProjectCommonException ex =
        updateCourseBatchFailure(startDate_String, endDate_String, mockCassandraResponse);
    Assert.assertTrue(
        ((ProjectCommonException) ex)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  private Response getMockCassandraResult() {

    Response response = new Response();
    response.put("response", "SUCCESS");
    return response;
  }

  private Response getMockCassandraRecordByIdResponse(int courseStatus) {

    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> courseResponseMap = new HashMap<>();

    courseResponseMap.put(JsonKey.ID, BATCH_ID);
    courseResponseMap.put(JsonKey.VER, "v1");
    courseResponseMap.put(JsonKey.NAME, BATCH_NAME);
    courseResponseMap.put(JsonKey.COUNTER_INCREMENT_STATUS, "false");
    courseResponseMap.put(JsonKey.ENROLMENTTYPE, "Invite-only");
    courseResponseMap.put(JsonKey.COURSE_ID, "do_2125635353836584961866");
    courseResponseMap.put(JsonKey.COURSE_CREATED_FOR, new ArrayList<Object>());
    courseResponseMap.put(JsonKey.STATUS, courseStatus);

    Date existingStartDate = null;
    Date existingEndDate = null;
    Calendar calendar = Calendar.getInstance();
    Calendar calendar3 = Calendar.getInstance();
    if (courseStatus == 1) {
      calendar.add(Calendar.DAY_OF_MONTH, -4);
      calendar3.add(Calendar.DAY_OF_MONTH, 3);
      existingStartDate = calendar.getTime();
      existingEndDate = calendar3.getTime();

      globalExistingStartDate_String = format.format(existingStartDate);
      globalExistingEndDate_String = format.format(existingEndDate);

      courseResponseMap.put(JsonKey.START_DATE, globalExistingStartDate_String);
      courseResponseMap.put(JsonKey.END_DATE, globalExistingEndDate_String);
    } else if (courseStatus == 0) {
      calendar.add(Calendar.DAY_OF_MONTH, 1);
      calendar3.add(Calendar.DAY_OF_MONTH, 2);
      existingStartDate = calendar.getTime();
      existingEndDate = calendar3.getTime();

      courseResponseMap.put(JsonKey.START_DATE, "2018-09-05");
      courseResponseMap.put(JsonKey.END_DATE, "2018-09-10");

      globalExistingStartDate_String = format.format(existingStartDate);
      globalExistingEndDate_String = format.format(existingEndDate);
      //        responseMap.put(JsonKey.START_DATE, globalExistingStartDate_String);
      //        responseMap.put(JsonKey.END_DATE, globalExistingEndDate_String);
    } else {
      calendar.add(Calendar.DAY_OF_MONTH, -5);
      calendar3.add(Calendar.DAY_OF_MONTH, -2);
      existingStartDate = calendar.getTime();
      existingEndDate = calendar3.getTime();

      globalExistingStartDate_String = format.format(existingStartDate);
      globalExistingEndDate_String = format.format(existingEndDate);
      courseResponseMap.put(JsonKey.START_DATE, globalExistingStartDate_String);
      courseResponseMap.put(JsonKey.END_DATE, globalExistingEndDate_String);
    }
    list.add(courseResponseMap);
    response.put(JsonKey.RESPONSE, list);

    return response;
  }
}
