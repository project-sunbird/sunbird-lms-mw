package org.sunbird.learner.actors.coursemanagement;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.collections.map.HashedMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.CourseBatchManagementActor;
import org.sunbird.learner.util.EkStepRequestUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EkStepRequestUtil.class, ServiceFactory.class, ElasticSearchUtil.class})
@PowerMockIgnore("javax.management.*")
public class CourseBatchManagementActorTest {

  public static final String ROOT_ORG = "ROOT_ORG";
  public static final String OTHER_ORG = "OTHER_ORG";
  public static final int VALUE = 10;
  public static final String COURSE_ID = "courseId";
  public static final String SOME_NAME = "someName";
  public static final String DESC = "desc";
  public static final String INVITE_ONLY = "invite-only";
  public static final String START_DATE = "startDate";
  public static final String MENTORS = "Mentors";
  private TestKit probe;
  private ActorRef subject;

  private static CassandraOperation mockCassandraOperation;
  private static final String BATCH_ID = "123";
  private static final String BATCH_NAME = "Some Batch Name";
  private static final String USER_ID = "USER_ID";

  SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
  private Request actorMessage;
  private String existingStartDate = "";
  private String existingEndDate = "";

  @Before
  public void setUp() {
    PowerMockito.mockStatic(ServiceFactory.class);
    mockCassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(mockCassandraOperation);
    PowerMockito.mockStatic(EkStepRequestUtil.class);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    ActorSystem system = ActorSystem.create("system");
    probe = new TestKit(system);

    Props props = Props.create(CourseBatchManagementActor.class);



    subject = system.actorOf(props);
    actorMessage = new Request();

  }

  private String calculateDate(int dayOffset) {

    Calendar calender = Calendar.getInstance();
    calender.add(Calendar.DAY_OF_MONTH, dayOffset);
    return format.format(calender.getTime());
  }

  private ProjectCommonException performUpdateCourseBatchFailureTest(
      String startDate, String endDate, Response mockGetRecordByIdResponse) {
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(mockGetRecordByIdResponse);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.UPDATE_BATCH.getValue());
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, BATCH_ID);
    innerMap.put(JsonKey.NAME, BATCH_NAME);
    innerMap.put(JsonKey.START_DATE, startDate);
    innerMap.put(JsonKey.END_DATE, endDate);
    reqObj.getRequest().put(JsonKey.BATCH, innerMap);

    subject.tell(reqObj, probe.getRef());

    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    return exception;
  }

  private Response performUpdateCourseBatchSuccessTest(
      String startDate,
      String endDate,
      Response mockGetRecordByIdResponse,
      Response mockUpdateRecordResponse) {

    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(mockGetRecordByIdResponse);

    when(mockCassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(mockUpdateRecordResponse);

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

    Response response = probe.expectMsgClass(duration("10 second"), Response.class);
    return response;
  }

  private Response getMockCassandraResult() {

    Response response = new Response();
    response.put("response", "SUCCESS");
    return response;
  }

  private Response getMockCassandraRecordByIdResponse(int batchProgressStatus) {

    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> courseResponseMap = new HashMap<>();

    courseResponseMap.put(JsonKey.ID, BATCH_ID);
    courseResponseMap.put(JsonKey.VER, "v1");
    courseResponseMap.put(JsonKey.NAME, BATCH_NAME);
    courseResponseMap.put(JsonKey.COUNTER_INCREMENT_STATUS, Boolean.FALSE);
    courseResponseMap.put(JsonKey.ENROLMENTTYPE, JsonKey.INVITE_ONLY);
    courseResponseMap.put(JsonKey.COURSE_ID, "someCourseId");
    courseResponseMap.put(JsonKey.COURSE_CREATED_FOR, new ArrayList<Object>());
    courseResponseMap.put(JsonKey.STATUS, batchProgressStatus);

    if (batchProgressStatus == ProjectUtil.ProgressStatus.STARTED.getValue()) {

      existingStartDate = calculateDate(-4);
      existingEndDate = calculateDate(2);
    } else if (batchProgressStatus == ProjectUtil.ProgressStatus.NOT_STARTED.getValue()) {

      existingStartDate = calculateDate(2);
      existingEndDate = calculateDate(4);
    } else {

      existingStartDate = calculateDate(-4);
      existingEndDate = calculateDate(-2);
    }

    courseResponseMap.put(JsonKey.START_DATE, existingStartDate);
    courseResponseMap.put(JsonKey.END_DATE, existingEndDate);

    list.add(courseResponseMap);
    response.put(JsonKey.RESPONSE, list);

    return response;
  }

  private String getOffsetDate(String date, int offSet) {

    try {
      Calendar calender = Calendar.getInstance();
      calender.setTime(format.parse(date));
      calender.add(Calendar.DATE, offSet);
      return format.format(calender.getTime());
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test
  public void testUpdateStartedCourseBatchFailureWithStartDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(
            getOffsetDate(existingStartDate, 1), null, mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateStartedCourseBatchFailureWithEndDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(
            null, getOffsetDate(existingEndDate, 4), mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.courseBatchStartDateRequired.getErrorCode()));
  }

  @Test
  public void testUpdateStartedCourseBatchFailureWithDifferentStartDateAndEndDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(
            getOffsetDate(existingStartDate, 2),
            getOffsetDate(existingEndDate, 4),
            mockGetRecordByIdResponse);
    Assert.assertTrue(
        (exception).getCode().equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateStartedCourseBatchSuccessWithSameStartDateAndEndDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    Response mockUpdateRecordResponse = getMockCassandraResult();
    Response response =
        performUpdateCourseBatchSuccessTest(
            existingStartDate,
            existingEndDate,
            mockGetRecordByIdResponse,
            mockUpdateRecordResponse);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateNotStartedCourseBatchSuccessWithFutureStartDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    Response mockUpdateRecordResponse = getMockCassandraResult();
    Response response =
        performUpdateCourseBatchSuccessTest(
            getOffsetDate(existingStartDate, 2),
            null,
            mockGetRecordByIdResponse,
            mockUpdateRecordResponse);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateNotStartedCourseBatchFailureWithFutureEndDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(null, calculateDate(4), mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.courseBatchStartDateRequired.getErrorCode()));
  }

  public void testUpdateNotStartedCourseBatchSuccessWithFutureStartDateAndEndDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    Response mockUpdateRecordResponse = getMockCassandraResult();
    Response response =
        performUpdateCourseBatchSuccessTest(
            getOffsetDate(existingStartDate, 2),
            getOffsetDate(existingEndDate, 4),
            mockGetRecordByIdResponse,
            mockUpdateRecordResponse);
    Assert.assertTrue(null != response && response.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testUpdateNotStartedCourseBatchFailureWithPastStartDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);

    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(calculateDate(-4), null, mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateNotStartedCourseBatchFailureWithPastEndDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(null, calculateDate(-2), mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.courseBatchStartDateRequired.getErrorCode()));
  }

  @Test
  public void testUpdateNotStartedCourseBatchFailureWithEndDateBeforeFutureStartDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(
            getOffsetDate(existingStartDate, 6),
            getOffsetDate(existingEndDate, 2),
            mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateCompletedCourseBatchFailureWithStartDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.COMPLETED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(
            getOffsetDate(existingStartDate, 2), null, mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }

  @Test
  public void testUpdateCompletedCourseBatchFailureWithEndDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.COMPLETED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(
            null, getOffsetDate(existingEndDate, 4), mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.courseBatchStartDateRequired.getErrorCode()));
  }

  @Test
  public void testUpdateCompletedCourseBatchFailureWithStartDateAndEndDate() {

    int batchProgressStatus = ProjectUtil.ProgressStatus.COMPLETED.getValue();
    Response mockGetRecordByIdResponse = getMockCassandraRecordByIdResponse(batchProgressStatus);
    ProjectCommonException exception =
        performUpdateCourseBatchFailureTest(
            getOffsetDate(existingStartDate, 2),
            getOffsetDate(existingEndDate, 4),
            mockGetRecordByIdResponse);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.invalidBatchStartDateError.getErrorCode()));
  }



  @Test
  public void testCreateCourseBatchInviteOnlySuccess() {
    mockCreateRequest(true);
    actorMessage.setOperation(ActorOperations.CREATE_BATCH.getValue());
    actorMessage.getRequest().putAll(getCourseBatchMap(false,false));
    actorMessage.getContext().put(JsonKey.HEADER,new HashMap<>());
    subject.tell(actorMessage, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(response != null);
  }

  @Test
  public void testCreateCourseBatchInviteOnlySuccessWithMentors() {
    mockCreateRequest(true);
    actorMessage.setOperation(ActorOperations.CREATE_BATCH.getValue());
    actorMessage.getRequest().putAll(getCourseBatchMap(true,false));
    actorMessage.getContext().put(JsonKey.HEADER,new HashMap<>());
    subject.tell(actorMessage, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(response != null);
  }
  @Test
  public void testCreateCourseBatchInviteOnlyFailureWithMentorseBelongToDifferentORG() {
    mockCreateRequest(false);
    actorMessage.setOperation(ActorOperations.CREATE_BATCH.getValue());
    actorMessage.getRequest().putAll(getCourseBatchMap(true,false));
    actorMessage.getContext().put(JsonKey.HEADER,new HashMap<>());
    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exception = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(exception != null);
  }

  @Test
  public void testCreateCourseBatchInviteOnlyFailureWithParticipantsBelongToDifferentOrg() {
    mockCreateRequest(false);
    actorMessage.setOperation(ActorOperations.CREATE_BATCH.getValue());
    actorMessage.getRequest().putAll(getCourseBatchMap(true,true));
    actorMessage.getContext().put(JsonKey.HEADER,new HashMap<>());
    subject.tell(actorMessage, probe.getRef());
    ProjectCommonException exception = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
    Assert.assertTrue(exception != null);
  }

  @Test
  public void testCreateCourseBatchInviteOnlySuccessWithValidMentorsAndParticipants() {
    mockCreateRequest(true);
    when(ElasticSearchUtil.complexSearch(
            Mockito.any(), Mockito.anyString(), Mockito.anyString())).thenReturn(getElasticSearchResponseForMultipleUser());
    actorMessage.setOperation(ActorOperations.CREATE_BATCH.getValue());
    actorMessage.getRequest().putAll(getCourseBatchMap(true,true));
    actorMessage.getContext().put(JsonKey.HEADER,new HashMap<>());
    actorMessage.getContext().put(JsonKey.REQUESTED_BY, "someId");
    subject.tell(actorMessage, probe.getRef());
    Response response = probe.expectMsgClass(duration("100 second"), Response.class);
    Assert.assertTrue(response != null);
  }

  private void  mockCreateRequest(boolean sameOrg){
    PowerMockito.when(mockCassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
            .thenReturn(new Response());
    when(EkStepRequestUtil.searchContent(Mockito.anyString(), Mockito.anyMap()))
            .thenReturn(getEkStepMockResponse());
    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyString())).thenReturn(getElasticSearchResponse(true)).thenReturn(getElasticSearchResponse(true)).thenReturn(getElasticSearchResponse(sameOrg));
  }

  public Map<String, Object> getCourseBatchMap(boolean addMentors, boolean addParticipants ) {
    Map<String, Object> courseBatchMap = new HashMap();
    courseBatchMap.put(JsonKey.COURSE_ID, COURSE_ID);
    courseBatchMap.put(JsonKey.NAME, SOME_NAME);
    courseBatchMap.put(JsonKey.DESCRIPTION, DESC);
    courseBatchMap.put(JsonKey.ENROLLMENT_TYPE, INVITE_ONLY);
    courseBatchMap.put(JsonKey.COURSE_CREATED_FOR, Arrays.asList(ROOT_ORG));
    courseBatchMap.put(JsonKey.START_DATE, START_DATE);

    if(addMentors){
      courseBatchMap.put(JsonKey.MENTORS,Arrays.asList(MENTORS));
    }
    if(addParticipants){
      courseBatchMap.put(JsonKey.PARTICIPANTS,Arrays.asList(USER_ID));

    }
    return courseBatchMap;
  }
  private Map getEkStepMockResponse() {
    Map<String, Object> ekstepResponse = new HashMap<String, Object>();
    ekstepResponse.put(JsonKey.COUNT, VALUE);

    List<Object> arr = Arrays.asList(ekstepResponse);
    Map<String, Object> ekstepMockResult = new HashMap<>();
    ekstepMockResult.put(JsonKey.CONTENTS, arr);
    return ekstepMockResult;
  }

  private Map getElasticSearchResponse(boolean sameOrg) {
    Map<String, Object> elasticSearchResponse = new HashMap<String, Object>();
    elasticSearchResponse.put(JsonKey
            .RESPONSE,Arrays.asList(""));
    if(sameOrg)
      elasticSearchResponse.put(JsonKey.ROOT_ORG_ID, ROOT_ORG);
    else
      elasticSearchResponse.put(JsonKey.ROOT_ORG_ID, OTHER_ORG);
    return elasticSearchResponse;
  }

  private Map getElasticSearchResponseForMultipleUser() {
    Map<String, Object> elasticSearchResponse = new HashMap<String, Object>();
    elasticSearchResponse.put(JsonKey
            .RESPONSE,Arrays.asList(""));
    elasticSearchResponse.put(JsonKey.ROOT_ORG_ID,ROOT_ORG);
    elasticSearchResponse.put(JsonKey.ID, USER_ID);
    Map<String,Object> resultResponse = new HashedMap();
    resultResponse.put(JsonKey.CONTENT,Arrays.asList(elasticSearchResponse));
    return resultResponse;
  }
}
