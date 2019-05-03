package org.sunbird.learner.actors;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.*;
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
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ContentSearchUtil;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  Util.class,
  DataCacheHandler.class,
  PageManagementActor.class,
  ContentSearchUtil.class
})
@PowerMockIgnore({"javax.management.*"})
@Ignore
public class LearnerStateUpdateActorTest {

  private static ActorSystem system = ActorSystem.create("system");;
  private static final Props props = Props.create(LearnerStateUpdateActor.class);

  private static String userId = "user121gama";
  private static String courseId = "alpha01crs";
  private static final String contentId = "cont3544TeBukGame";
  private static Util.DbInfo contentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB);
  private static Util.DbInfo coursedbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
  private static final String batchId = "220j2536h37841hc3u";
  private static Util.DbInfo batchdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);

  private static CassandraOperationImpl cassandraOperation;

  @BeforeClass
  public static void setUp() {

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);

    //
    //
    ////    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    //    insertCourse();
    //    insertBatch();
  }

  @Before
  public void beforeTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());

    when(cassandraOperation.getRecordsByProperty(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCassandraRecordById());
  }

  private Response getCassandraRecordById() {

    Response response = new Response();
    return response;
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  /* private static void insertBatch() {

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Map<String, Object> batchMap = new HashMap<String, Object>();
    batchMap.put(JsonKey.ID, batchId);
    batchMap.put(JsonKey.COURSE_ID, courseId);
    batchMap.put(JsonKey.CREATED_DATE, (String) format.format(new Date()));
    batchMap.put(JsonKey.START_DATE, (String) format.format(new Date()));
    Calendar now = Calendar.getInstance();
    now.add(Calendar.DAY_OF_MONTH, 5);
    Date after5Days = now.getTime();
    batchMap.put(JsonKey.END_DATE, (String) format.format(after5Days));

    cassandraOperation.insertRecord(
        batchdbInfo.getKeySpace(), batchdbInfo.getTableName(), batchMap);
  }*/

  /*private static void insertCourse() {
    Map<String, Object> courseMap = new HashMap<String, Object>();
    courseMap.put(
        JsonKey.ID,
        OneWayHashing.encryptVal(
            userId
                + JsonKey.PRIMARY_KEY_DELIMETER
                + courseId
                + JsonKey.PRIMARY_KEY_DELIMETER
                + batchId));

    courseMap.put(JsonKey.COURSE_ID, courseId);
    courseMap.put(JsonKey.USER_ID, userId);
    courseMap.put(JsonKey.CONTENT_ID, courseId);
    courseMap.put(JsonKey.BATCH_ID, batchId);
    cassandraOperation.insertRecord(
        coursedbInfo.getKeySpace(), coursedbInfo.getTableName(), courseMap);
  }*/

  @Test
  public void checkTelemetryKeyFailure() throws Exception {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    String telemetryEnvKey = "user";
    PowerMockito.mockStatic(Util.class);
    PowerMockito.doNothing()
        .when(
            Util.class,
            "initializeContext",
            Mockito.any(Request.class),
            Mockito.eq(telemetryEnvKey));

    Request req = new Request();
    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
    Map<String, Object> content1 = createContent();
    content1.put(JsonKey.STATUS, new BigInteger("2"));
    contentList.add(content1);

    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CONTENTS, contentList);
    innerMap.put(JsonKey.USER_ID, userId);
    req.setOperation(ActorOperations.ADD_CONTENT.getValue());
    req.setRequest(innerMap);
    subject.tell(req, probe.getRef());
    // probe.expectMsgClass(Response.class);
    Thread.sleep(3000);
    Response dbbRes =
        cassandraOperation.getRecordsByProperty(
            contentdbInfo.getKeySpace(),
            contentdbInfo.getTableName(),
            JsonKey.CONTENT_ID,
            contentId);
    List list = (List) dbbRes.getResult().get(JsonKey.RESPONSE);
    Assert.assertTrue(!(telemetryEnvKey.charAt(0) >= 65 && telemetryEnvKey.charAt(0) <= 90));
  }

  @Test
  public void addContentTest() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request req = new Request();
    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
    Map<String, Object> content1 = createContent();
    content1.put(JsonKey.STATUS, new BigInteger("2"));
    contentList.add(content1);

    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CONTENTS, contentList);
    innerMap.put(JsonKey.USER_ID, userId);
    req.setOperation(ActorOperations.ADD_CONTENT.getValue());
    req.setRequest(innerMap);
    subject.tell(req, probe.getRef());
    // probe.expectMsgClass(Response.class);
    Thread.sleep(3000);
    Response dbbRes =
        cassandraOperation.getRecordsByProperty(
            contentdbInfo.getKeySpace(),
            contentdbInfo.getTableName(),
            JsonKey.CONTENT_ID,
            contentId);
    List list = (List) dbbRes.getResult().get(JsonKey.RESPONSE);
    Assert.assertEquals(1, list.size());
  }

  @Test
  public void updateContentTest() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request req = new Request();
    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
    Map<String, Object> content1 = createContent();
    content1.put(JsonKey.STATUS, new BigInteger("1"));

    contentList.add(content1);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CONTENTS, contentList);
    innerMap.put(JsonKey.USER_ID, userId);
    req.setOperation(ActorOperations.ADD_CONTENT.getValue());
    req.setRequest(innerMap);

    subject.tell(req, probe.getRef());
    probe.expectMsgClass(Response.class);
    Thread.sleep(3000);
    Response dbbRes =
        cassandraOperation.getRecordsByProperty(
            contentdbInfo.getKeySpace(),
            contentdbInfo.getTableName(),
            JsonKey.CONTENT_ID,
            contentId);
    List list = (List) dbbRes.getResult().get(JsonKey.RESPONSE);
    Assert.assertEquals(1, list.size());
  }

  @Test
  public void updateContentTest001() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request req = new Request();
    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
    Map<String, Object> content1 = createContent();
    content1.put(JsonKey.STATUS, new BigInteger("2"));

    contentList.add(content1);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CONTENTS, contentList);
    innerMap.put(JsonKey.USER_ID, userId);
    req.setOperation(ActorOperations.ADD_CONTENT.getValue());
    req.setRequest(innerMap);

    subject.tell(req, probe.getRef());
    probe.expectMsgClass(Response.class);
    Thread.sleep(3000);
    Response dbbRes =
        cassandraOperation.getRecordsByProperty(
            contentdbInfo.getKeySpace(),
            contentdbInfo.getTableName(),
            JsonKey.CONTENT_ID,
            contentId);
    List list = (List) dbbRes.getResult().get(JsonKey.RESPONSE);
    Assert.assertEquals(1, list.size());
  }

  @Test
  public void updateContentTestWithInvalidDateFormat() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request req = new Request();
    List<Map<String, Object>> contentList = new ArrayList<Map<String, Object>>();
    Map<String, Object> content1 = createContent();
    content1.put(JsonKey.STATUS, new BigInteger("2"));
    content1.put(JsonKey.LAST_ACCESS_TIME, "2017-87-20 21:03:29:599+0530");

    contentList.add(content1);
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CONTENTS, contentList);
    innerMap.put(JsonKey.USER_ID, userId);
    req.setOperation(ActorOperations.ADD_CONTENT.getValue());
    req.setRequest(innerMap);

    subject.tell(req, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void onReceiveUpdateContentWithInvalidOperationTest() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request req = new Request();
    req.setOperation("INVALID_OPERATION");
    subject.tell(req, probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  @Test
  public void onReceiveUpdateContentWithInvalidRequest() throws Throwable {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell("INVALID REQUEST", probe.getRef());
    ProjectCommonException exc = probe.expectMsgClass(ProjectCommonException.class);
    Assert.assertTrue(null != exc);
  }

  private Map<String, Object> createContent() {

    Map<String, Object> content = new HashMap<String, Object>();
    /*
     * String key = userId + JsonKey.PRIMARY_KEY_DELIMETER + contentId +
     * JsonKey.PRIMARY_KEY_DELIMETER +
     * courseId+JsonKey.PRIMARY_KEY_DELIMETER+batchId;
     */
    content.put(JsonKey.LAST_ACCESS_TIME, ProjectUtil.getFormattedDate());
    content.put(JsonKey.COMPLETED_COUNT, "0");
    content.put(JsonKey.STATUS, "1");
    content.put(JsonKey.LAST_UPDATED_TIME, ProjectUtil.getFormattedDate());
    content.put(JsonKey.LAST_COMPLETED_TIME, ProjectUtil.getFormattedDate());
    // String id = OneWayHashing.encryptVal(key);
    // content.put(JsonKey.ID , id);
    content.put(JsonKey.COURSE_ID, courseId);
    content.put(JsonKey.USER_ID, userId);
    content.put(JsonKey.CONTENT_ID, contentId);
    content.put(JsonKey.BATCH_ID, batchId);
    content.put(JsonKey.PROGRESS, new BigInteger("100"));
    return content;
  }

  @AfterClass
  public static void destroy() {

    cassandraOperation.deleteRecord(
        coursedbInfo.getKeySpace(),
        coursedbInfo.getTableName(),
        OneWayHashing.encryptVal(
            userId
                + JsonKey.PRIMARY_KEY_DELIMETER
                + courseId
                + JsonKey.PRIMARY_KEY_DELIMETER
                + batchId));
    String key =
        userId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + contentId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + batchId;
    String contentid = OneWayHashing.encryptVal(key);
    cassandraOperation.deleteRecord(
        contentdbInfo.getKeySpace(), contentdbInfo.getTableName(), contentid);
    cassandraOperation.deleteRecord(batchdbInfo.getKeySpace(), batchdbInfo.getTableName(), batchId);
  }
}
