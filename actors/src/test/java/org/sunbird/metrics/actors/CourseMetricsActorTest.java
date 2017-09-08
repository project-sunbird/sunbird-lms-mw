package org.sunbird.metrics.actors;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.LearnerStateActor;
import org.sunbird.learner.util.Util;

/**
 * Created by arvind on 8/9/17.
 */
public class CourseMetricsActorTest {

  static ActorSystem system;
  final static Props props = Props.create(CourseMetricsActor.class);
  static TestActorRef<CourseMetricsActor> ref;
  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static Util.DbInfo batchDbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
  private static Util.DbInfo reportTrackingdbInfo = Util.dbInfoMap.get(JsonKey.REPORT_TRACKING_DB);
  static String userId = "dnk298voopir80249";
  static String courseId = "mclr309f39";
  static String batchId ="jkwf6t3r083fp4h";
  static final String orgId = "vdckcyigc68569";
  private static final String contentId = "nnci3u9f97mxcfu";

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create("system");
    ref = TestActorRef.create(system, props, "testActor");
    insertBatchDataToES();
    insertOrgDataToES();
    insertUserCoursesDataToES();
    insertUserDataToES();
  }

  private static void insertUserDataToES(){
    Map<String , Object> userMap = new HashMap<>();
    userMap.put(JsonKey.USER_ID , userId);
    userMap.put(JsonKey.FIRST_NAME , "alpha");
    userMap.put(JsonKey.ID , userId);
    userMap.put(JsonKey.USERNAME , "alpha-beta");
    userMap.put(JsonKey.REGISTERED_ORG_ID, orgId);
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(),EsType.user.getTypeName() , userId , userMap);
  }

  private static void insertBatchDataToES(){
    Map<String , Object> batchMap = new HashMap<>();
    batchMap.put(JsonKey.ID , batchId);
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(),EsType.course.getTypeName() , batchId , batchMap);
  }

  private static void insertUserCoursesDataToES(){
    Map<String , Object> userCoursesMap = new HashMap<>();
    userCoursesMap.put(JsonKey.ID , batchId+JsonKey.PRIMARY_KEY_DELIMETER+userId);
    userCoursesMap.put(JsonKey.BATCH_ID , batchId);
    userCoursesMap.put(JsonKey.USER_ID, userId);
    userCoursesMap.put(JsonKey.REGISTERED_ORG_ID , orgId);
    userCoursesMap.put(JsonKey.PROGRESS , 1);
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(),EsType.usercourses.getTypeName() , batchId+JsonKey.PRIMARY_KEY_DELIMETER+userId , userCoursesMap);
  }

  private static void insertOrgDataToES(){
    Map<String , Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID , orgId);
    orgMap.put(JsonKey.ORGANISATION_NAME , "IIM");
    orgMap.put(JsonKey.ORGANISATION_ID , orgId);
    ElasticSearchUtil.createData(EsIndex.sunbird.getIndexName(),EsType.organisation.getTypeName() , batchId , orgMap);
  }

  @Test
  public void testCourseProgress(){

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request actorMessage = new Request();
    actorMessage.put(JsonKey.REQUESTED_BY , userId);
    actorMessage.put(JsonKey.BATCH_ID , batchId);
    actorMessage.put(JsonKey.PERIOD , "fromBegining");
    actorMessage.setOperation(ActorOperations.COURSE_PROGRESS_METRICS.getValue());

    subject.tell(actorMessage, probe.getRef());
    Response res= probe.expectMsgClass(duration("100 second"),Response.class);
    System.out.println("SUCCESS");
    System.out.println("SUCCESS");

  }

}
