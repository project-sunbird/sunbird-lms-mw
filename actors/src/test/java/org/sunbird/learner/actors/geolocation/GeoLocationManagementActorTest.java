package org.sunbird.learner.actors.geolocation;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.LearnerStateActor;
import org.sunbird.learner.util.Util;

/**
 * Created by arvind on 3/11/17.
 */
public class GeoLocationManagementActorTest {


  static ActorSystem system;
  final static Props props = Props.create(GeoLocationManagementActor.class);
  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
  private static Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
  private static final String orgId = "hhjcjr79fw4p89";
  private static final String location = "jiuewcicri";
  private static final String type = "husvej";
  private static final String userId = "vcurc633r8911";


  @BeforeClass
  public static void setUp(){

    Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
    system = ActorSystem.create("system");

    Map<String , Object> orgMap = new HashMap<String , Object>();
    orgMap.put(JsonKey.ID , orgId);
    cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgMap);

  }

  @Test
  public void createGeoLocationTest(){

    List<Map<String , Object>> dataList = new ArrayList<>();

    Map<String , Object> dataMap = new HashMap<>();
    dataMap.put(JsonKey.LOCATION , "location");
    dataMap.put(JsonKey.TYPE , type);

    dataList.add(dataMap);

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Request actorMessage = new Request();

    actorMessage.getRequest().put(JsonKey.REQUESTED_BY , userId);
    actorMessage.setOperation(ActorOperations.CREATE_GEO_LOCATION.getValue());

    actorMessage.getRequest().put(JsonKey.DATA , dataList);
    actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID , orgId);

    subject.tell(actorMessage, probe.getRef());
    Response res= probe.expectMsgClass(duration("100 second"),Response.class);

  }


  @AfterClass
  public static void destroy(){

    cassandraOperation.deleteRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgId);

  }



}
