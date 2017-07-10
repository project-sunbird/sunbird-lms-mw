package org.sunbird.learner.actors;

import java.util.HashMap;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import static akka.testkit.JavaTestKit.duration;

/**
 * @author arvind
 */
public class CourseEnrollmentActorTest {


    static ActorSystem system;
    final static  Props props = Props.create(CourseEnrollmentActor.class);
    static Util.DbInfo courseEnrollmentdbInfo = null;

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    }


    @Test()
    public void onReceiveTest() {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1");
        reqObj.setOperation(ActorOperations.ENROLL_COURSE.getValue());
        reqObj.put(JsonKey.COURSE_ID, "do_212282810555342848180");
        reqObj.put(JsonKey.USER_ID, "USR");
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.COURSE, reqObj.getRequest());
        innerMap.put(JsonKey.USER_ID, "USR");
        innerMap.put(JsonKey.ID, "");
        reqObj.setRequest(innerMap);
        try {
            subject.tell(reqObj, probe.getRef());
            probe.expectMsgClass(duration("10 second"),Response.class);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }finally{
        	CassandraOperation op = new CassandraOperationImpl();
        	op.deleteRecord(courseEnrollmentdbInfo.getKeySpace(), courseEnrollmentdbInfo.getTableName(), OneWayHashing.encryptVal("USR"+ JsonKey.PRIMARY_KEY_DELIMETER+"do_212282810555342848180"));
        }
    }

    @Test()
    public void onReceiveTestWithInvalidOperation() throws Throwable {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1211");
        reqObj.setOperation("INVALID_OPERATION");
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.COURSE, reqObj.getRequest());
        innerMap.put(JsonKey.USER_ID, "USR1");
        innerMap.put(JsonKey.ID, "");
        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }
}
