package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author arvind
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserManagementActorTest {

    static ActorSystem system;
    final static Props props = Props.create(UserManagementActor.class);
    static Util.DbInfo userManagementDB = null;

    private static String userName ;
    private static String email;

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
        userManagementDB = Util.dbInfoMap.get(JsonKey.USER_DB);
        userName = UUID.randomUUID().toString();
        email = UUID.randomUUID().toString()+"@gmail.com";
        String id = OneWayHashing.encryptVal(userName);
    }

    //@Test
    public void createUserTest(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1");
        reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.USERNAME, userName);
        innerMap.put(JsonKey.EMAIL, email);
        innerMap.put(JsonKey.PASSWORD , "password");
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        Response res =  probe.expectMsgClass(Response.class);

    }

    //@Test
    public void createUserTestWithDuplicateEmail(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1");
        reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.USERNAME, userName);
        innerMap.put(JsonKey.EMAIL, email);
        innerMap.put(JsonKey.PASSWORD , "password");
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }

    //@Test
    public void createUserTestWithDuplicateUserName(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setRequest_id("1");
        reqObj.setOperation(ActorOperations.CREATE_USER.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.USERNAME, userName);
        innerMap.put(JsonKey.EMAIL, email);
        innerMap.put(JsonKey.PASSWORD , "password");
        Map<String , Object> request = new HashMap<String , Object>();
        request.put(JsonKey.USER , innerMap);
        reqObj.setRequest(request);

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);

    }


}
