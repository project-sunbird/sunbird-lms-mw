package org.sunbird.user.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.bean.ShadowUser;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.UserManagementActorTestBase;
import org.sunbird.user.util.MigrationUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        MigrationUtils.class,
        SystemSettingClientImpl.class
})
@PowerMockIgnore({"javax.management.*"})
public class TenantMigrationActorTest extends UserManagementActorTestBase{
    Props props = Props.create(TenantMigrationActor.class);
    ActorSystem system = ActorSystem.create("system");

    @Before
    public void beforeEachTest() {
        ActorRef actorRef = mock(ActorRef.class);
        PowerMockito.mockStatic(RequestRouter.class);
        when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);
        PowerMockito.mockStatic(SystemSettingClientImpl.class);
        SystemSettingClientImpl systemSettingClient = mock(SystemSettingClientImpl.class);
        when(SystemSettingClientImpl.getInstance()).thenReturn(systemSettingClient);
        when(systemSettingClient.getSystemSettingByFieldAndKey(
                Mockito.any(ActorRef.class),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyObject()))
                .thenReturn(new HashMap<>());
        PowerMockito.mockStatic(MigrationUtils.class);
    }

    @Test
    public void testUserMigrateRejectWhenUserFound() {
        when(MigrationUtils.getEligibleUsersById(Mockito.anyString(),Mockito.anyMap())).thenReturn(getShadowUserList());
        boolean result = testScenario(getMigrateReq(ActorOperations.MIGRATE_USER,JsonKey.REJECT), null);
        assertTrue(result);
    }

    @Test
    public void testUserMigrateRejectWhenUserNotFound() {
        List<ShadowUser> shadowUserList = new ArrayList<>();
        when(MigrationUtils.getEligibleUsersById(Mockito.anyString(),Mockito.anyMap())).thenReturn(shadowUserList);
        boolean result = testScenario(getMigrateReq(ActorOperations.MIGRATE_USER,JsonKey.REJECT), ResponseCode.invalidUserId);
        assertTrue(result);
    }

    @Test
    public void testUserMigrationAcceptWhenUserNotFound(){
        List<ShadowUser> shadowUserList = new ArrayList<>();
        when(MigrationUtils.getEligibleUsersById(Mockito.anyString(),Mockito.anyMap())).thenReturn(shadowUserList);
        boolean result = testScenario(getMigrateReq(ActorOperations.MIGRATE_USER,JsonKey.ACCEPT), ResponseCode.invalidUserId);
        assertTrue(result);
    }
    @Test
    public void testUserMigrationAcceptWhenUserFound(){
        when(MigrationUtils.getEligibleUsersById(Mockito.anyString(),Mockito.anyMap())).thenReturn(getShadowUserList());
        boolean result = testScenario(getMigrateReq(ActorOperations.MIGRATE_USER,JsonKey.ACCEPT), ResponseCode.invalidUserId);
        assertTrue(result);
    }





    public boolean testScenario(Request reqObj, ResponseCode errorCode) {
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        subject.tell(reqObj, probe.getRef());

        if (errorCode == null) {
            Response res = probe.expectMsgClass(duration("10 second"), Response.class);
            return null != res && res.getResponseCode() == ResponseCode.OK;
        } else {
            ProjectCommonException res =
                    probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
            System.out.println(res.getCode()+":error code:"+errorCode.getErrorCode());
            return res.getCode().equals(errorCode.getErrorCode())
                    || res.getResponseCode() == errorCode.getResponseCode();
        }
    }


    public Request getMigrateReq(ActorOperations actorOperation,String action) {
        Request reqObj = new Request();
        Map reqMap = new HashMap<>();
        reqMap.put(JsonKey.USER_ID, "anyUserId");
        reqMap.put(JsonKey.USER_EXT_ID, "anyUserExtId");
        reqMap.put(JsonKey.CHANNEL, "anyChannel");
        reqMap.put(JsonKey.ACTION, action);
        reqMap.put(JsonKey.FEED_ID, "anyFeedId");
        reqObj.setRequest(reqMap);
        reqObj.setOperation(actorOperation.getValue());
        System.out.println(reqMap);
        return reqObj;
    }

    private List<ShadowUser> getShadowUserList(){
        ShadowUser shadowUser = new ShadowUser.ShadowUserBuilder()
                .setChannel("anyChannel")
                .setUserExtId("anyUserExtId")
                .setUserId("anyUserId")
                .build();
        List<ShadowUser> shadowUserList = new ArrayList<>();
        shadowUserList.add(shadowUser);
        return shadowUserList;

    }


}