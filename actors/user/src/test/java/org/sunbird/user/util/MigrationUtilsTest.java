package org.sunbird.user.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.bean.ClaimStatus;
import org.sunbird.bean.ShadowUser;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.user.service.impl.UserServiceImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        UserServiceImpl.class,
        ServiceFactory.class,
        CassandraOperationImpl.class

})
@PowerMockIgnore({"javax.management.*"})
public class MigrationUtilsTest {

    private static Response response;
    public static CassandraOperationImpl cassandraOperationImpl;
    @Before
    public void beforeEachTest() {
        response=new Response();
        PowerMockito.mockStatic(ServiceFactory.class);
        cassandraOperationImpl = mock(CassandraOperationImpl.class);
        when(ServiceFactory.getInstance()).thenReturn(cassandraOperationImpl);
        when(cassandraOperationImpl.searchValueInList(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, JsonKey.USER_IDs, "DEF")).thenReturn(getRecordsById(false),getRecordsById(true),getRecordsById(false));
        when(cassandraOperationImpl.searchValueInList(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, JsonKey.USER_IDs, "ABC",new HashMap<>())).thenReturn(getRecordsById(false),getRecordsById(true));
        when(cassandraOperationImpl.searchValueInList(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, JsonKey.USER_IDs, "EFG",new HashMap<>())).thenReturn(getRecordsByIdFailure());
    }

    @Test
    public void testGetRecordByUserIdSuccess() {
        ShadowUser shadowUser=MigrationUtils.getRecordByUserId("DEF");
        Assert.assertEquals("TN",shadowUser.getChannel());
    }

    @Test
    public void testGetRecordByUserIdFailure() {
        ShadowUser shadowUser=MigrationUtils.getRecordByUserId("DEF");
        Assert.assertEquals(null,shadowUser);
    }

    @Test
    public void testUpdateRecord(){
        Map<String, Object> compositeKeysMap = new HashMap<>();
        compositeKeysMap.put(JsonKey.USER_EXT_ID, "anyUserExtId");
        compositeKeysMap.put(JsonKey.CHANNEL, "anyChannel");
        when(cassandraOperationImpl.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, new HashMap<>(), compositeKeysMap)).thenReturn(response);
        boolean isRecordUpdated=MigrationUtils.updateRecord(new HashMap<>(),"anyChannel","anyUserExtId");
        Assert.assertEquals(true,isRecordUpdated);
    }

    @Test
    public void testmarkUserAsRejected(){
        ShadowUser shadowUser=new ShadowUser.ShadowUserBuilder()
                .setChannel("anyChannel")
                .setUserExtId("anyUserExtId")
                .build();
        Map<String, Object> compositeKeysMap = new HashMap<>();
        compositeKeysMap.put(JsonKey.USER_EXT_ID, "anyUserExtId");
        compositeKeysMap.put(JsonKey.CHANNEL, "anyChannel");
        when(cassandraOperationImpl.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, new HashMap<>(), compositeKeysMap)).thenReturn(response);
        boolean isRecordUpdated=MigrationUtils.markUserAsRejected(shadowUser);
        Assert.assertEquals(true,isRecordUpdated);


    }

    @Test
    public void testUpdateClaimStatus(){
        ShadowUser shadowUser=new ShadowUser.ShadowUserBuilder()
                .setChannel("anyChannel")
                .setUserExtId("anyUserExtId")
                .build();
        Map<String, Object> compositeKeysMap = new HashMap<>();
        compositeKeysMap.put(JsonKey.USER_EXT_ID, "anyUserExtId");
        compositeKeysMap.put(JsonKey.CHANNEL, "anyChannel");
        when(cassandraOperationImpl.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, new HashMap<>(), compositeKeysMap)).thenReturn(response);
        boolean isRecordUpdated=MigrationUtils.updateClaimStatus(shadowUser,ClaimStatus.ELIGIBLE.getValue());
        Assert.assertEquals(true,isRecordUpdated);
    }

    @Test
    public void testGetEligibleUserByIdsSuccess(){
       List<ShadowUser>shadowUserList= MigrationUtils.getEligibleUsersById("ABC",new HashMap<>());
       Assert.assertEquals(1,shadowUserList.size());

    }
    @Test
    public void testGetEligibleUserByIdsFailure(){
        List<ShadowUser>shadowUserList= MigrationUtils.getEligibleUsersById("ABC",new HashMap<>());
        Assert.assertEquals(0,shadowUserList.size());
    }
    @Test
    public void testGetEligibleUserByIds(){
        List<ShadowUser>shadowUserList= MigrationUtils.getEligibleUsersById("EFG",new HashMap<>());
        Assert.assertEquals(0,shadowUserList.size());
    }
    @Test
    public void testGetEligibleUserByIdsWithoutProps(){
        List<ShadowUser>shadowUserList= MigrationUtils.getEligibleUsersById("DEF");
        Assert.assertEquals(1,shadowUserList.size());
    }


    private Response getRecordsById(boolean empty) {
        Response res = new Response();
        List<Map<String, Object>> list = new ArrayList<>();
        if (!empty) {
            Map<String, Object> map = new HashMap<>();
            map.put(JsonKey.CHANNEL, "TN");
            map.put(JsonKey.USER_ID, "ABC");
            map.put(JsonKey.CLAIM_STATUS, ClaimStatus.ELIGIBLE.getValue());
            list.add(map);
        }
        res.put(JsonKey.RESPONSE, list);
        return res;
    }

    private Response getRecordsByIdFailure() {
        Response res = new Response();
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put(JsonKey.CHANNEL, "TN");
        map.put(JsonKey.USER_ID, "EFG");
        map.put(JsonKey.CLAIM_STATUS, ClaimStatus.MULTIMATCH.getValue());
        list.add(map);
        res.put(JsonKey.RESPONSE, list);
        return res;
    }
}