package org.sunbird.badge.service.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.badge.model.BadgeClassExtension;
import org.sunbird.badge.service.BadgeClassExtensionService;
import org.sunbird.badge.service.BadgingService;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BadgrServiceImpl.class, HttpUtil.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class BadgrServiceImplBadgeClassTest {
    BadgingService badgrServiceImpl;

    private Request request;

    BadgeClassExtensionService mockBadgeClassExtensionService;

    private static final String BADGE_CLASS_COMMON_RESPONSE_SUCCESS = "{\"created_at\":\"2018-03-05T09:35:33.722993Z\",\"id\":1,\"issuer\":\"http://localhost:8000/public/issuers/oracle-university\",\"json\":{\"name\":\"Java SE 8 Programmer\",\"image\":\"http://localhost:8000/public/badges/java-se-8-programmer/image\",\"criteria\":\"https://education.oracle.com/pls/web_prod-plq-dad/db_pages.getpage?page_id=5001&get_params=p_exam_id:1Z0-808\",\"@context\":\"https://w3id.org/openbadges/v1\",\"issuer\":\"http://localhost:8000/public/issuers/oracle-university\",\"type\":\"BadgeClass\",\"id\":\"http://localhost:8000/public/badges/java-se-8-programmer\",\"description\":\"A basic Java SE 8 certification.\"},\"name\":\"Java SE 8 Programmer\",\"image\":\"http://localhost:8000/media/uploads/badges/issuer_badgeclass_76c4cb77-40c7-4694-bee2-de15bd45f6cb.png\",\"slug\":\"java-se-8-programmer\",\"recipient_count\":1,\"created_by\":\"http://localhost:8000/user/1\"}";
    private static final String BADGE_CLASS_CREATE_RESPONSE_FAILURE_ISSUER_NOT_FOUND = "\"Issuer invalid not found or inadequate permissions.\"";
    private static final String BADGE_CLASS_GET_RESPONSE_FAILURE_BADGE_NOT_FOUND = "\"BadgeClass invalid could not be found, or inadequate permissions.\"";

    private static final String VALUE_BADGE_ID = "java-se-8-programmer";
    private static final String VALUE_BADGE_ID_URL = "http://localhost:8000/public/badges/java-se-8-programmer";
    private static final String VALUE_ISSUER_ID = "oracle-university";
    private static final String VALUE_ISSUER_ID_URL = "http://localhost:8000/public/issuers/oracle-university";
    private static final String VALUE_NAME = "Java SE 8 Programmer";
    private static final String VALUE_DESCRIPTION = "A basic Java SE 8 certification.";
    private static final String VALUE_BADGE_CRITERIA = "https://education.oracle.com/pls/web_prod-plq-dad/db_pages.getpage?page_id=5001&get_params=p_exam_id:1Z0-808";
    private static final String VALUE_IMAGE = "http://localhost:8000/media/uploads/badges/issuer_badgeclass_76c4cb77-40c7-4694-bee2-de15bd45f6cb.png";
    private static final String VALUE_ROOT_ORG_ID = "AP";
    private static final String VALUE_TYPE = "user";
    private static final String VALUE_SUBTYPE = "award";
    private static final String VALUE_ROLES_JSON = "[ \"roleId1\" ]";
    private static final ArrayList<String> VALUE_ROLES_LIST = new ArrayList<>(Arrays.asList("roleId1"));
    private static final String VALUE_CREATED_DATE = "2018-03-05T09:35:33.722993Z";

    private static final String INVALID_VALUE = "invalid";

    @Before
    public void setUp() {
        PowerMockito.mockStatic(HttpUtil.class);
        mockBadgeClassExtensionService = PowerMockito.mock(BadgeClassExtensionServiceImpl.class);

        badgrServiceImpl = new BadgrServiceImpl(mockBadgeClassExtensionService);
        request = new Request();
    }

    @Test
    public void testCreateBadgeClassSuccess() throws IOException {
        PowerMockito.when(HttpUtil.postFormData(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(new HttpUtilResponse(BADGE_CLASS_COMMON_RESPONSE_SUCCESS, 200));
        PowerMockito.doNothing().when(mockBadgeClassExtensionService).save(Mockito.any());

        Map<String, String> formParamsMap = new HashMap<>();

        formParamsMap.put(BadgingJsonKey.ISSUER_ID, VALUE_ISSUER_ID);
        formParamsMap.put(BadgingJsonKey.BADGE_CRITERIA, VALUE_BADGE_CRITERIA);
        formParamsMap.put(JsonKey.NAME, VALUE_NAME);
        formParamsMap.put(JsonKey.DESCRIPTION, VALUE_DESCRIPTION);
        formParamsMap.put(JsonKey.ROOT_ORG_ID, VALUE_ROOT_ORG_ID);
        formParamsMap.put(JsonKey.TYPE, VALUE_TYPE);
        formParamsMap.put(JsonKey.SUBTYPE, VALUE_SUBTYPE);
        formParamsMap.put(JsonKey.ROLES, VALUE_ROLES_JSON);

        Map<String, Object> fileParamsMap = new HashMap<>();
        fileParamsMap.put(JsonKey.IMAGE, VALUE_IMAGE);

        request.put(JsonKey.FORM_PARAMS, formParamsMap);
        request.put(JsonKey.FILE_PARAMS, fileParamsMap);

        Response response = badgrServiceImpl.createBadgeClass(request);

        assertEquals(ResponseCode.OK, response.getResponseCode());

        assertEquals(VALUE_BADGE_ID, response.getResult().get(BadgingJsonKey.BADGE_ID));
        assertEquals(VALUE_BADGE_ID_URL, response.getResult().get(BadgingJsonKey.BADGE_ID_URL));
        assertEquals(VALUE_ISSUER_ID, response.getResult().get(BadgingJsonKey.ISSUER_ID));
        assertEquals(VALUE_ISSUER_ID_URL, response.getResult().get(BadgingJsonKey.ISSUER_ID_URL));
        assertEquals(VALUE_NAME, response.getResult().get(JsonKey.NAME));
        assertEquals(VALUE_DESCRIPTION, response.getResult().get(JsonKey.DESCRIPTION));
        assertEquals(VALUE_BADGE_CRITERIA, response.getResult().get(BadgingJsonKey.BADGE_CRITERIA));
        assertEquals(VALUE_IMAGE, response.getResult().get(JsonKey.IMAGE));
        assertEquals(VALUE_ROOT_ORG_ID, response.getResult().get(JsonKey.ROOT_ORG_ID));
        assertEquals(VALUE_TYPE, response.getResult().get(JsonKey.TYPE));
        assertEquals(VALUE_SUBTYPE, response.getResult().get(JsonKey.SUBTYPE));
        assertEquals(VALUE_ROLES_LIST.toString(), response.getResult().get(JsonKey.ROLES).toString());
        assertEquals(VALUE_CREATED_DATE, response.getResult().get(JsonKey.CREATED_DATE));
    }

    @Test
    public void testCreateBadgeClassFailureInvalidIssuer() throws IOException {
        PowerMockito.when(HttpUtil.postFormData(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(new HttpUtilResponse(BADGE_CLASS_CREATE_RESPONSE_FAILURE_ISSUER_NOT_FOUND, ResponseCode.RESOURCE_NOT_FOUND.getResponseCode()));
        PowerMockito.doNothing().when(mockBadgeClassExtensionService).save(Mockito.any());

        Map<String, String> formParamsMap = new HashMap<>();

        formParamsMap.put(BadgingJsonKey.ISSUER_ID, INVALID_VALUE);

        formParamsMap.put(BadgingJsonKey.BADGE_CRITERIA, VALUE_BADGE_CRITERIA);
        formParamsMap.put(JsonKey.NAME, VALUE_NAME);
        formParamsMap.put(JsonKey.DESCRIPTION, VALUE_DESCRIPTION);
        formParamsMap.put(JsonKey.ROOT_ORG_ID, VALUE_ROOT_ORG_ID);
        formParamsMap.put(JsonKey.TYPE, VALUE_TYPE);
        formParamsMap.put(JsonKey.SUBTYPE, VALUE_SUBTYPE);
        formParamsMap.put(JsonKey.ROLES, VALUE_ROLES_JSON);

        Map<String, Object> fileParamsMap = new HashMap<>();
        fileParamsMap.put(JsonKey.IMAGE, VALUE_IMAGE);

        request.put(JsonKey.FORM_PARAMS, formParamsMap);
        request.put(JsonKey.FILE_PARAMS, fileParamsMap);

        boolean thrown = false;

        try {
            badgrServiceImpl.createBadgeClass(request);
        } catch (ProjectCommonException exception) {
            thrown = true;
            assertEquals(ResponseCode.RESOURCE_NOT_FOUND.getResponseCode(), exception.getResponseCode());
        }

        assertEquals(true, thrown);
    }

    @Test
    public void testCreateBadgeClassFailureIOException() throws IOException {
        PowerMockito.when(HttpUtil.postFormData(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenThrow(new IOException());
        PowerMockito.doNothing().when(mockBadgeClassExtensionService).save(Mockito.any());

        Map<String, String> formParamsMap = new HashMap<>();

        formParamsMap.put(BadgingJsonKey.ISSUER_ID, INVALID_VALUE);

        formParamsMap.put(BadgingJsonKey.BADGE_CRITERIA, VALUE_BADGE_CRITERIA);
        formParamsMap.put(JsonKey.NAME, VALUE_NAME);
        formParamsMap.put(JsonKey.DESCRIPTION, VALUE_DESCRIPTION);
        formParamsMap.put(JsonKey.ROOT_ORG_ID, VALUE_ROOT_ORG_ID);
        formParamsMap.put(JsonKey.TYPE, VALUE_TYPE);
        formParamsMap.put(JsonKey.SUBTYPE, VALUE_SUBTYPE);
        formParamsMap.put(JsonKey.ROLES, VALUE_ROLES_JSON);

        Map<String, Object> fileParamsMap = new HashMap<>();
        fileParamsMap.put(JsonKey.IMAGE, VALUE_IMAGE);

        request.put(JsonKey.FORM_PARAMS, formParamsMap);
        request.put(JsonKey.FILE_PARAMS, fileParamsMap);

        boolean thrown = false;

        try {
            badgrServiceImpl.createBadgeClass(request);
        } catch (ProjectCommonException exception) {
            thrown = true;
            assertEquals(ResponseCode.SERVER_ERROR.getResponseCode(), exception.getResponseCode());
        }

        assertEquals(true, thrown);
    }

    @Test
    public void testGetBadgeClassSuccess() throws IOException {
        PowerMockito.when(HttpUtil.doGetRequest(Mockito.any(), Mockito.any()))
                .thenReturn(new HttpUtilResponse(BADGE_CLASS_COMMON_RESPONSE_SUCCESS, 200));
        PowerMockito.when(mockBadgeClassExtensionService.get(VALUE_BADGE_ID)).thenReturn(
                new BadgeClassExtension(VALUE_BADGE_ID, VALUE_ISSUER_ID, VALUE_ROOT_ORG_ID, VALUE_TYPE, VALUE_SUBTYPE, VALUE_ROLES_LIST)
        );

        request.put(BadgingJsonKey.ISSUER_ID, VALUE_ISSUER_ID);
        request.put(BadgingJsonKey.BADGE_ID, VALUE_BADGE_ID);

        Response response = badgrServiceImpl.getBadgeClassDetails(request);

        assertEquals(ResponseCode.OK, response.getResponseCode());

        assertEquals(VALUE_BADGE_ID, response.getResult().get(BadgingJsonKey.BADGE_ID));
        assertEquals(VALUE_BADGE_ID_URL, response.getResult().get(BadgingJsonKey.BADGE_ID_URL));
        assertEquals(VALUE_ISSUER_ID, response.getResult().get(BadgingJsonKey.ISSUER_ID));
        assertEquals(VALUE_ISSUER_ID_URL, response.getResult().get(BadgingJsonKey.ISSUER_ID_URL));
        assertEquals(VALUE_NAME, response.getResult().get(JsonKey.NAME));
        assertEquals(VALUE_DESCRIPTION, response.getResult().get(JsonKey.DESCRIPTION));
        assertEquals(VALUE_BADGE_CRITERIA, response.getResult().get(BadgingJsonKey.BADGE_CRITERIA));
        assertEquals(VALUE_IMAGE, response.getResult().get(JsonKey.IMAGE));
        assertEquals(VALUE_ROOT_ORG_ID, response.getResult().get(JsonKey.ROOT_ORG_ID));
        assertEquals(VALUE_TYPE, response.getResult().get(JsonKey.TYPE));
        assertEquals(VALUE_SUBTYPE, response.getResult().get(JsonKey.SUBTYPE));
        assertEquals(VALUE_ROLES_LIST.toString(), response.getResult().get(JsonKey.ROLES).toString());
        assertEquals(VALUE_CREATED_DATE, response.getResult().get(JsonKey.CREATED_DATE));
    }

    @Test
    public void testGetBadgeClassFailureInvalidBadgeId() throws IOException {
        PowerMockito.when(HttpUtil.doGetRequest(Mockito.any(), Mockito.any()))
                .thenReturn(new HttpUtilResponse(BADGE_CLASS_GET_RESPONSE_FAILURE_BADGE_NOT_FOUND, ResponseCode.RESOURCE_NOT_FOUND.getResponseCode()));
        PowerMockito.when(mockBadgeClassExtensionService.get(VALUE_BADGE_ID)).thenReturn(
                new BadgeClassExtension(VALUE_BADGE_ID, VALUE_ISSUER_ID, VALUE_ROOT_ORG_ID, VALUE_TYPE, VALUE_SUBTYPE, VALUE_ROLES_LIST)
        );

        request.put(BadgingJsonKey.ISSUER_ID, VALUE_ISSUER_ID);
        request.put(BadgingJsonKey.BADGE_ID, INVALID_VALUE);

        boolean thrown = false;

        try {
            badgrServiceImpl.getBadgeClassDetails(request);
        } catch (ProjectCommonException exception) {
            thrown = true;
            assertEquals(ResponseCode.RESOURCE_NOT_FOUND.getResponseCode(), exception.getResponseCode());
        }

        assertEquals(true, thrown);
    }

    @Test
    public void testGetBadgeClassFailureIOException() throws IOException {
        PowerMockito.when(HttpUtil.doGetRequest(Mockito.any(), Mockito.any()))
                .thenThrow(new IOException());
        PowerMockito.when(mockBadgeClassExtensionService.get(VALUE_BADGE_ID)).thenReturn(
                new BadgeClassExtension(VALUE_BADGE_ID, VALUE_ISSUER_ID, VALUE_ROOT_ORG_ID, VALUE_TYPE, VALUE_SUBTYPE, VALUE_ROLES_LIST)
        );

        request.put(BadgingJsonKey.ISSUER_ID, VALUE_ISSUER_ID);
        request.put(BadgingJsonKey.BADGE_ID, VALUE_BADGE_ID);

        boolean thrown = false;

        try {
            badgrServiceImpl.getBadgeClassDetails(request);
        } catch (ProjectCommonException exception) {
            thrown = true;
            assertEquals(ResponseCode.SERVER_ERROR.getResponseCode(), exception.getResponseCode());
        }

        assertEquals(true, thrown);
    }


}
