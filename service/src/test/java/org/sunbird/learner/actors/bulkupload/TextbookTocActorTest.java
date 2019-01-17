package org.sunbird.learner.actors.bulkupload;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.util.TextBookTocUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  TextBookTocUtil.class,
  ProjectUtil.class,
})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
public class TextbookTocActorTest {

  private static ActorSystem system;
  private static final Props props =
      Props.create(org.sunbird.learner.actors.bulkupload.TextbookTocActor.class);
  private static ObjectMapper mapper = new ObjectMapper();

  private static final String TB_WITHOUT_CHILDREN =
      "{\"id\":\"ekstep.content.find\",\"ver\":\"3.0\",\"ts\":\"2018-12-12T06:21:17ZZ\",\"params\":{\"resmsgid\":\"bf8273ca-be0f-4062-8986-fbfff07002ac\",\"msgid\":null,\"err\":null,\"status\":\"successful\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"content\":{\"ownershipType\":[\"createdBy\"],\"code\":\"Science\",\"channel\":\"in.ekstep\",\"description\":\"Test TextBook\",\"language\":[\"English\"],\"mimeType\":\"application/vnd.ekstep.content-collection\",\"idealScreenSize\":\"normal\",\"createdOn\":\"2018-12-12T06:20:57.814+0000\",\"appId\":\"ekstep_portal\",\"contentDisposition\":\"inline\",\"contentEncoding\":\"gzip\",\"lastUpdatedOn\":\"2018-12-12T06:20:57.814+0000\",\"contentType\":\"TextBook\",\"dialcodeRequired\":\"No\",\"identifier\":\"do_11265332762881228812868\",\"audience\":[\"Learner\"],\"visibility\":\"Default\",\"os\":[\"All\"],\"consumerId\":\"a6654129-b58d-4dd8-9cf2-f8f3c2f458bc\",\"mediaType\":\"content\",\"osId\":\"org.ekstep.quiz.app\",\"languageCode\":\"en\",\"versionKey\":\"1544595657814\",\"idealScreenDensity\":\"hdpi\",\"framework\":\"NCF\",\"compatibilityLevel\":1,\"name\":\"Science-10\",\"status\":\"Draft\"}}}";

  private static final String TB_CREATE_VALID_REQ =
      "{\"mode\":\"create\",\"fileData\":[{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\",\"reservedDialcodes\":\"2019\",\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.1.1 Key parts in the head\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.2 .1 Respiratory System\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"Textbook\":\"Science-10\"}}]}";

  private static final String DUPLICATE_ROW_REQ =
      "{\"mode\":\"create\",\"fileData\":[{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.1.1 Key parts in the head\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.2 .1 Respiratory System\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"Textbook\":\"Science-10\"}}]}";

  private static final String REQUIRED_FIELD_MISS_REQ =
      "{\"mode\":\"create\",\"fileData\":[{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:3\":\"5.1.1 Key parts in the head\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.2 .1 Respiratory System\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"Textbook\":\"Science-10\"}}]}";

  private static final String data =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,"
          + "QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords\n"
          + "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,,,";

  private static final String dataDuplicateEntry =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,"
          + "QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords\n"
          + "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,,\n"
          + "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,,";

  private static final String dataDailCodeMore =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,"
          + "QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords\n"
          + "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,,\n"
          + "do_1126788813057638401122,,,,test,unit1,,Yes,2569,,,";

  private static final String dataDailcodeNotreq =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,"
          + "QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords\n"
          + "do_1126788813057638401122,,,,test,unit1,,No,2019,,,";

  private static final String dataFailureTopic =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,"
          + "QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords\n"
          + "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,topi,abc";

  private static final String dataFailureDailcode =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,"
          + "QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords\n"
          + "do_1126788813057638401122,,,,test,unit1,,Yes,2089,,,";

  private static final String dataFailureDailcodeUnique =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,"
          + "QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords\n"
          + "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,,";

  private static final String dataFailure1 =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,"
          + "QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords";

  private static final String TEXTBOOK_TOC_INPUT_MAPPING =
      "{\"identifier\":\"Identifier\",\"frameworkCategories\":{\"medium\":\"Medium\",\"gradeLevel\":\"Grade\",\"subject\":\"Subject\"},\"hierarchy\":{\"Textbook\":\"Textbook Name\",\"L:1\":\"Level 1 Textbook Unit\",\"L:2\":\"Level 2 Textbook Unit\",\"L:3\":\"Level 3 Textbook Unit\",\"L:4\":\"Level 4 Textbook Unit\"},\"metadata\":{\"description\":\"Description\",\"dialcodeRequired\":\"QR Code Required?\",\"dialcodes\":\"QR Code\",\"purpose\":\"Purpose of Content to be linked\",\"topic\":\"Mapped Topics\",\"keywords\":\"Keywords\"}}";
  private static final String MANDATORY_VALUES =
      "{\"Textbook\":\"Textbook Name\",\"L:1\":\"Level 1 Textbook Unit\"}";
  private static final String CONTENT_TYPE = "any";

  @Before
  public void setUp() {
    PowerMockito.mockStatic(TextBookTocUtil.class);
    PowerMockito.mockStatic(ProjectUtil.class);
    system = ActorSystem.create("system");
    when(ProjectUtil.getConfigValue(Mockito.anyString())).thenReturn(TEXTBOOK_TOC_INPUT_MAPPING);
    when(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_MAX_CSV_ROWS)).thenReturn("5");
    when(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_MANDATORY_FIELDS))
        .thenReturn(MANDATORY_VALUES);
    when(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_ALLOWED_CONTNET_TYPES))
        .thenReturn(CONTENT_TYPE);
    when(ProjectUtil.getConfigValue(JsonKey.EKSTEP_BASE_URL)).thenReturn("http://www.abc.com/");
    when(ProjectUtil.getConfigValue(JsonKey.UPDATE_HIERARCHY_API)).thenReturn("");
    HttpResponse<String> updateResponse = null;
    /*
     * when(Unirest.patch("http://www.abc.com/"). headers(Mockito.anyMap()).
     * body(Mockito.anyString()).asString()).thenReturn(updateResponse);
     */
  }

  @Test
  public void testUpdateFailureDailcodecorrectAndIncorrect() throws IOException {
    mock(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, dataDailCodeMore);
    Assert.assertEquals(
        res.getCode(), ResponseCode.errorDialCodeNotReservedForTextBook.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateFailureDailcodeNotreq() throws IOException {
    mock(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, dataDailcodeNotreq);
    Assert.assertEquals(res.getCode(), ResponseCode.errorConflictingValues.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateFailureDuplicateEntry() throws IOException {
    mock(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, dataDuplicateEntry);
    Assert.assertEquals(res.getCode(), ResponseCode.errorDuplicateEntries.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateFailureblankCsv() throws IOException {
    mock(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, dataFailure1);
    Assert.assertEquals(res.getCode(), ResponseCode.blankCsvData.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateInvalideTopicFailure() throws IOException {
    mock(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, dataFailureTopic);
    Assert.assertEquals(res.getCode(), ResponseCode.errorInvalidTopic.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateInvalideDailcodeFailure() throws IOException {
    mock(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, dataFailureDailcode);
    Assert.assertEquals(
        res.getCode(), ResponseCode.errorDialCodeNotReservedForTextBook.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateInvalideDialCodeUniquenessFailure() throws IOException {
    mock(true);
    ProjectCommonException res =
        (ProjectCommonException) doRequest(true, dataFailureDailcodeUnique);
    Assert.assertEquals(res.getCode(), ResponseCode.errorDialCodeAlreadyAssociated.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  @Ignore
  public void testUpdateSuccess() throws IOException {
    mock(false);
    Response res = (Response) doRequest(false, data);
    Assert.assertNotNull(res);
  }

  private Object doRequest(boolean error, String data) throws IOException {
    TestKit probe = new TestKit(system);
    ActorRef toc = system.actorOf(props);
    Request request = new Request();
    request.setContext(
        new HashMap<String, Object>() {
          {
            put("userId", "test");
          }
        });
    request.put(JsonKey.TEXTBOOK_ID, "do_1126788813057638401122");
    InputStream stream = new ByteArrayInputStream(data.getBytes());
    byte[] byteArray = IOUtils.toByteArray(stream);
    request.getRequest().put(JsonKey.DATA, byteArray);
    request.put(JsonKey.DATA, byteArray);
    request.setOperation(TextbookActorOperation.TEXTBOOK_TOC_UPLOAD.getValue());
    toc.tell(request, probe.getRef());
    if (error) {
      ProjectCommonException res =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      return res;
    }
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    return res;
  }

  private void mock(boolean bool) {
    when(TextBookTocUtil.getRelatedFrameworkById(Mockito.anyString()))
        .thenReturn(getFrameworkMap());
    when(TextBookTocUtil.readHierarchy(Mockito.anyString())).thenReturn(getReadHierarchy(bool));
    when(TextBookTocUtil.readContent(Mockito.anyString())).thenReturn(getReadContentTextbookData());
  }

  private static Response getTbWithoutChildren() throws IOException {
    return mapper.readValue(TB_WITHOUT_CHILDREN, Response.class);
  }

  private static Response getSuccess() {
    Response response = new Response();
    response.put("content_id", "do_11265332762881228812868");
    return response;
  }

  private Map<String, String> getDefaultHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put(JsonKey.AUTHORIZATION, JsonKey.BEARER);
    return headers;
  }

  private Response getReadHierarchy(boolean error) {
    // TODO Auto-generated method stub
    Response res = new Response();
    List<String> dialCodes = new ArrayList<>();
    if (error) {
      dialCodes.add("1564");
    } else {
      dialCodes.add("2019");
    }
    res.put(
        JsonKey.CONTENT,
        new HashMap<String, Object>() {
          {
            put(JsonKey.DIAL_CODES, dialCodes);
            put(JsonKey.CHILDREN, new ArrayList<>());
            put(JsonKey.IDENTIFIER, "do_1126788813057638401122");
          }
        });
    return res;
  }

  private Response getReadContentTextbookData() {
    Response res = new Response();
    Map<String, Object> textBookdata = new HashMap<>();
    List<String> s = new ArrayList<>();
    s.add("2019");
    textBookdata.put(JsonKey.RESERVED_DIAL_CODES, s);
    textBookdata.put(JsonKey.CONTENT_TYPE, CONTENT_TYPE);
    textBookdata.put(JsonKey.MIME_TYPE, "application/vnd.ekstep.content-collection");
    textBookdata.put(JsonKey.NAME, "test");
    res.put(JsonKey.CONTENT, textBookdata);
    return res;
  }

  private Response getFrameworkMap() {
    Map<String, Object> map = new HashMap<>();
    Response res = new Response();
    return res;
  }
}
