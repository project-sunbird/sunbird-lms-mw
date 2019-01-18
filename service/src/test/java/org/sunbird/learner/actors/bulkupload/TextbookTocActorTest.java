package org.sunbird.learner.actors.bulkupload;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.HttpRequestWithBody;
import com.mashape.unirest.request.body.RequestBodyEntity;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
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
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.util.TextBookTocUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TextBookTocUtil.class, ProjectUtil.class, Unirest.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
public class TextbookTocActorTest {

  private static ActorSystem system;
  private static final Props props =
      Props.create(org.sunbird.learner.actors.bulkupload.TextbookTocActor.class);

  private static final String NORMAIL_HEADER =
      "Identifier,Medium,Grade,Subject,Textbook Name,Level 1 Textbook Unit,Description,QR Code Required?,QR Code,Purpose of Content to be linked,Mapped Topics,Keywords\n";
  private static final String NORMAL_DATA = "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,,";
  private static final String EXTRA_DATA = "do_1126788813057638401122,,,,test,unit1,,Yes,2569,,,";
  private static final String DAIL_CODE_NOT_REQ_DATA =
      "do_1126788813057638401122,,,,test,unit1,,No,2019,,,";
  private static final String TOPIC_FAILURE_DATA =
      "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,topi,abc";
  private static final String DIAL_CODE_FAILURE_DATA =
      "do_1126788813057638401122,,,,test,unit1,,Yes,2089,,,";
  private static final String DAIL_CODE_UNIQUE_FAILURE_DATA =
      "do_1126788813057638401122,,,,test,unit1,,Yes,2019,,,";

  private static final String DATA = NORMAIL_HEADER + NORMAL_DATA;

  private static final String DATA_DUPLICATE_ENTRY =
      NORMAIL_HEADER + NORMAL_DATA + "\n" + NORMAL_DATA;

  private static final String DATA_DAIL_CODE_EXTRA =
      NORMAIL_HEADER + NORMAL_DATA + "\n" + EXTRA_DATA;

  private static final String DATA_DAIL_CODE_NOT_REQ = NORMAIL_HEADER + DAIL_CODE_NOT_REQ_DATA;

  private static final String DATA_TOPIC_FAILURE = NORMAIL_HEADER + TOPIC_FAILURE_DATA;

  private static final String DATA_DAIL_CODE_FAILURE = NORMAIL_HEADER + DIAL_CODE_FAILURE_DATA;

  private static final String DATA_DAIL_CODE_UNIQUENESS_FAILURE =
      NORMAIL_HEADER + DAIL_CODE_UNIQUE_FAILURE_DATA;

  private static final String DATA_FAILURE_ONLY_HEADERS = NORMAIL_HEADER;

  private static final String TEXTBOOK_TOC_INPUT_MAPPING =
      getFileAsString("FrameworkForTextbookTocActorTest.json");
  private static final String MANDATORY_VALUES = getFileAsString("MandatoryValue.json");
  private static final String CONTENT_TYPE = "any";

  @Before
  public void setUp() {
    PowerMockito.mockStatic(TextBookTocUtil.class);
    PowerMockito.mockStatic(ProjectUtil.class);
    PowerMockito.mockStatic(Unirest.class);
    system = ActorSystem.create("system");
    when(ProjectUtil.getConfigValue(Mockito.anyString())).thenReturn(TEXTBOOK_TOC_INPUT_MAPPING);
    when(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_MAX_CSV_ROWS)).thenReturn("5");
    when(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_MANDATORY_FIELDS))
        .thenReturn(MANDATORY_VALUES);
    when(ProjectUtil.getConfigValue(JsonKey.TEXTBOOK_TOC_ALLOWED_CONTNET_TYPES))
        .thenReturn(CONTENT_TYPE);
    when(ProjectUtil.getConfigValue(JsonKey.EKSTEP_BASE_URL)).thenReturn("http://www.abc.com/");
    when(ProjectUtil.getConfigValue(JsonKey.UPDATE_HIERARCHY_API)).thenReturn("");
  }

  @Test
  public void testUpdateFailureWithCorrectAndIncorrectDialCodeData() throws IOException {
    mockRequiredMethods(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, DATA_DAIL_CODE_EXTRA);
    Assert.assertEquals(
        res.getCode(), ResponseCode.errorDialCodeNotReservedForTextBook.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateFailureDailcodeNotreq() throws IOException {
    mockRequiredMethods(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, DATA_DAIL_CODE_NOT_REQ);
    Assert.assertEquals(res.getCode(), ResponseCode.errorConflictingValues.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateFailureDuplicateEntry() throws IOException {
    mockRequiredMethods(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, DATA_DUPLICATE_ENTRY);
    Assert.assertEquals(res.getCode(), ResponseCode.errorDuplicateEntries.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateFailureblankCsv() throws IOException {
    mockRequiredMethods(false);
    ProjectCommonException res =
        (ProjectCommonException) doRequest(true, DATA_FAILURE_ONLY_HEADERS);
    Assert.assertEquals(res.getCode(), ResponseCode.blankCsvData.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateInvalideTopicFailure() throws IOException {
    mockRequiredMethods(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, DATA_TOPIC_FAILURE);
    Assert.assertEquals(res.getCode(), ResponseCode.errorInvalidTopic.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateInvalideDailcodeFailure() throws IOException {
    mockRequiredMethods(false);
    ProjectCommonException res = (ProjectCommonException) doRequest(true, DATA_DAIL_CODE_FAILURE);
    Assert.assertEquals(
        res.getCode(), ResponseCode.errorDialCodeNotReservedForTextBook.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateInvalideDialCodeUniquenessFailure() throws IOException {
    mockRequiredMethods(true);
    ProjectCommonException res =
        (ProjectCommonException) doRequest(true, DATA_DAIL_CODE_UNIQUENESS_FAILURE);
    Assert.assertEquals(res.getCode(), ResponseCode.errorDialCodeAlreadyAssociated.getErrorCode());
    Assert.assertNotNull(res);
  }

  @Test
  public void testUpdateSuccess() throws Exception {
    mockRequiredMethods(false);
    mockResponseSuccess();
    Response res = (Response) doRequest(false, DATA);
    Assert.assertNotNull(res);
  }

  private void mockResponseSuccess() throws Exception {
    HttpRequestWithBody http = Mockito.mock(HttpRequestWithBody.class);
    RequestBodyEntity entity = Mockito.mock(RequestBodyEntity.class);
    HttpResponse<String> res = Mockito.mock(HttpResponse.class);
    when(Unirest.patch(Mockito.anyString())).thenReturn(http);
    when(http.headers(Mockito.anyMap())).thenReturn(http);
    when(http.body(Mockito.anyString())).thenReturn(entity);
    when(entity.asString()).thenReturn(res);
    when(res.getBody()).thenReturn("{\"responseCode\" :\"OK\" }");
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

  private void mockRequiredMethods(boolean bool) {
    when(TextBookTocUtil.getRelatedFrameworkById(Mockito.anyString())).thenReturn(new Response());
    when(TextBookTocUtil.readHierarchy(Mockito.anyString())).thenReturn(getReadHierarchy(bool));
    when(TextBookTocUtil.readContent(Mockito.anyString())).thenReturn(getReadContentTextbookData());
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

  private static String getFileAsString(String fileName) {
    File file = null;

    try {
      file = new File(TextbookTocActorTest.class.getClassLoader().getResource(fileName).getFile());
      Path path = Paths.get(file.getPath());
      List<String> data = Files.readAllLines(path);
      return data.get(0);
    } catch (FileNotFoundException e) {
      ProjectLogger.log(e.getMessage(), e);
    } catch (IOException e) {
      ProjectLogger.log(e.getMessage(), e);
    }
    return null;
  }
}
