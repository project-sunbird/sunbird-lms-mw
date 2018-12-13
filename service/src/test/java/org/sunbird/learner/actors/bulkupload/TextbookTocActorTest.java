package org.sunbird.learner.actors.bulkupload;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.ExecutionContexts;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
import org.sunbird.common.models.util.TextbookActorOperation;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.content.util.TextBookTocUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TextBookTocUtil.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
public class TextbookTocActorTest {

    private static ActorSystem system;
    private static final Props props = Props.create(TextbookTocActor.class);
    private static ObjectMapper mapper = new ObjectMapper();
    private static final String TB_WITHOUT_CHILDREN = "{\"id\":\"ekstep.content.find\",\"ver\":\"3.0\",\"ts\":\"2018-12-12T06:21:17ZZ\",\"params\":{\"resmsgid\":\"bf8273ca-be0f-4062-8986-fbfff07002ac\",\"msgid\":null,\"err\":null,\"status\":\"successful\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"content\":{\"ownershipType\":[\"createdBy\"],\"code\":\"Science\",\"channel\":\"in.ekstep\",\"description\":\"Test TextBook\",\"language\":[\"English\"],\"mimeType\":\"application/vnd.ekstep.content-collection\",\"idealScreenSize\":\"normal\",\"createdOn\":\"2018-12-12T06:20:57.814+0000\",\"appId\":\"ekstep_portal\",\"contentDisposition\":\"inline\",\"contentEncoding\":\"gzip\",\"lastUpdatedOn\":\"2018-12-12T06:20:57.814+0000\",\"contentType\":\"TextBook\",\"dialcodeRequired\":\"No\",\"identifier\":\"do_11265332762881228812868\",\"audience\":[\"Learner\"],\"visibility\":\"Default\",\"os\":[\"All\"],\"consumerId\":\"a6654129-b58d-4dd8-9cf2-f8f3c2f458bc\",\"mediaType\":\"content\",\"osId\":\"org.ekstep.quiz.app\",\"languageCode\":\"en\",\"versionKey\":\"1544595657814\",\"idealScreenDensity\":\"hdpi\",\"framework\":\"NCF\",\"compatibilityLevel\":1,\"name\":\"Science-10\",\"status\":\"Draft\"}}}";
    private static final String TB_CREATE_VALID_REQ = "{\"mode\":\"create\",\"fileData\":[{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.1.1 Key parts in the head\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.2 .1 Respiratory System\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"Textbook\":\"Science-10\"}}]}";
    private static final String DUPLICATE_ROW_REQ = "{\"mode\":\"create\",\"fileData\":[{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.1.1 Key parts in the head\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.2 .1 Respiratory System\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"Textbook\":\"Science-10\"}}]}";
    private static final String REQUIRED_FIELD_MISS_REQ ="{\"mode\":\"create\",\"fileData\":[{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:3\":\"5.1.1 Key parts in the head\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:3\":\"5.2 .1 Respiratory System\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.1 Parts of Body\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"purpose\":\"Video of lungs pumping\",\"subject\":\"Science\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"Yes\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"L:2\":\"5.2 Organ Systems\",\"Textbook\":\"Science-10\"}},{\"metadata\":{\"gradeLevel\":\"Class 10\",\"keywords\":\"head,eyes,nose,mouth\",\"subject\":\"Science\",\"description\":\"Explains key parts in the head such as eyes,nose,ears and mouth\",\"medium\":\"Hindi\",\"dialcodeRequired\":\"No\"},\"hierarchy\":{\"L:1\":\"5. Human Body\",\"Textbook\":\"Science-10\"}}]}";

    @Before
    public void setUp() {
        PowerMockito.mockStatic(TextBookTocUtil.class);
        system = ActorSystem.create("system");
    }


    @Test
    public void testDuplicateRow() throws IOException {
        TestKit probe = new TestKit(system);
        ActorRef toc = system.actorOf(props);

        Map<String, Object> readTb = getTbWithoutChildren();
        when(TextBookTocUtil.readContent(Mockito.anyString())).thenReturn(readTb);
        Request request = new Request();
        request.setContext(new HashMap<String, Object>(){{put("userId", "test");}});
        request.put(JsonKey.TEXTBOOK_ID, "do_11265332762881228812868");
        request.put(JsonKey.DATA, mapper.readValue(DUPLICATE_ROW_REQ, Map.class));
        request.setOperation(TextbookActorOperation.TEXTBOOK_TOC_UPLOAD.getValue());

        toc.tell(request, probe.getRef());
        ProjectCommonException res =
                probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
        Assert.assertTrue(null != res);
        Assert.assertEquals("Duplicate Textbook node found. Please check and upload again. Row number 6", res.getMessage());
    }

    @Test
    public void testReqFieldMiss() throws IOException {
        TestKit probe = new TestKit(system);
        ActorRef toc = system.actorOf(props);

        Map<String, Object> readTb = getTbWithoutChildren();
        when(TextBookTocUtil.readContent(Mockito.anyString())).thenReturn(readTb);
        Request request = new Request();
        request.setContext(new HashMap<String, Object>(){{put("userId", "test");}});
        request.put(JsonKey.TEXTBOOK_ID, "do_11265332762881228812868");
        request.put(JsonKey.DATA, mapper.readValue(REQUIRED_FIELD_MISS_REQ, Map.class));
        request.setOperation(TextbookActorOperation.TEXTBOOK_TOC_UPLOAD.getValue());

        toc.tell(request, probe.getRef());
        ProjectCommonException res =
                probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
        Assert.assertTrue(null != res);
        Assert.assertEquals("Data in mandatory fields is missing. Mandatory fields are: [Textbook Name, Level 1 Textbook Unit]", res.getMessage());
    }


    @Test
    public void testDownloadInvalid() throws IOException {
        TestKit probe = new TestKit(system);
        ActorRef toc = system.actorOf(props);

        Map<String, Object> readTb = getTbWithoutChildren();
        when(TextBookTocUtil.readContent(Mockito.anyString())).thenReturn(readTb);
        Request request = new Request();
        request.setContext(new HashMap<String, Object>(){{put("userId", "test");}});
        request.put(JsonKey.TEXTBOOK_ID, "");
        request.setOperation(TextbookActorOperation.TEXTBOOK_TOC_URL.getValue());

        toc.tell(request, probe.getRef());
        ProjectCommonException res =
                probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
        Assert.assertTrue(null != res);
        Assert.assertEquals("Invalid Textbook. Please Provide Valid Textbook Identifier.", res.getMessage());
    }


    @Ignore
    public void testTocUplodSuccess() throws IOException {
        TestKit probe = new TestKit(system);
        ActorRef toc = system.actorOf(props);

        Map<String, Object> readTb = getTbWithoutChildren();
        when(TextBookTocUtil.readContent(Mockito.anyString())).thenReturn(readTb);

        Request request = new Request();
        request.setContext(new HashMap<String, Object>(){{put("userId", "test");}});
        request.put(JsonKey.TEXTBOOK_ID, "do_11265332762881228812868");
        request.put(JsonKey.DATA, mapper.readValue(TB_CREATE_VALID_REQ, Map.class));
        request.setOperation(TextbookActorOperation.TEXTBOOK_TOC_UPLOAD.getValue());

        toc.tell(request, probe.getRef());
        Response res = probe.expectMsgClass(duration("100 second"), Response.class);
        Assert.assertNotNull(res);
        Assert.assertEquals("OK", res.getResponseCode().name());
    }

    private static Map<String, Object> getTbWithoutChildren() throws IOException {
        Map<String, Object> response = mapper.readValue(TB_WITHOUT_CHILDREN, Map.class);
        return response;
    }

    private static Response getSuccess() {
        Response response = new Response();
        response.put("content_id", "do_11265332762881228812868");
        return response;
    }



}
