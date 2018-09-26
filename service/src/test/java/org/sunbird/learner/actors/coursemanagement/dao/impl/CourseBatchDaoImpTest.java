package org.sunbird.learner.actors.coursemanagement.dao.impl;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.dao.CourseBatchDao;
import org.sunbird.learner.actors.coursebatch.dao.impl.CourseBatchDaoImpl;
import org.sunbird.models.course.batch.CourseBatch;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class CourseBatchDaoImpTest {
  private CassandraOperation cassandraOperation;
  private CourseBatchDao courseBatchDaoImpl;
  private static String COURSE_BATCH_ID = "someId";

  @Before
  public void setUp() {
    courseBatchDaoImpl = new CourseBatchDaoImpl();
  }

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    Mockito.reset(cassandraOperation);
  }

  @Test
  public void testCreateCourseBatchSuccess() {
    PowerMockito.when(
            cassandraOperation.insertRecord(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(new Response());

    CourseBatch courseBatch = new CourseBatch();
    courseBatch.setId(COURSE_BATCH_ID);
    courseBatch.initCount();
    courseBatch.setStatus(0);
    Response insertResponse = courseBatchDaoImpl.create(courseBatch);
    Assert.assertTrue(null != insertResponse);
  }

  @Test
  public void testGetCourseBatchSuccess() {
    PowerMockito.when(
            cassandraOperation.getRecordById(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCourseBatchResponse(false));
    CourseBatch courseBatch = courseBatchDaoImpl.readById(COURSE_BATCH_ID);
    Assert.assertTrue(null != courseBatch);
  }

  @Test
  public void testGetCourseBatchFailur() {
    PowerMockito.when(
            cassandraOperation.getRecordById(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCourseBatchResponse(true));
    try {
      CourseBatch courseBatch = courseBatchDaoImpl.readById(COURSE_BATCH_ID);
    } catch (ProjectCommonException exception) {
      Assert.assertTrue(
          exception
              .getMessage()
              .equalsIgnoreCase(ResponseCode.invalidCourseBatchId.getErrorMessage()));
    }
  }

  @Test
  public void testUpdateCourseBatchSuccess() {
    PowerMockito.when(
            cassandraOperation.updateRecord(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(new Response());

    CourseBatch courseBatch = new CourseBatch();
    courseBatch.setId(COURSE_BATCH_ID);
    courseBatch.initCount();
    courseBatch.setStatus(0);
    Response insertResponse =
        courseBatchDaoImpl.update(new ObjectMapper().convertValue(courseBatch, Map.class));
    Assert.assertTrue(null != insertResponse);
  }

  private Response getCourseBatchResponse(boolean isEmpty) {
    Response response = new Response();
    if (!isEmpty)
      response.put(
          JsonKey.RESPONSE,
          new ArrayList<Map<String, Object>>(Arrays.asList(new HashMap<String, Object>())));
    else {
      response.put(JsonKey.RESPONSE, new ArrayList<Map<String, Object>>());
    }
    return response;
  }
}
