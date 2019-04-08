package org.sunbird.learner.actors.coursebatch.dao;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.dao.impl.UserCoursesDaoImpl;
import org.sunbird.models.user.courses.UserCourses;

/** Created by rajatgupta on 08/04/19. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class UserCoursesDaoTest {
  private CassandraOperation cassandraOperation;
  private UserCoursesDao userCoursesDao;

  @BeforeClass
  public static void setUp() {}

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    userCoursesDao = new UserCoursesDaoImpl();
  }

  @Test
  public void readUserCoursesFailure() {
    Response readResponse = new Response();
    //        readResponse.put(JsonKey.RESPONSE, Arrays.asList(new HashMap<>()));
    when(cassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(readResponse);
    UserCourses response = userCoursesDao.read(JsonKey.ID);
    Assert.assertEquals(null, response);
  }
}
