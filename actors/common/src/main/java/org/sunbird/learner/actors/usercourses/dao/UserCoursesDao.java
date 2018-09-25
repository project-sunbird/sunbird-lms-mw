package org.sunbird.learner.actors.usercourses.dao;

import java.util.Map;
import org.sunbird.common.models.response.Response;
import org.sunbird.models.user.courses.UserCourses;

public interface UserCoursesDao {

  /**
   * Get user courses information.
   *
   * @param identifier list i.e. courseId, batchId, userId
   * @return user course information
   */
  UserCourses read(String id);

  /**
   * Update user courses information.
   *
   * @param
   */
  Response update(Map<String, Object> updateAttributes);

  /**
   * Insert user skills information.
   *
   * @param
   */
  Response insert(Map<String, Object> userCoursesDetails);
}
