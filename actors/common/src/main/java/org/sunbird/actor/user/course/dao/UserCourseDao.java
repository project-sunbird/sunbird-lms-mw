package org.sunbird.actor.user.course.dao;

import java.util.Map;
import org.sunbird.models.user.courses.UserCourses;

public interface UserCourseDao {

  /**
   * Get user courses information.
   *
   * @param identifier list i.e. courseId, batchId, userId
   * @return user course information
   */
  UserCourses read(String id);

  /**
   * Update user skills.
   *
   * @param
   */
  String update(Map<String, Object> updateAttributes, Map<String, Object> compositeKey);
}
