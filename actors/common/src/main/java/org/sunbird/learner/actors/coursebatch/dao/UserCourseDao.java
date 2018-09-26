package org.sunbird.learner.actors.coursebatch.dao;

import java.util.Map;
import org.sunbird.common.models.response.Response;

public interface UserCourseDao {
  Response createCourseEnrolment(Map<String, Object> map);
}
