package org.sunbird.learner.actors.coursebatch.dao;

import java.util.List;
import java.util.Map;
import org.sunbird.common.models.response.Response;

/** Created by rajatgupta on 12/09/18. */
public interface CourseBatchDao {

  /**
   * Update system setting.
   *
   * @param map Create course batch map
   * @return Response containing setting identifier.
   */
  Response create(Map<String, Object> map);

  /**
   * Update system setting.
   *
   * @param map Create course batch map
   * @return Response containing setting identifier.
   */
  Response update(Map<String, Object> map);

  /**
   * Read system setting for given identifier.
   *
   * @param id fetch course batch using id
   * @return course batch information
   */
  List<Map<String, Object>> readById(String id);

  /**
   * Read all system settings.
   *
   * @return Response containing list of course batch.
   */
  List<Map<String, Object>> readAll();

  /**
   * Read all system settings.
   *
   * @return Response containing operation is success or not.
   */
  Response delete(String id);

  List<Map<String, Object>> readPublishedCourse(String id);

  Response createCourseEnrolment(Map<String, Object> map);
}
