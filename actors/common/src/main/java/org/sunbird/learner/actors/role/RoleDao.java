package org.sunbird.learner.actors.role;

import java.util.List;
import org.sunbird.models.role.Role;

public interface RoleDao {

  /**
   * Get All Records
   *
   * @return List of all Roles from Role Table
   */
  List<Role> getAllRecords();
}
