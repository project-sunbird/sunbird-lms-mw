package org.sunbird.learner.actors.role.group.dao;

import java.util.List;
import org.sunbird.models.role.group.RoleGroup;

public interface RoleGroupDao {

  /**
   * Get All RoleGroups
   *
   * @return List of all RoleGroups from RoleGroup Table
   */
  List<RoleGroup> getRoleGroups();
}
