package org.sunbird.learner.actors.skill.dao;

import java.util.List;
import java.util.Map;
import org.sunbird.models.user.skill.Skill;

public interface UserSkillDao {

  /**
   * Add skills for user.
   *
   * @param userSkill map containing information about user skill
   */
  void add(Map<String, Object> userSkill);

  /**
   * Delete skill for user.
   *
   * @param identifierList list of id which needs to be deleted
   * @return boolean
   */
  boolean delete(List<String> identifierList);

  /**
   * Read skill for user.
   *
   * @param id skill id
   * @return skill
   */
  Skill read(String id);

  /**
   * Update skill for user.
   *
   * @param skill skill which need to be updated
   * @return
   */
  void update(Skill skill);
}
