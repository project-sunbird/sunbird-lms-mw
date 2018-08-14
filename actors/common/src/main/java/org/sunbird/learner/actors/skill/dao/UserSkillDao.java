package org.sunbird.learner.actors.skill.dao;

import java.util.List;
import java.util.Map;
import org.sunbird.models.user.skill.Skill;

public interface UserSkillDao {

  /**
   * This method will add skills for user ProjectCommonException.
   *
   * @param userSkill map containing information about user skill
   */
  void add(Map<String, Object> userSkill);

  /**
   * This method will delete skill for user ProjectCommonException.
   *
   * @param identifierList list of id which needs to be deleted
   * @return boolean
   */
  boolean delete(List<String> identifierList);

  /**
   * This method will read skill for user ProjectCommonException.
   *
   * @param id skill id
   * @return skill
   */
  Skill read(String id);

  /**
   * This method will update skill for user ProjectCommonException.
   *
   * @param skill skill which need to be updated
   * @return
   */
  void update(Skill skill);
}
