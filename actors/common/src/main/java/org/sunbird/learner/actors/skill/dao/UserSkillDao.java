package org.sunbird.learner.actors.skill.dao;

import java.util.List;
import java.util.Map;
import org.sunbird.models.user.skill.Skill;

public interface UserSkillDao {

  /** This method will add skills for user ProjectCommonException. */
  void addUserSkill(Map<String, Object> userSkill);

  /** This method will delete skill for user ProjectCommonException. */
  boolean deleteUserSkill(List<String> identifierList);

  /** This method will read skill for user ProjectCommonException. */
  Skill read(String id);
  /** This method will update skill for user ProjectCommonException. */
  void update(Skill skill);
}
