package org.sunbird.learner.actors.role.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.role.dao.RoleDao;
import org.sunbird.learner.actors.role.dao.impl.RoleDaoImpl;
import org.sunbird.learner.actors.role.group.service.RoleGroupService;
import org.sunbird.learner.actors.url.action.service.UrlActionService;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.models.role.Role;

public class RoleService {

  private static RoleDao roleDao = RoleDaoImpl.getInstance();

  @SuppressWarnings("unchecked")
  public static Response getUserRoles() {
    Response mergeResponse = new Response();
    List<Map<String, Object>> resposnemap = new ArrayList<>();
    List<Role> roleList = roleDao.getRoles();
    if (roleList != null && !(roleList.isEmpty())) {
      // This map will have all the master roles
      for (Role role : roleList) {
        Map<String, Object> roleResponseMap = new HashMap<>();
        roleResponseMap.put(JsonKey.ID, role.getId());
        roleResponseMap.put(JsonKey.NAME, role.getName());
        List<String> roleGroupIdList = role.getRoleGroupId();
        List<Map<String, Object>> actionGroupListMap = new ArrayList<>();
        roleResponseMap.put(JsonKey.ACTION_GROUPS, actionGroupListMap);
        Map<String, Object> subRoleResponseMap = null;
        for (String roleGroupId : roleGroupIdList) {
          subRoleResponseMap = new HashMap<>();
          Map<String, Object> subRoleMap = RoleGroupService.getRoleGroupMap(roleGroupId);
          List<String> subRole = (List) subRoleMap.get(JsonKey.URL_ACTION_ID);
          List<Map<String, Object>> roleUrlResponList = new ArrayList<>();
          subRoleResponseMap.put(JsonKey.ID, subRoleMap.get(JsonKey.ID));
          subRoleResponseMap.put(JsonKey.NAME, subRoleMap.get(JsonKey.NAME));

          for (String rolemap : subRole) {
            roleUrlResponList.add(UrlActionService.getUrlActionMap(rolemap));
          }
          if (subRoleResponseMap.containsKey(JsonKey.ACTIONS)) {
            List<Map<String, Object>> listOfMap =
                (List<Map<String, Object>>) subRoleResponseMap.get(JsonKey.ACTIONS);
            listOfMap.addAll(roleUrlResponList);
          } else {
            subRoleResponseMap.put(JsonKey.ACTIONS, roleUrlResponList);
          }
          actionGroupListMap.add(subRoleResponseMap);
        }
        resposnemap.add(roleResponseMap);
      }
    }
    mergeResponse.getResult().put(JsonKey.ROLES, resposnemap);
    return mergeResponse;
  }

  public static void validateRoles(List<String> roleList) {
    Map<String, Object> roleMap = DataCacheHandler.getRoleMap();
    if (null != roleMap && !roleMap.isEmpty()) {
      for (String role : roleList) {
        if (null == roleMap.get(role.trim())) {
          throw new ProjectCommonException(
              ResponseCode.invalidRole.getErrorCode(),
              ResponseCode.invalidRole.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
  }
}
