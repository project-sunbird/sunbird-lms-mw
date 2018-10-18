package org.sunbird.learner.actors.role.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.learner.actors.role.dao.RoleDao;
import org.sunbird.learner.actors.role.dao.impl.RoleDaoImpl;
import org.sunbird.learner.actors.role.group.dao.RoleGroupDao;
import org.sunbird.learner.actors.role.group.dao.impl.RoleGroupDaoImpl;
import org.sunbird.learner.actors.url.action.dao.UrlActionDao;
import org.sunbird.learner.actors.url.action.dao.impl.UrlActionDaoImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.models.role.Role;
import org.sunbird.models.role.group.RoleGroup;
import org.sunbird.models.url.action.UrlAction;

public class RoleService {

  private static RoleDao roleDao = RoleDaoImpl.getInstance();
  private static RoleGroupDao roleGroupDao = RoleGroupDaoImpl.getInstance();
  private static UrlActionDao urlActionDao = UrlActionDaoImpl.getInstance();

  @SuppressWarnings("unchecked")
  private Response getUserRoles() {
    Util.DbInfo roleDbInfo = Util.dbInfoMap.get(JsonKey.ROLE);
    Util.DbInfo roleGroupDbInfo = Util.dbInfoMap.get(JsonKey.ROLE_GROUP);
    Util.DbInfo urlActionDbInfo = Util.dbInfoMap.get(JsonKey.URL_ACTION);
    Response mergeResponse = new Response();
    List<Map<String, Object>> resposnemap = new ArrayList<>();
    List<Map<String, Object>> list = null;
    List<Role> roleList = roleDao.getRoles();
    List<RoleGroup> roleGroupList = roleGroupDao.getRoleGroups();
    List<UrlAction> urlActionList = urlActionDao.getUrlActions();
    if (roleList != null && !(roleList.isEmpty())) {
      // This map will have all the master roles
      for (Role role : roleList) {
        Map<String, Object> roleResponseMap = new HashMap<>();
        roleResponseMap.put(JsonKey.ID, role.getId());
        roleResponseMap.put(JsonKey.NAME, role.getName());
        List<String> roleGroup = role.getRoleGroupId();
        List<Map<String, Object>> actionGroupListMap = new ArrayList<>();
        roleResponseMap.put(JsonKey.ACTION_GROUPS, actionGroupListMap);
        Map<String, Object> subRoleResponseMap = null;
        for (String val : roleGroup) {
          subRoleResponseMap = new HashMap<>();
          Map<String, Object> subRoleMap = getSubRoleListMap(roleGroupMap, val);
          List<String> subRole = (List) subRoleMap.get(JsonKey.URL_ACTION_ID);
          List<Map<String, Object>> roleUrlResponList = new ArrayList<>();
          subRoleResponseMap.put(JsonKey.ID, subRoleMap.get(JsonKey.ID));
          subRoleResponseMap.put(JsonKey.NAME, subRoleMap.get(JsonKey.NAME));
          for (String rolemap : subRole) {
            roleUrlResponList.add(getRoleAction(urlActionListMap, rolemap));
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
  /*
  Response response =
      cassandraOperation.getAllRecords(roleDbInfo.getKeySpace(), roleDbInfo.getTableName());
  Response rolegroup =
      cassandraOperation.getAllRecords(
          roleGroupDbInfo.getKeySpace(), roleGroupDbInfo.getTableName());
  Response urlAction =
      cassandraOperation.getAllRecords(
          urlActionDbInfo.getKeySpace(), urlActionDbInfo.getTableName());
  List<Map<String, Object>> urlActionListMap =
      (List<Map<String, Object>>) urlAction.getResult().get(JsonKey.RESPONSE);
  List<Map<String, Object>> roleGroupMap =
      (List<Map<String, Object>>) rolegroup.getResult().get(JsonKey.RESPONSE);
  list = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);

  if (roleList != null && !(roleList.isEmpty())) {
    // This map will have all the master roles
    for (Map<String, Object> map : list) {
  	  for (Role role : roleList) {
      Map<String, Object> roleResponseMap = new HashMap<>();
      roleResponseMap.put(JsonKey.ID, map.get(JsonKey.ID));
      roleResponseMap.put(JsonKey.NAME, map.get(JsonKey.NAME));
      List<String> roleGroup = (List) map.get(JsonKey.ROLE_GROUP_ID);
      List<Map<String, Object>> actionGroupListMap = new ArrayList<>();
      roleResponseMap.put(JsonKey.ACTION_GROUPS, actionGroupListMap);
      Map<String, Object> subRoleResponseMap = null;
      for (String val : roleGroup) {
        subRoleResponseMap = new HashMap<>();
        Map<String, Object> subRoleMap = getSubRoleListMap(roleGroupMap, val);
        List<String> subRole = (List) subRoleMap.get(JsonKey.URL_ACTION_ID);
        List<Map<String, Object>> roleUrlResponList = new ArrayList<>();
        subRoleResponseMap.put(JsonKey.ID, subRoleMap.get(JsonKey.ID));
        subRoleResponseMap.put(JsonKey.NAME, subRoleMap.get(JsonKey.NAME));
        for (String rolemap : subRole) {
          roleUrlResponList.add(getRoleAction(urlActionListMap, rolemap));
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
  */
  private Map<String, Object> getSubRoleListMap(
      List<Map<String, Object>> urlActionListMap, String roleName) {
    Map<String, Object> response = new HashMap<>();
    if (urlActionListMap != null && !(urlActionListMap.isEmpty())) {
      for (Map<String, Object> map : urlActionListMap) {
        if (map.get(JsonKey.ID).equals(roleName)) {
          response.put(JsonKey.ID, map.get(JsonKey.ID));
          response.put(JsonKey.NAME, map.get(JsonKey.NAME));
          response.put(
              JsonKey.URL_ACTION_ID,
              map.get(JsonKey.URL_ACTION_ID) != null
                  ? map.get(JsonKey.URL_ACTION_ID)
                  : new ArrayList<>());
          return response;
        }
      }
    }
    return response;
  }
}
