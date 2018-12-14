package org.sunbird.user;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

public class UserServiceGenerateUsernameTest {

  private static String englishName = "Some Random Name";
  private static String hindiName = "कोई अज्ञात नाम";
  private static Pattern pattern;
  private static String userAsciiNameRegex = "(^([a-z])+[0-9]{4})";
  private static String userNonAsciiNameRegex = "(^(कोईअज्ञातनाम)+[0-9]{4})";
  private static UserService userService = new UserServiceImpl();

  @Test
  public void testGenerateUsernamesSuccessWithEnglishName() {
    assertTrue(performTest(englishName, userAsciiNameRegex));
  }

  @Test
  public void testGenerateUsernamesFailureWithBlankName() {
    List<String> result = userService.generateUsernames("", new ArrayList<String>());
    assertTrue(result == null);
  }

  @Test
  public void testGenerateUsernamesSuccessWithHindiName() {
    assertTrue(performTest(hindiName, userNonAsciiNameRegex));
  }

  private boolean performTest(String name, String validatorRegex) {
    List<String> result = userService.generateUsernames(name, new ArrayList<String>());
    pattern = Pattern.compile(validatorRegex);
    boolean flag = true;
    for (int i = 0; i < result.size(); i++) {
      Matcher matcher = pattern.matcher(result.get(i));
      if (!matcher.matches()) {
        flag = false;
        break;
      }
    }
    return flag;
  }
}
