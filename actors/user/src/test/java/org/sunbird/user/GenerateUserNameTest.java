package org.sunbird.user;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;

public class GenerateUserNameTest {

  private static String asciiName = "Some Random Name";
  private static String nonAsciiName = "कोई अज्ञात नाम";
  private static Pattern pattern;
  private static String userAsciiNameRegex = "(^([a-z])+[0-9]{4})";
  private static String userNonAsciiNameRegex = "(^(कोईअज्ञातनाम)+[0-9]{4})";
  private static UserService userService = new UserServiceImpl();

  @Test
  public void generateUserNameForASCIIStringSuccess() {
    assertTrue(userNameValidation(asciiName, userAsciiNameRegex));
  }

  @Test
  public void generateUserNameForEmptyNameFailure() {
    List<String> result = userService.generateUsernames("", new ArrayList<String>());
    assertTrue(result == null);
  }

  @Test
  public void generateUserNameForNonASCIISuccess() {
    assertTrue(userNameValidation(nonAsciiName, userNonAsciiNameRegex));
  }

  private boolean userNameValidation(String name, String validatorRegex) {
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
