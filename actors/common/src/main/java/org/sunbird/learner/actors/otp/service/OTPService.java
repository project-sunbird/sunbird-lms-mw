package org.sunbird.learner.actors.otp.service;

import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.learner.actors.otp.dao.OTPDao;
import org.sunbird.learner.actors.otp.dao.impl.OTPDaoImpl;

public class OTPService {

  private static OTPDao otpDao = OTPDaoImpl.getInstance();

  public Map<String, Object> getOTPDetails(String type, String key) {
    return otpDao.getOTPDetails(type, key);
  }

  public void insertOTPDetails(String type, String key, String otp) {
    otpDao.insertOTPDetails(type, key, otp);
  }

  public void deleteOtp(String type,String key){
    otpDao.deleteOtp(type,key);
  }

  public static String getOTPSMSBody(Map<String, String> smsTemplate) {
    try {
      Properties props = new Properties();
      props.put("resource.loader", "class");
      props.put(
          "class.resource.loader.class",
          "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");

      VelocityEngine ve = new VelocityEngine();
      ve.init(props);
      Template t = ve.getTemplate("/OTPSMSTemplate.vm");
      VelocityContext context = new VelocityContext(smsTemplate);
      StringWriter writer = new StringWriter();
      t.merge(context, writer);
      return writer.toString();
    } catch (Exception ex) {
      ProjectLogger.log(
          "OTPService:getOTPSMSBody: Exception occurred with error message = " + ex.getMessage(), ex);
    }
    return "";
  }

}
