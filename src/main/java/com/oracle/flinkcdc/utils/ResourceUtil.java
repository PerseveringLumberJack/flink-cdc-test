package com.oracle.flinkcdc.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class ResourceUtil {

    private static final Logger logger = LoggerFactory.getLogger(ResourceUtil.class);

    /**
     * 属性文件全名
     */
    private static final String PFILE = "/opt/trend_oper/flink-job.properties";
   // private static final String PFILE = "D:\\\\home\\\\down\\\\flink-job.properties";
    /**
     * 对应于属性文件的文件对象变量
     */
    private static File m_file = null;
    /**
     * 属性文件的最后修改日期
     */
    private static long m_lastModifiedTime = 0;

    private static ResourceBundle rb;

    private static BufferedInputStream inputStream;

    static {
        try {
         /*   m_file = new File(PFILE);
            m_lastModifiedTime = m_file.lastModified();
            inputStream = new BufferedInputStream(new FileInputStream(PFILE));*/
            //InputStream inputStream = ResourceBundle.class.getResourceAsStream("/flink-job.properties");
            InputStream inputStream = new BufferedInputStream(new FileInputStream(PFILE));
            rb = new PropertyResourceBundle(inputStream);
            inputStream.close();
        } catch (FileNotFoundException e) {

            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getKey(String key) {
      /*  long newTime = m_file.lastModified();
        if (newTime > m_lastModifiedTime) {
            // Get rid of the old properties
            try {
                m_lastModifiedTime = m_file.lastModified();
                inputStream = new BufferedInputStream(new FileInputStream(PFILE));
                rb = new PropertyResourceBundle(inputStream);
                inputStream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
        String result = rb.getString(key);

        return result;
    }

}
