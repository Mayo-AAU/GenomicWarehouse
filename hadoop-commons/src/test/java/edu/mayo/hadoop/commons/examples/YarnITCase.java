package edu.mayo.hadoop.commons.examples;

import edu.mayo.hadoop.commons.minicluster.MiniClusterUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import com.github.sakserv.minicluster.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;

/**
 * Created by m102417 on 2/15/16.
 *
 * Checks that the Yarn service is able to be booted and running
 *
 */
public class YarnITCase {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(YarnITCase.class);
    //properties file
    public static final String YARN_PROPS = "target/test-classes/yarn.properties";
    private static Properties props;
    private static YarnLocalCluster yarnLocalCluster;

    static {
        try {
            props = MiniClusterUtil.loadPropertiesFile(YARN_PROPS);
        } catch(IOException e) {
            LOG.error("Unable to load property file: {}", YARN_PROPS);
        }
    }

    @Before
    public void setup(){
        yarnLocalCluster = MiniClusterUtil.startYarn(props);
    }



    @AfterClass
    public static void tearDown() throws Exception {
        yarnLocalCluster.stop();
    }

//TODO: Yarn test not up and running... Client was removed or refactored or something, can't figure it out yet.
//    @Test
//    public void testYarnLocalClusterIntegrationTest() {
//
//        String[] args = new String[7];
//        args[0] = "whoami";
//        args[1] = "1";
//        args[2] = getClass().getClassLoader().getResource("simple-yarn-app-1.1.0.jar").toString();
//        args[3] = yarnLocalCluster.getResourceManagerAddress();
//        args[4] = yarnLocalCluster.getResourceManagerHostname();
//        args[5] = yarnLocalCluster.getResourceManagerSchedulerAddress();
//        args[6] = yarnLocalCluster.getResourceManagerResourceTrackerAddress();
//
//        try {
//            Client.main(args);
//        } catch(Exception e) {
//            e.printStackTrace();
//        }
//
//        // simple yarn app running "whoami",
//        // validate the container contents matches the java user.name
//        assertEquals(System.getProperty("user.name"), getStdoutContents());
//
//    }

    public String getStdoutContents() {
        String contents = "";
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(getStdoutPath()));
            contents = new String(encoded, Charset.defaultCharset()).trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contents;
    }

    public String getStdoutPath() {
        File dir = new File("./target/" + yarnLocalCluster.getTestName());
        String[] nmDirs = dir.list();
        for (String nmDir: nmDirs) {
            if (nmDir.contains("logDir")) {
                String[] appDirs = new File(dir.toString() + "/" + nmDir).list();
                for (String appDir: appDirs) {
                    if (appDir.contains("0001")) {
                        String[] containerDirs = new File(dir.toString() + "/" + nmDir + "/" + appDir).list();
                        for (String containerDir: containerDirs) {
                            if(containerDir.contains("000002")) {
                                return dir.toString() + "/" + nmDir + "/" + appDir + "/" + containerDir + "/stdout";
                            }
                        }
                    }
                }
            }
        }
        return "";
    }

}
