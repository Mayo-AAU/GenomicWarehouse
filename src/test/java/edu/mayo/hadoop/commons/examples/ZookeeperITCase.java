package edu.mayo.hadoop.commons.examples;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import edu.mayo.hadoop.commons.minicluster.MiniClusterUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by m102417 on 2/12/16.
 * checks that Zookeeper is up and running and correctly responds to queries on localhost
 */
public class ZookeeperITCase {

    private static Properties prop;

    @Before
    public void setup() throws Exception {
        prop = MiniClusterUtil.loadPropertiesFile("target/test-classes/zookeeper.properties");
        MiniClusterUtil.startZookeeper(prop);
    }

    @After
    public void teardown() throws Exception {
        MiniClusterUtil.stopZookeeper();
    }

    /**
     * tests that zookeeper is up and running
     */
    @Test
    public void running(){
        ZookeeperLocalCluster zookeeperLocalCluster = MiniClusterUtil.getZookeeperLocalCluster();
        assertEquals(prop.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY),
                zookeeperLocalCluster.getZookeeperConnectionString());
    }
}
