package edu.mayo.hadoop.commons.examples;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;

import edu.mayo.hadoop.commons.minicluster.MiniClusterUtil;

/**
 * Created by m102417 on 2/12/16. checks that Zookeeper is up and running and
 * correctly responds to queries on localhost
 */
public class ZookeeperITCase {

    private static Properties prop;

    @Before
    public void setup() throws Exception {
        try (InputStream stream = HBaseITCase.class.getResourceAsStream("/zookeeper.properties")) {
            prop = MiniClusterUtil.loadPropertiesStream(stream);
        }
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
    public void running() {
        ZookeeperLocalCluster zookeeperLocalCluster = MiniClusterUtil.getZookeeperLocalCluster();
        assertEquals(prop.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY), zookeeperLocalCluster.getZookeeperConnectionString());
    }
}
