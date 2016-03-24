package edu.mayo.hadoop.commons.examples;

import static org.junit.Assert.assertTrue;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;

/**
 * Created by m102417 on 2/12/16. checks that Zookeeper is up and running and correctly responds to queries on localhost
 */
public class ZookeeperITCase {

    private static Properties prop;

    @Before
    public void setup() throws Exception {
        // Get a config, just to have AutoConfigure startup the cluster
        AutoConfigure.getConfiguration();
    }

    @After
    public void teardown() throws Exception {
        AutoConfigure.stop();
    }

    /**
     * tests that zookeeper is up and running
     * 
     * @see https://ihong5.wordpress.com/2014/05/27/maven-how-to-connect-to-a-zookeeper-in-java/
     * 
     * @throws Exception
     */
    @Test
    public void running() throws Exception {
        CountDownLatch connectedSignal = new CountDownLatch(1);

        ZooKeeper zookeeper = new ZooKeeper("127.0.0.1:22010", 5000, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }

            }
        });
        assertTrue("Connect to zookeeper", connectedSignal.await(10, TimeUnit.SECONDS));
    }
}
