package edu.mayo.hadoop.commons.hbase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;

import edu.mayo.hadoop.commons.minicluster.MiniClusterUtil;

/**
 * Find a HBase configuration based on simple heuristics.
 * <p>
 * This class will determine an HBase configuration to return. A simple set of
 * rules are as follows:
 * <p>
 * <ul>
 * <li>if <code>/etc/hbase/conf</code> exists, use that configuration (HDP)</li>
 * <li>else start up a <a href=
 * "http://www.lopakalogic.com/articles/hadoop-articles/hadoop-testing-with-minicluster/">
 * mini-cluster</a></li>
 * </ul>
 * </p>
 * Created by Daniel Blezek on 2/23/16.
 */
public class AutoConfigure {
    static Logger logger = Logger.getLogger(AutoConfigure.class);

    public static org.apache.hadoop.conf.Configuration getConfiguration() throws Exception {

        // Check for a site wide configuration for HDP
        String configPath = "/etc/hbase/conf/hbase-site.xml";
        if (Files.exists(Paths.get(configPath))) {
            logger.debug("Found " + configPath + " configuring HBase");
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path(configPath));
            return conf;
        }

        // Return a mini-cluster
        Properties props = new Properties();

        for (String resourceName : new String[]{"/hbase.properties", "/hdfs.properties", "/yarn.properties", "/zookeeper.properties"}) {
            try (InputStream stream = AutoConfigure.class.getResourceAsStream(resourceName)) {
                props.putAll(MiniClusterUtil.loadPropertiesStream(stream));
            }
        }

        MiniClusterUtil.startHBASE(props);
        return MiniClusterUtil.getHbaseLocalCluster().getHbaseConfiguration();
    }
}
