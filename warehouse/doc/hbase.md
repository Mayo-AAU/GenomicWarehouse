# Adventures in connecting to HBase

First off, getting your ports correct is very, very important.  [The HBase port](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.3.2/bk_HDP_Reference_Guide/content/hbase-ports.html) changed in HDP `2.3` to `16000` and it is not forwarded by default in the VirtualBox Sandbox image.

## Initial problems and help

After some initial problems, posted to HortonWorks Community.

[https://community.hortonworks.com/questions/14136/how-do-i-connect-to-hbase-on-sandbox-vm-from-host.html](https://community.hortonworks.com/questions/14136/how-do-i-connect-to-hbase-on-sandbox-vm-from-host.html)

---

I have setup the HDP Sandbox 2.3.2 under VirtualBox running on my Mac. I am able to interact with HBase from inside the VM without any problem. However, when I try to connect via Java from my host, the connection fails. I have forwarded port 16000 and created a "sandbox.hortonworks.com" entry in /etc/hosts (similar to this post). Here is my Java code:

    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
    conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    HBaseAdmin.checkHBaseAvailable(conf);
    
**Update:**

A more helpful exception than the one below:

Master is not running: org.apache.hadoop.hbase.MasterNotRunningException: The node /hbase is not in ZooKeeper. It should have been written by the master. Check the value configured in 'zookeeper.znode.parent'. There could be a mismatch with the one configured in the master.
Not sure why '/hbase' is being used, when I set "zookeeper.znode.parent" to "/hbase-unsecure" explicitly!

**Update #2:**

Running the code on the Sandbox gives exactly the same error. Any help?

--

Posted to HortonWorks Community and got a helpful response:

