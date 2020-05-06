package de.hpi.julianweise.slave;

import akka.actor.testkit.typed.javadsl.LoggingTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

public class ADBSlaveTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @After
    public void cleanup() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @AfterClass
    public static void after() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetPool();
        ADBQueryManager.resetSingleton();
    }

    @Test
    public void expectHealthyStartup() {
        LoggingTestKit.info("DBSlave started")
                      .expect(testKit.system(), () -> testKit.spawn(ADBSlave.create()));
    }
}
