package de.hpi.julianweise.slave;

import akka.actor.testkit.typed.javadsl.LoggingTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import org.junit.ClassRule;
import org.junit.Test;

public class ADBSlaveSupervisorTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void expectHealthyStartup() {
        LoggingTestKit.info("DBSlave started")
                      .expect(testKit.system(), () -> testKit.spawn(ADBSlaveSupervisor.create()));
    }
}
