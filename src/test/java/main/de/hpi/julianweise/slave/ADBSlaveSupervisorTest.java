package main.de.hpi.julianweise.slave;

import akka.actor.testkit.typed.javadsl.LoggingTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import de.hpi.julianweise.slave.ADBSlaveSupervisor;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ADBSlaveSupervisorTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @After
    public void after() {
        folder.delete();
    }

    @Test
    public void expectHealthyStartup() {
        LoggingTestKit.info("DBSlave started")
                      .expect(testKit.system(), () ->
                              testKit.spawn(ADBSlaveSupervisor.create()));
    }
}
