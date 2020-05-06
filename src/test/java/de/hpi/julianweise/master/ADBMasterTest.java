package de.hpi.julianweise.master;

import akka.actor.testkit.typed.javadsl.LoggingTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.master.config.MasterConfiguration;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcess;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class ADBMasterTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @ClassRule
    public final static TemporaryFolder folder = new TemporaryFolder();

    @AfterClass
    public static void after() {
        folder.delete();
        testKit.after();
    }

    @Test
    public void expectHealthyStartup() throws IOException {
        File testFile = folder.newFile("empty-test-file.csv");

        MasterConfiguration masterConfiguration = new MasterConfiguration();
        masterConfiguration.setInputFile(testFile.toPath());

        TestProbe<ADBLoadAndDistributeDataProcess.Command> testRef = testKit.createTestProbe();
        Behavior<ADBLoadAndDistributeDataProcess.Command> mockedProcessBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBLoadAndDistributeDataProcess.Command> mockedProcess =
                Behaviors.monitor(ADBLoadAndDistributeDataProcess.Command.class, testRef.ref(), mockedProcessBehavior);

        LoggingTestKit.info("DBMaster started")
                      .expect(testKit.system(),
                              () -> testKit.spawn(ADBMasterFactory.createDefault(mockedProcess)));
    }
}
