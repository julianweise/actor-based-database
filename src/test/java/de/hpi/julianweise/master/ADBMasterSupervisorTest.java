package de.hpi.julianweise.master;

import akka.actor.testkit.typed.javadsl.LoggingTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBMasterSupervisorTest {

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
                              () -> testKit.spawn(ADBMasterSupervisorFactory.createDefault(mockedProcess)));
    }

    @Test
    public void expectNoResponseForStartOperationalService() throws IOException {
        File testFile = folder.newFile("empty-test-file-2.csv");

        MasterConfiguration masterConfiguration = new MasterConfiguration();
        masterConfiguration.setInputFile(testFile.toPath());

        TestProbe<ADBLoadAndDistributeDataProcess.Command> testRef = testKit.createTestProbe();
        Behavior<ADBLoadAndDistributeDataProcess.Command> mockedProcessBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBLoadAndDistributeDataProcess.Command> mockedProcess =
                Behaviors.monitor(ADBLoadAndDistributeDataProcess.Command.class, testRef.ref(), mockedProcessBehavior);

        ActorRef<ADBMasterSupervisor.Command> masterSupervisor =
                testKit.spawn(ADBMasterSupervisorFactory.createDefault(mockedProcess));

        masterSupervisor.tell(new ADBMasterSupervisor.StartOperationalService());
        assertThat(true).isTrue();
    }
}
