package main.de.hpi.julianweise.master;

import akka.actor.testkit.typed.javadsl.LoggingTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.master.ADBMasterSupervisor;
import de.hpi.julianweise.master.MasterConfiguration;
import main.de.hpi.julianweise.csv.TestEntityFactory;
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
    public static TemporaryFolder folder = new TemporaryFolder();

    @AfterClass
    public static void after() {
        folder.delete();
    }

    @Test
    public void expectHealthyStartup() throws IOException {
        File testFile = folder.newFile("empty-test-file.csv");

        MasterConfiguration masterConfiguration = new MasterConfiguration();
        masterConfiguration.setInputFile(testFile.toPath());
        TestEntityFactory entityFactory = new TestEntityFactory();

        LoggingTestKit.info("DBMaster started")
                      .expect(testKit.system(), () ->
                              testKit.spawn(ADBMasterSupervisor.create(masterConfiguration, entityFactory)));
    }

    @Test
    public void expectLogMessageForErrorResponse() throws IOException {
        File testFile = folder.newFile("empty-test-file-2.csv");

        MasterConfiguration masterConfiguration = new MasterConfiguration();
        masterConfiguration.setInputFile(testFile.toPath());
        TestEntityFactory entityFactory = new TestEntityFactory();
        ActorRef<ADBMasterSupervisor.Response> master = testKit.spawn(ADBMasterSupervisor.create(masterConfiguration,
                entityFactory));

        ADBMasterSupervisor.ErrorResponse errorResponse = new ADBMasterSupervisor.ErrorResponse("Test error");

        LoggingTestKit.error("Test error")
                      .expect(
                              testKit.system(), () -> {
                                  master.tell(errorResponse);
                                  return null;
                              });
    }

    @Test
    public void expectCommandToStartParsingAfterReceivedResponseReadyForParsing() throws IOException {
        File testFile = folder.newFile("empty-test-file-3.csv");
        TestProbe<CSVParsingActor.Command> probe = testKit.createTestProbe();

        MasterConfiguration masterConfiguration = new MasterConfiguration();
        masterConfiguration.setInputFile(testFile.toPath());
        TestEntityFactory entityFactory = new TestEntityFactory();
        ActorRef<ADBMasterSupervisor.Response> master = testKit.spawn(ADBMasterSupervisor.create(masterConfiguration,
                entityFactory));

        master.tell(CSVParsingActor.CSVReadyForParsing.builder().respondTo(probe.ref()).build());

        CSVParsingActor.ParseNextCSVChunk response = (CSVParsingActor.ParseNextCSVChunk) probe.receiveMessage();

        assertThat(response.getClass().getCanonicalName()).isEqualTo(CSVParsingActor.ParseNextCSVChunk.class.getCanonicalName());
        assertThat(response.getClient().path()).isEqualTo(master.path());
    }
}
