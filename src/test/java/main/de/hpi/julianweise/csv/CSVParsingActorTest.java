package main.de.hpi.julianweise.csv;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.master.ADBMasterSupervisor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class CSVParsingActorTest {

    private static final int CHUNK_SIZE = 50;

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    public ActorRef<CSVParsingActor.Command> parser = testKit.spawn(CSVParsingActor.create(), "test-parser");
    private ADBEntityFactory testFactory = new TestEntityFactory();


    @AfterClass
    public static void cleanup() {
        testKit.after();
        folder.delete();
    }

    @After
    public void after() {
        testKit.stop(parser);
    }

    @Test
    public void testParsingBeforeCSVIsOpenFails() {
        TestProbe<ADBMasterSupervisor.Response> probe = testKit.createTestProbe();
        parser.tell(new CSVParsingActor.ParseNextCSVChunk(probe.ref()));
        ADBMasterSupervisor.ErrorResponse response = (ADBMasterSupervisor.ErrorResponse) probe.receiveMessage();
        assertThat(response.getErrorMessage()).contains("Open CSV before parsing");
    }

    @Test
    public void testValidOpeningForParsing() throws IOException {
        final File testCSV = folder.newFile("test-valid-opening.csv");
        String csvContent = "headerA,headerB,headerC\nvalueA,valueB,valueC";
        Files.write(Paths.get(testCSV.getAbsolutePath()), csvContent.getBytes());

        TestProbe<ADBMasterSupervisor.Response> probe = testKit.createTestProbe();
        parser.tell(new CSVParsingActor.OpenCSVForParsing(testCSV.getAbsolutePath(), probe.ref(), this.testFactory,
                CHUNK_SIZE));
        ADBMasterSupervisor.Response response = probe.receiveMessage();
        assertThat(response.getClass().getCanonicalName())
                .isEqualTo(CSVParsingActor.CSVReadyForParsing.class.getCanonicalName());
    }

    @Test
    public void testInvalidFilePathDoesNotReturnResult() {
        TestProbe<ADBMasterSupervisor.Response> probe = testKit.createTestProbe();
        parser.tell(new CSVParsingActor.OpenCSVForParsing("invalid Path", probe.ref(), this.testFactory,
                CHUNK_SIZE));
        ADBMasterSupervisor.ErrorResponse response = (ADBMasterSupervisor.ErrorResponse) probe.receiveMessage();
        assertThat(response.getErrorMessage()).contains("Invalid File");
    }

    @Test
    public void testValidParsing() throws IOException {
        final File testCSV = folder.newFile("test.csv");
        String csvContent = "headerA,headerB,headerC\n200,TestString,1.02";
        Files.write(Paths.get(testCSV.getAbsolutePath()), csvContent.getBytes());

        TestProbe<ADBMasterSupervisor.Response> probe = testKit.createTestProbe();
        parser.tell(new CSVParsingActor.OpenCSVForParsing(testCSV.getAbsolutePath(), probe.ref(), this.testFactory
                , CHUNK_SIZE));
        ADBMasterSupervisor.Response response = probe.receiveMessage();
        assertThat(response.getClass().getCanonicalName())
                .isEqualTo(CSVParsingActor.CSVReadyForParsing.class.getCanonicalName());

        parser.tell(new CSVParsingActor.ParseNextCSVChunk(probe.ref()));
        CSVParsingActor.DomainDataChunk chunk = (CSVParsingActor.DomainDataChunk) probe.receiveMessage();

        TestEntity results = (TestEntity) chunk.getChunk().get(0);

        assertThat(chunk.getChunk().size()).isEqualTo(1);

        assertThat(results.getAInteger()).isEqualTo(200);
        assertThat(results.getBString()).isEqualTo("TestString");
        assertThat(results.getCFloat()).isEqualTo(1.02f);

    }

}
