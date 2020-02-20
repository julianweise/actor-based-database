package main.de.hpi.julianweise.csv;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.master.DBMasterSupervisor;
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

    @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    public ActorRef<CSVParsingActor.Command> parser = testKit.spawn(CSVParsingActor.create(), "test-parser");

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
        TestProbe<DBMasterSupervisor.Response> probe = testKit.createTestProbe();
        parser.tell(new CSVParsingActor.ParseNextCSVChunk(probe.ref()));
        DBMasterSupervisor.ErrorResponse response = (DBMasterSupervisor.ErrorResponse) probe.receiveMessage();
        assertThat(response.getErrorMessage()).contains("Open CSV before parsing");
    }

    @Test
    public void testValidOpeningForParsing() throws IOException {
        final File testCSV = folder.newFile("test-valid-opening.csv");
        String csvContent = "headerA,headerB,headerC\nvalueA,valueB,valueC";
        Files.write(Paths.get(testCSV.getAbsolutePath()), csvContent.getBytes());

        TestProbe<DBMasterSupervisor.Response> probe = testKit.createTestProbe();
        parser.tell(new CSVParsingActor.OpenCSVForParsing(testCSV.getAbsolutePath(), probe.ref()));
        DBMasterSupervisor.Response response = probe.receiveMessage();
        assertThat(response.getClass().getCanonicalName())
                .isEqualTo(CSVParsingActor.CSVReadyForParsing.class.getCanonicalName());
    }

    @Test
    public void testInvalidFilePathDoesNotReturnResult() {
        TestProbe<DBMasterSupervisor.Response> probe = testKit.createTestProbe();
        parser.tell(new CSVParsingActor.OpenCSVForParsing("invalid Path", probe.ref()));
        DBMasterSupervisor.ErrorResponse response = (DBMasterSupervisor.ErrorResponse) probe.receiveMessage();
        assertThat(response.getErrorMessage()).contains("Invalid File");
    }

    @Test
    public void testValidParsing() throws IOException {
        final File testCSV = folder.newFile("test.csv");
        String csvContent = "headerA,headerB,headerC\nvalueA,valueB,valueC";
        Files.write(Paths.get(testCSV.getAbsolutePath()), csvContent.getBytes());

        TestProbe<DBMasterSupervisor.Response> probe = testKit.createTestProbe();
        parser.tell(new CSVParsingActor.OpenCSVForParsing(testCSV.getAbsolutePath(), probe.ref()));
        DBMasterSupervisor.Response response = probe.receiveMessage();
        assertThat(response.getClass().getCanonicalName())
                .isEqualTo(CSVParsingActor.CSVReadyForParsing.class.getCanonicalName());

        parser.tell(new CSVParsingActor.ParseNextCSVChunk(probe.ref()));
        CSVParsingActor.CSVChunk chunk = (CSVParsingActor.CSVChunk) probe.receiveMessage();

        assertThat(chunk.getChunk().size()).isEqualTo(1);

        assertThat(chunk.getChunk().get(0).getTuples().size()).isEqualTo(3);
        assertThat(chunk.getChunk().get(0).getTuples().get(0).getKey()).isEqualTo("headerA");
        assertThat(chunk.getChunk().get(0).getTuples().get(1).getKey()).isEqualTo("headerB");
        assertThat(chunk.getChunk().get(0).getTuples().get(2).getKey()).isEqualTo("headerC");

        assertThat(chunk.getChunk().get(0).getTuples().get(0).getValue()).isEqualTo("valueA");
        assertThat(chunk.getChunk().get(0).getTuples().get(1).getValue()).isEqualTo("valueB");
        assertThat(chunk.getChunk().get(0).getTuples().get(2).getValue()).isEqualTo("valueC");
    }

}
