package de.hpi.julianweise.csv;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class CSVParsingActorTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();


    @AfterClass
    public static void cleanup() {
        testKit.after();
        folder.delete();
    }

    @Test
    public void testValidParsing() throws IOException {
        ADBEntityFactoryProvider adbEntityFactoryProvider = new ADBEntityFactoryProvider(new TestEntityFactory());

        final String testCSV = folder.newFile("test.csv").getAbsolutePath();
        String csvContent = "headerA,headerB,headerC\n200,TestString,1.02";
        Files.write(Paths.get(testCSV), csvContent.getBytes());

        Behavior<CSVParsingActor.Command> parserBehavior = CSVParsingActorFactory.createForFile(testCSV);
        ActorRef<CSVParsingActor.Command> parser = testKit.spawn(parserBehavior, "test-parser");


        TestProbe<CSVParsingActor.Response> probe = testKit.createTestProbe();

        parser.tell(new CSVParsingActor.ParseNextCSVChunk(probe.ref()));
        CSVParsingActor.DomainDataChunk chunk = probe.expectMessageClass(CSVParsingActor.DomainDataChunk.class);

        TestEntity results = (TestEntity) chunk.getChunk().get(0);

        assertThat(chunk.getChunk().size()).isEqualTo(1);

        assertThat(results.aInteger).isEqualTo(200);
        assertThat(results.bString).isEqualTo("TestString");
        assertThat(results.cFloat).isEqualTo(1.02f);

        testKit.stop(parser);
    }

    @Test
    public void testFullyParsedIsSentAtEndOfFile() throws IOException {
        final String testCSV = folder.newFile("test-empty.csv").getAbsolutePath();
        String csvContent = "";
        Files.write(Paths.get(testCSV), csvContent.getBytes());

        Behavior<CSVParsingActor.Command> parserBehavior = CSVParsingActorFactory.createForFile(testCSV);
        ActorRef<CSVParsingActor.Command> parser = testKit.spawn(parserBehavior, "test-parser");


        TestProbe<CSVParsingActor.Response> probe = testKit.createTestProbe();

        parser.tell(new CSVParsingActor.ParseNextCSVChunk(probe.ref()));
        CSVParsingActor.Response response = probe.receiveMessage();

        assertThat(response.getClass().getCanonicalName()).isEqualTo(CSVParsingActor.CSVFullyParsed.class.getCanonicalName());

        testKit.stop(parser);
    }
}
