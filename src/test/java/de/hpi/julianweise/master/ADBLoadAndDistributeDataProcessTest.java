package de.hpi.julianweise.master;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcess;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcessFactory;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBLoadAndDistributeDataProcessTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @AfterClass
    public static void cleanUp() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @Test
    public void testParserIsActivatedOnStar() {
        TestProbe<ADBMaster.Command> respondToProbe = testKit.createTestProbe();
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser));

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.Start(respondToProbe.ref()));

        CSVParsingActor.Command csvCommand = parserTestProbe.receiveMessage();

        assertThat(csvCommand.getClass()).isEqualTo(CSVParsingActor.ParseNextCSVChunk.class);
    }

    @Test
    public void testProcessConcludesDistributionAfterFinalizingCSVParsing() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser));

        CSVParsingActor.CSVFullyParsed parserNote = new CSVParsingActor.CSVFullyParsed();
        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedCSVParserResponse(parserNote));

    }

    @Test
    public void testParserIsRequestedToParseNextChunkAfterBatchHasBeenDistributed() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBMaster.Command> respondToProbe = testKit.createTestProbe();


        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);


        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser));

        ADBDataDistributor.BatchDistributed distResponse = new ADBDataDistributor.BatchDistributed();

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.Start(respondToProbe.ref()));

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedNodeDistributorResponse(distResponse));

        CSVParsingActor.Command csvCommand = parserTestProbe.receiveMessage();

        assertThat(csvCommand.getClass()).isEqualTo(CSVParsingActor.ParseNextCSVChunk.class);
    }

    @Test
    public void testDistributorIsStoppedAfterSuccessfulDistribution() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBMaster.Command> respondToProbe = testKit.createTestProbe();


        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser));

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.Start(respondToProbe.ref()));
        processUnderTest.tell(new ADBLoadAndDistributeDataProcess
                .WrappedCSVParserResponse(new CSVParsingActor.CSVFullyParsed()));
    }


}