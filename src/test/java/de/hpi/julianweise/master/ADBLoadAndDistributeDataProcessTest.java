package de.hpi.julianweise.master;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.shard.ADBShardDistributor;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBLoadAndDistributeDataProcessTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @AfterClass
    public static void cleanUp() {
        testKit.after();
    }

    @Test
    public void testParserIsActivatedOnStar() {
        TestProbe<ADBMasterSupervisor.Command> respondToProbe = testKit.createTestProbe();
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBShardDistributor.Command> distributorTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBShardDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBShardDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBShardDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.Start(respondToProbe.ref()));

        CSVParsingActor.Command csvCommand = parserTestProbe.receiveMessage();

        assertThat(csvCommand.getClass()).isEqualTo(CSVParsingActor.ParseNextCSVChunk.class);
    }

    @Test
    public void testProcessForwardsNewCSVDataChunkToDistributor() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBShardDistributor.Command> distributorTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBShardDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBShardDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBShardDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        CSVParsingActor.DomainDataChunk chunk =
                new CSVParsingActor.DomainDataChunk(Collections.singletonList(new TestEntity(1, "Test", 1f, true,
                        12.03234, 'w')));
        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedCSVParserResponse(chunk));

        ADBShardDistributor.Command distCommand = distributorTestProbe.receiveMessage();

        assertThat(distCommand.getClass()).isEqualTo(ADBShardDistributor.DistributeBatch.class);
        ADBShardDistributor.DistributeBatch distBatchCommand = (ADBShardDistributor.DistributeBatch) distCommand;
        assertThat(distBatchCommand.getEntities().size()).isOne();
        assertThat(distBatchCommand.getEntities().get(0)).isEqualTo(chunk.getChunk().get(0));
    }

    @Test
    public void testProcessConcludesDistributionAfterFinalizingCSVParsing() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBShardDistributor.Command> distributorTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBShardDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBShardDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBShardDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        CSVParsingActor.CSVFullyParsed parserNote = new CSVParsingActor.CSVFullyParsed();
        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedCSVParserResponse(parserNote));

        ADBShardDistributor.Command distCommand = distributorTestProbe.receiveMessage();

        assertThat(distCommand.getClass()).isEqualTo(ADBShardDistributor.ConcludeDistribution.class);
    }

    @Test
    public void testParserIsRequestedToParseNextChunkAfterBatchHasBeenDistributed() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBShardDistributor.Command> distributorTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBShardDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBShardDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBShardDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        ADBShardDistributor.BatchDistributed distResponse = new ADBShardDistributor.BatchDistributed();

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedShardDistributorResponse(distResponse));

        CSVParsingActor.Command csvCommand = parserTestProbe.receiveMessage();

        assertThat(csvCommand.getClass()).isEqualTo(CSVParsingActor.ParseNextCSVChunk.class);
    }

    @Test
    public void testDistributorIsStoppedAfterSuccessfulDistribution() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBShardDistributor.Command> distributorTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBShardDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBShardDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBShardDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        ADBShardDistributor.DataFullyDistributed distResponse = new ADBShardDistributor.DataFullyDistributed();

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedShardDistributorResponse(distResponse));

        distributorTestProbe.expectNoMessage();
    }


}