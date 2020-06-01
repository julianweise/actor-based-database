package de.hpi.julianweise.master;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcess;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcessFactory;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
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
        TestProbe<ADBDataDistributor.Command> distributorTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBDataDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBDataDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBDataDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.Start(respondToProbe.ref()));

        CSVParsingActor.Command csvCommand = parserTestProbe.receiveMessage();

        assertThat(csvCommand.getClass()).isEqualTo(CSVParsingActor.ParseNextCSVChunk.class);
    }

    @Test
    public void testProcessForwardsNewCSVDataChunkToDistributor() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBDataDistributor.Command> distributorTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBDataDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBDataDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBDataDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        ObjectList<ADBEntity> chunkList = new ObjectArrayList<>();
        chunkList.add(new TestEntity(1, "Test", 1f, true, 12.03234));
        CSVParsingActor.DomainDataChunk chunk = new CSVParsingActor.DomainDataChunk(chunkList);
        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedCSVParserResponse(chunk));

        ADBDataDistributor.Command distCommand = distributorTestProbe.receiveMessage();

        assertThat(distCommand.getClass()).isEqualTo(ADBDataDistributor.DistributeBatch.class);
        ADBDataDistributor.DistributeBatch distBatchCommand = (ADBDataDistributor.DistributeBatch) distCommand;
        assertThat(distBatchCommand.getEntities().size()).isOne();
        assertThat(distBatchCommand.getEntities().get(0)).isEqualTo(chunk.getChunk().get(0));
    }

    @Test
    public void testProcessConcludesDistributionAfterFinalizingCSVParsing() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBDataDistributor.Command> distributorTestProbe = testKit.createTestProbe();

        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBDataDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBDataDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBDataDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        CSVParsingActor.CSVFullyParsed parserNote = new CSVParsingActor.CSVFullyParsed();
        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedCSVParserResponse(parserNote));

        ADBDataDistributor.Command distCommand = distributorTestProbe.receiveMessage();

        assertThat(distCommand.getClass()).isEqualTo(ADBDataDistributor.ConcludeDistribution.class);
    }

    @Test
    public void testParserIsRequestedToParseNextChunkAfterBatchHasBeenDistributed() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBDataDistributor.Command> distributorTestProbe = testKit.createTestProbe();
        TestProbe<ADBMaster.Command> respondToProbe = testKit.createTestProbe();


        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBDataDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBDataDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBDataDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        ADBDataDistributor.BatchDistributed distResponse = new ADBDataDistributor.BatchDistributed();

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.Start(respondToProbe.ref()));

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedShardDistributorResponse(distResponse));

        CSVParsingActor.Command csvCommand = parserTestProbe.receiveMessage();

        assertThat(csvCommand.getClass()).isEqualTo(CSVParsingActor.ParseNextCSVChunk.class);
    }

    @Test
    public void testDistributorIsStoppedAfterSuccessfulDistribution() {
        TestProbe<CSVParsingActor.Command> parserTestProbe = testKit.createTestProbe();
        TestProbe<ADBDataDistributor.Command> distributorTestProbe = testKit.createTestProbe();
        TestProbe<ADBLoadAndDistributeDataProcess.Command> processUnderTestProbe = testKit.createTestProbe();
        TestProbe<ADBMaster.Command> respondToProbe = testKit.createTestProbe();


        Behavior<CSVParsingActor.Command> mockedParserBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<CSVParsingActor.Command> mockedParser =
                Behaviors.monitor(CSVParsingActor.Command.class, parserTestProbe.ref(), mockedParserBehavior);

        Behavior<ADBDataDistributor.Command> mockedDistributorBehavior =
                Behaviors.receiveMessage(message -> Behaviors.same());
        Behavior<ADBDataDistributor.Command> mockedDistributor =
                Behaviors.monitor(ADBDataDistributor.Command.class, distributorTestProbe.ref(), mockedDistributorBehavior);

        ActorRef<ADBLoadAndDistributeDataProcess.Command> processUnderTest =
                testKit.spawn(ADBLoadAndDistributeDataProcessFactory.createDefault(mockedParser, mockedDistributor));

        ADBDataDistributor.DataFullyDistributed distResponse = new ADBDataDistributor.DataFullyDistributed();

        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.Start(respondToProbe.ref()));


        processUnderTest.tell(new ADBLoadAndDistributeDataProcess.WrappedShardDistributorResponse(distResponse));

        distributorTestProbe.expectNoMessage();
        processUnderTestProbe.expectTerminated(processUnderTest);
    }


}