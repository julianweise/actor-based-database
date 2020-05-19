package de.hpi.julianweise.query.session.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query.ADBMasterQuerySessionFactory;
import de.hpi.julianweise.master.query.join.ADBMasterJoinSession;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBJoinQuerySessionTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
    }

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @AfterClass
    public static void after() {
        testKit.after();
    }

    @Test
    public void expectCorrectRegistrationAtStartUp() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shard = testKit.createTestProbe();

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ObjectList<ActorRef<ADBQueryManager.Command>> shardList = new ObjectArrayList<>();
        shardList.add(shard.ref());
        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(shardList, query, 1,
                        supervisor.ref()));

        ADBQueryManager.QueryEntities queryEntities = shard.expectMessageClass(ADBQueryManager.QueryEntities.class);

        assertThat(queryEntities.getQuery()).isEqualTo(query);
        assertThat(queryEntities.getTransactionId()).isEqualTo(1);
        assertThat(queryEntities.getRespondTo()).isEqualTo(joinSession);
    }

    @Test
    public void expectNoShardToJoinWithIsSentForOnlyOneShard() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> queryManager = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler = testKit.createTestProbe();

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ObjectList<ActorRef<ADBQueryManager.Command>> shardList = new ObjectArrayList<>();
        shardList.add(queryManager.ref());
        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(shardList, query, 1,
                        supervisor.ref()));

        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(queryManager.ref(), joinSessionHandler.ref()));


        joinSession.tell(new ADBMasterJoinSession.RequestNextNodeToJoin(joinSessionHandler.ref()));

        ADBSlaveJoinSession.NoMoreShardsToJoinWith response =
                joinSessionHandler.expectMessageClass(ADBSlaveJoinSession.NoMoreShardsToJoinWith.class);

        assertThat(response.getTransactionId()).isEqualTo(1);
    }

    @Test
    public void expectCorrectNextJoinSuggestionForEachShard() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shard1 = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shard2 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler2 = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shard1.ref());
        shards.add(shard2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(shards, query, 1, supervisor.ref()));


        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(shard1.ref(), joinSessionHandler1.ref()));
        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(shard2.ref(), joinSessionHandler2.ref()));

        joinSession.tell(new ADBMasterJoinSession.RequestNextNodeToJoin(joinSessionHandler1.ref()));

        ADBSlaveJoinSession.JoinWithShard response1 = joinSessionHandler1
                .expectMessageClass(ADBSlaveJoinSession.JoinWithShard.class);

        joinSession.tell(new ADBMasterJoinSession.RequestNextNodeToJoin(joinSessionHandler2.ref()));

        ADBSlaveJoinSession.NoMoreShardsToJoinWith response2 = joinSessionHandler2
                .expectMessageClass(ADBSlaveJoinSession.NoMoreShardsToJoinWith.class);

        assertThat(response1.getCounterpart()).isEqualTo(joinSessionHandler2.ref());
        assertThat(response2.getTransactionId()).isEqualTo(1);
    }

    @Test
    public void expectCorrectNextJoinSuggestionForEachShardEvenAfterDelayedHandlerMapping() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shard1 = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shard2 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler2 = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shard1.ref());
        shards.add(shard2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(shards, query, 1, supervisor.ref()));

        joinSession.tell(new ADBMasterJoinSession.TriggerShardComparison(shard2.ref(), joinSessionHandler1.ref()));

        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(shard1.ref(), joinSessionHandler1.ref()));
        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(shard2.ref(), joinSessionHandler2.ref()));

        ADBSlaveJoinSession.JoinWithShard response1 = joinSessionHandler1
                .expectMessageClass(ADBSlaveJoinSession.JoinWithShard.class);

        assertThat(response1.getCounterpart()).isEqualTo(joinSessionHandler2.ref());
    }

    @Test
    public void expectJoinQueryResultsDeliveryAfterTransactionConclusion() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shard1 = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shard2 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler2 = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shard1.ref());
        shards.add(shard2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(shards, query, 1, supervisor.ref()));


        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(shard1.ref(), joinSessionHandler1.ref()));
        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(shard2.ref(), joinSessionHandler2.ref()));

        ObjectArrayList<ADBKeyPair> results = new ObjectArrayList<>();
        results.add(new ADBKeyPair(1, 2));

        joinSession.tell(ADBMasterJoinSession.JoinQueryResults.builder()
                                                              .transactionId(1)
                                                              .joinResults(results)
                                                              .nodeId(1)
                                                              .build());

        // Receive self-join results first to decrease partial result counter
        joinSession.tell(new ADBMasterJoinSession.JoinQueryResults(new ObjectArrayList<>()));

        joinSession.tell(new ADBMasterQuerySession.ConcludeTransaction(joinSessionHandler1.ref()));
        joinSession.tell(new ADBMasterQuerySession.ConcludeTransaction(joinSessionHandler2.ref()));

        ADBPartitionInquirer.TransactionResultChunk response3 = supervisor
                .expectMessageClass(ADBPartitionInquirer.TransactionResultChunk.class);

        assertThat(response3.getTransactionId()).isEqualTo(1);
        assertThat(response3.getResults().size()).isEqualTo(1);
        assertThat(response3.getResults().get(0)).isEqualTo(results.get(0));
    }

}