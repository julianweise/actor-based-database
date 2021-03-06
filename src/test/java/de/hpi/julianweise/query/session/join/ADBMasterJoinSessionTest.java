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
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.ADBPartialJoinResult;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.query.join.JoinExecutionPlan;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBMasterJoinSessionTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
        ADBComparator.buildComparatorMapping();
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
        TestProbe<ADBQueryManager.Command> queryManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partition = testKit.createTestProbe();

        ADBJoinQuery query = new ADBJoinQuery();
        query.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers = new ObjectArrayList<>();
        queryManagers.add(queryManager.ref());
        ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers = new ObjectArrayList<>();
        partitionManagers.add(partition.ref());
        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(queryManagers, partitionManagers, query, 1,
                        supervisor.ref()));

        ADBQueryManager.QueryEntities queryEntities = queryManager.expectMessageClass(ADBQueryManager.QueryEntities.class);

        assertThat(queryEntities.getQuery()).isEqualTo(query);
        assertThat(queryEntities.getTransactionId()).isEqualTo(1);
        assertThat(queryEntities.getRespondTo()).isEqualTo(joinSession);
    }

    @Test
    public void expectNoNodeToJoinWithIsSentForOnlyOneNode() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> queryManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partition = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler = testKit.createTestProbe();

        ADBJoinQuery query = new ADBJoinQuery();
        query.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers = new ObjectArrayList<>();
        queryManagers.add(queryManager.ref());
        ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers = new ObjectArrayList<>();
        partitionManagers.add(partition.ref());
        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(queryManagers, partitionManagers, query, 1,
                        supervisor.ref()));

        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(partition.ref(), joinSessionHandler.ref()));


        joinSession.tell(new ADBMasterJoinSession.RequestNextNodeToJoin(joinSessionHandler.ref()));

        ADBSlaveJoinSession.NoMoreNodesToJoinWith response =
                joinSessionHandler.expectMessageClass(ADBSlaveJoinSession.NoMoreNodesToJoinWith.class);

        assertThat(response.getTransactionId()).isEqualTo(1);
    }

    @Test
    public void expectCorrectNextJoinSuggestionForEachNode() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> queryManager1 = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> queryManager2 = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partitionManager1 = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partitionManager2 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler2 = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers = new ObjectArrayList<>();
        queryManagers.add(queryManager1.ref());
        queryManagers.add(queryManager2.ref());

        ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers = new ObjectArrayList<>();
        partitionManagers.add(partitionManager1.ref());
        partitionManagers.add(partitionManager2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(queryManagers, partitionManagers, query, 1,
                        supervisor.ref()));


        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(partitionManager1.ref(),
                joinSessionHandler1.ref()));
        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(partitionManager2.ref(),
                joinSessionHandler2.ref()));

        joinSession.tell(new ADBMasterJoinSession.RequestNextNodeToJoin(joinSessionHandler1.ref()));

        ADBSlaveJoinSession.JoinWithNode response1 = joinSessionHandler1
                .expectMessageClass(ADBSlaveJoinSession.JoinWithNode.class);

        joinSession.tell(new ADBMasterJoinSession.RequestNextNodeToJoin(joinSessionHandler2.ref()));

        ADBSlaveJoinSession.StealWorkFrom response2 = joinSessionHandler2
                .expectMessageClass(ADBSlaveJoinSession.StealWorkFrom.class);

        assertThat(response1.getContext().getRight()).isEqualTo(partitionManager2.ref());
        assertThat(response1.getContext().getLeft()).isEqualTo(partitionManager1.ref());
    }

    @Test
    public void expectCorrectNextJoinSuggestionForEachNodeEvenAfterDelayedHandlerMapping() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> queryManager1 = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> queryManager2 = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partitionManager1 = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partitionManager2 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler2 = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers = new ObjectArrayList<>();
        queryManagers.add(queryManager1.ref());
        queryManagers.add(queryManager2.ref());

        ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers = new ObjectArrayList<>();
        partitionManagers.add(partitionManager1.ref());
        partitionManagers.add(partitionManager2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(queryManagers, partitionManagers, query, 1,
                        supervisor.ref()));

        joinSession.tell(new ADBMasterJoinSession.JoinExecutionPlanWrapper(new JoinExecutionPlan
                .NextJoinNodePair(partitionManager1.ref(), partitionManager2.ref(), partitionManager1.ref(), true)));

        joinSession.tell(new ADBMasterQuerySession
                .RegisterQuerySessionHandler(partitionManager1.ref(), joinSessionHandler1.ref()));
        joinSession.tell(new ADBMasterQuerySession
                .RegisterQuerySessionHandler(partitionManager2.ref(), joinSessionHandler2.ref()));

        ADBSlaveJoinSession.JoinWithNode response1 = joinSessionHandler1
                .expectMessageClass(ADBSlaveJoinSession.JoinWithNode.class);

        assertThat(response1.getContext().getRight()).isEqualTo(partitionManager1.ref());
        assertThat(response1.getContext().getLeft()).isEqualTo(partitionManager2.ref());
    }

    @Test
    public void expectJoinQueryResultsDeliveryAfterTransactionConclusion() {

        TestProbe<ADBPartitionInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> queryManager1 = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> queryManager2 = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partitionManager1 = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partitionManager2 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBSlaveJoinSession.Command> joinSessionHandler2 = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers = new ObjectArrayList<>();
        queryManagers.add(queryManager1.ref());
        queryManagers.add(queryManager2.ref());

        ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers = new ObjectArrayList<>();
        partitionManagers.add(partitionManager1.ref());
        partitionManagers.add(partitionManager2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBMasterJoinSession.Command> joinSession =
                testKit.spawn(ADBMasterQuerySessionFactory.create(queryManagers, partitionManagers, query, 1,
                        supervisor.ref()));


        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(partitionManager1.ref(), joinSessionHandler1.ref()));
        joinSession.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(partitionManager2.ref(),
                joinSessionHandler2.ref()));

        ADBPartialJoinResult results = new ADBPartialJoinResult();
        results.addResult(1,2 );

        joinSession.tell(ADBMasterJoinSession.JoinQueryResults.builder()
                                                              .transactionId(1)
                                                              .joinResults(results)
                                                              .nodeId(1)
                                                              .build());

        // Receive self-join results first to decrease partial result counter
        joinSession.tell(new ADBMasterJoinSession.JoinQueryResults(new ADBPartialJoinResult()));

        joinSession.tell(new ADBMasterQuerySession.ConcludeTransaction(joinSessionHandler1.ref()));
        joinSession.tell(new ADBMasterQuerySession.ConcludeTransaction(joinSessionHandler2.ref()));

        ADBPartitionInquirer.TransactionResultChunk response3 = supervisor
                .expectMessageClass(ADBPartitionInquirer.TransactionResultChunk.class);

        assertThat(response3.getTransactionId()).isEqualTo(1);
        assertThat(response3.getSize()).isEqualTo(1);
        assertThat(response3.getResults().iterator().next()).isEqualTo(results.get(0));
    }

}