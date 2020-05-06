package de.hpi.julianweise.slave.query.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query.join.ADBMasterJoinSession;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySessionFactory;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBSlaveJoinSessionTest {

    public static final int TRANSACTION_ID = 1;

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
        testKit.spawn(ADBSlave.create());
    }

    @After
    public void cleanup() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
        testKit = new TestKitJunitResource();
    }

    @AfterClass
    public static void after() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @Test
    public void expectRequestForNextShardComparisonAfterExecuteCommand() {
        TestProbe<ADBMasterQuerySession.Command> querySession = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ADBQueryManager.QueryEntities queryCommand = ADBQueryManager.QueryEntities.builder()
                                                                           .query(joinQuery)
                                                                           .respondTo(querySession.ref())
                                                                           .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
                                                                           .transactionId(TRANSACTION_ID)
                                                                           .build();

        ActorRef<ADBSlaveJoinSession.Command> joinHandler = testKit.spawn(ADBSlaveQuerySessionFactory
                .createForJoinQuery(queryCommand));

        joinHandler.tell(new ADBSlaveQuerySession.Execute());

        ADBMasterQuerySession.RegisterQuerySessionHandler handlerRegistration =
                querySession.expectMessageClass(ADBMasterQuerySession.RegisterQuerySessionHandler.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(joinHandler);
        assertThat(handlerRegistration.getQueryManager()).isEqualTo(ADBQueryManager.getInstance());

        ADBMasterJoinSession.RequestNextNodeToJoin request =
                querySession.expectMessageClass(ADBMasterJoinSession.RequestNextNodeToJoin.class);

        assertThat(request.getRespondTo()).isEqualTo(joinHandler);
    }

//    @Test
//    public void expectOtherShardBeingRequestedToOpenANewShardJoinSession() {
//        TestProbe<ADBMasterQuerySession.Command> querySession = testKit.createTestProbe();
//        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
//        TestProbe<ADBJoinAttributeComparator.Command> comparatorPool = testKit.createTestProbe();
//
//        TestProbe<ADBSlaveQuerySession.Command> otherShardJoinHandler = testKit.createTestProbe();
//        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();
//
//        List<ADBEntity> localData = new ArrayList<>();
//        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
//        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));
//
//        Map<String, ADBSortedEntityAttributes> sortedEntityAttributes = ADBSortedEntityAttributes.of(localData);
//
//        ADBJoinQuery joinQuery = new ADBJoinQuery();
//        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));
//
//        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
//                                                                    .query(joinQuery)
//                                                                    .respondTo(querySession.ref())
//                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
//                                                                    .transactionId(TRANSACTION_ID)
//                                                                    .build();
//
//        ActorRef<ADBSlaveJoinSession.Command> joinHandler = testKit.spawn(ADBSlaveQuerySessionFactory
//                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID, sortedEntityAttributes, comparatorPool.ref()));
//
//        joinHandler.tell(new ADBSlaveJoinSession.JoinWithShard(otherShardJoinHandler.ref(), GLOBAL_SHARD_ID));
//
//        ADBSlaveJoinSession.OpenInterShardJoinSession request =
//                otherShardJoinHandler.expectMessageClass(ADBSlaveJoinSession.OpenInterShardJoinSession.class);
//
//        assertThat(request.getInitiatingSession().path().parent()).isEqualTo(joinHandler.path());
//        assertThat(request.getInitiatingShardId()).isEqualTo(GLOBAL_SHARD_ID);
//    }
//
//    @Test
//    public void expectToRespondToANewOpenJoinShardSessionRequest() {
//        TestProbe<ADBMasterQuerySession.Command> querySession = testKit.createTestProbe();
//        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
//        TestProbe<ADBJoinAttributeComparator.Command> comparatorPool = testKit.createTestProbe();
//        TestProbe<ADBJoinWithNodeSession.Command> joinWithShardSession = testKit.createTestProbe();
//        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();
//
//        List<ADBEntity> localData = new ArrayList<>();
//        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
//        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));
//
//        Map<String, ADBSortedEntityAttributes> sortedEntityAttributes = ADBSortedEntityAttributes.of(localData);
//
//        ADBJoinQuery joinQuery = new ADBJoinQuery();
//        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));
//
//        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
//                                                                    .query(joinQuery)
//                                                                    .respondTo(querySession.ref())
//                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
//                                                                    .transactionId(TRANSACTION_ID)
//                                                                    .build();
//
//        ActorRef<ADBSlaveJoinSession.Command> joinHandler = testKit.spawn(ADBSlaveQuerySessionFactory
//                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID, sortedEntityAttributes, comparatorPool.ref()));
//
//        joinHandler.tell(new ADBSlaveJoinSession.OpenInterShardJoinSession(joinWithShardSession.ref(),
//                GLOBAL_SHARD_ID));
//
//        ADBJoinWithNodeSession.RegisterHandler registration =
//                joinWithShardSession.expectMessageClass(ADBJoinWithNodeSession.RegisterHandler.class);
//
//        assertThat(registration.getSessionHandler().path().parent()).isEqualTo(joinHandler.path());
//    }
//
//
//    @Test
//    public void expectHandlerToRequestNextShardComparisonAndDeliverResults() {
//        TestProbe<ADBMasterQuerySession.Command> querySession = testKit.createTestProbe();
//        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
//        TestProbe<ADBJoinAttributeComparator.Command> comparatorPool = testKit.createTestProbe();
//
//        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();
//
//        List<ADBEntity> remoteData = new ArrayList<>();
//        remoteData.add(new TestEntity(3, "Test", 1f, true, 1.1, 'a'));
//        remoteData.add(new TestEntity(4, "Test", 1f, true, 1.1, 'a'));
//
//        List<ADBEntity> localData = new ArrayList<>();
//        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
//        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));
//
//        Map<String, ADBSortedEntityAttributes> sortedEntityAttributes = ADBSortedEntityAttributes.of(localData);
//
//        ADBJoinQuery joinQuery = new ADBJoinQuery();
//        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));
//
//        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
//                                                                    .query(joinQuery)
//                                                                    .respondTo(querySession.ref())
//                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
//                                                                    .transactionId(TRANSACTION_ID)
//                                                                    .build();
//
//        ActorRef<ADBSlaveJoinSession.Command> joinHandler = testKit.spawn(ADBSlaveQuerySessionFactory
//                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID, sortedEntityAttributes, comparatorPool.ref()));
//
//        joinHandler.tell(new ADBSlaveJoinSession.HandleJoinShardResults(Collections.singletonList(
//                new ADBPair<>(0, remoteData.get(0)))));
//
//        ADBMasterQuerySession.RegisterQuerySessionHandler handlerRegistration =
//                querySession.expectMessageClass(ADBMasterQuerySession.RegisterQuerySessionHandler.class);
//
//        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(joinHandler);
//        assertThat(handlerRegistration.getShard()).isEqualTo(shard.ref());
//
//        ADBMasterJoinSession.RequestNextShardComparison request =
//                querySession.expectMessageClass(ADBMasterJoinSession.RequestNextShardComparison.class);
//
//        assertThat(request.getRequestingQueryManager()).isEqualTo(shard.ref());
//        assertThat(request.getRespondTo()).isEqualTo(joinHandler);
//
//        ADBLargeMessageReceiver.InitializeTransfer initializeTransfer =
//                initializeTransferTestProbe.expectMessageClass(ADBLargeMessageReceiver.InitializeTransfer.class);
//
//        assertThat(initializeTransfer.getType()).isEqualTo(ADBMasterJoinSession.JoinQueryResults.class);
//    }
//
//    @Test
//    public void expectConcludeTransactionForNoMoreShardsToJoinWith() {
//        TestProbe<ADBMasterQuerySession.Command> querySession = testKit.createTestProbe();
//        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
//        TestProbe<ADBJoinAttributeComparator.Command> comparatorPool = testKit.createTestProbe();
//        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();
//
//        List<ADBEntity> localData = new ArrayList<>();
//        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
//        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));
//
//        Map<String, ADBSortedEntityAttributes> sortedEntityAttributes = ADBSortedEntityAttributes.of(localData);
//
//        ADBJoinQuery joinQuery = new ADBJoinQuery();
//        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));
//
//        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
//                                                                    .query(joinQuery)
//                                                                    .respondTo(querySession.ref())
//                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
//                                                                    .transactionId(TRANSACTION_ID)
//                                                                    .build();
//
//        ActorRef<ADBSlaveJoinSession.Command> joinHandler = testKit.spawn(ADBSlaveQuerySessionFactory
//                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID, sortedEntityAttributes, comparatorPool.ref()));
//
//        joinHandler.tell(new ADBSlaveJoinSession.NoMoreShardsToJoinWith(TRANSACTION_ID));
//
//        ADBMasterQuerySession.RegisterQuerySessionHandler handlerRegistration =
//                querySession.expectMessageClass(ADBMasterQuerySession.RegisterQuerySessionHandler.class);
//
//        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(joinHandler);
//        assertThat(handlerRegistration.getShard()).isEqualTo(shard.ref());
//
//        ADBMasterQuerySession.ConcludeTransaction command =
//                querySession.expectMessageClass(ADBMasterQuerySession.ConcludeTransaction.class);
//
//        assertThat(command.getTransactionId()).isEqualTo(TRANSACTION_ID);
//        assertThat(command.getShard()).isEqualTo(shard.ref());
//    }
//
//    @Test
//    public void expectJoinSessionHandlerToStopOnTerminateCommand() {
//        TestProbe<ADBMasterQuerySession.Command> querySession = testKit.createTestProbe();
//        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
//        TestProbe<ADBJoinAttributeComparator.Command> comparatorPool = testKit.createTestProbe();
//        TestProbe<ADBSlaveQuerySession.Command> joinHandlerProbe = testKit.createTestProbe();
//        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();
//
//        List<ADBEntity> localData = new ArrayList<>();
//        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
//        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));
//
//        Map<String, ADBSortedEntityAttributes> sortedEntityAttributes = ADBSortedEntityAttributes.of(localData);
//
//        ADBJoinQuery joinQuery = new ADBJoinQuery();
//        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));
//
//        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
//                                                                    .query(joinQuery)
//                                                                    .respondTo(querySession.ref())
//                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
//                                                                    .transactionId(TRANSACTION_ID)
//                                                                    .build();
//
//        ActorRef<ADBSlaveJoinSession.Command> joinHandler = testKit.spawn(ADBSlaveQuerySessionFactory
//                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID, sortedEntityAttributes,
//                        comparatorPool.ref()));
//
//        joinHandler.tell(new ADBSlaveJoinSession.Terminate(TRANSACTION_ID));
//
//        joinHandlerProbe.expectTerminated(joinHandler);
//    }

}