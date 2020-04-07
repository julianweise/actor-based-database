package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.join.ADBJoinQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandler;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandlerFactory;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBJoinQuerySessionHandlerTest {

    public static int TRANSACTION_ID = 1;
    public static int GLOBAL_SHARD_ID = 1;

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
    }

    @AfterClass
    public static void after() {
        testKit.after();
    }

    @Test
    public void expectRequestForNextShardComparisonAfterExecuteCommand() {
        TestProbe<ADBQuerySession.Command> querySession = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        List<ADBEntityType> localData = new ArrayList<>();
        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
                                                                    .query(joinQuery)
                                                                    .respondTo(querySession.ref())
                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
                                                                    .transactionId(TRANSACTION_ID)
                                                                    .build();

        ActorRef<ADBJoinQuerySessionHandler.Command> joinHandler = testKit.spawn(ADBQuerySessionHandlerFactory
                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID));

        joinHandler.tell(new ADBQuerySessionHandler.Execute());

        ADBQuerySession.RegisterQuerySessionHandler handlerRegistration =
                querySession.expectMessageClass(ADBQuerySession.RegisterQuerySessionHandler.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(joinHandler);
        assertThat(handlerRegistration.getShard()).isEqualTo(shard.ref());

        ADBJoinQuerySession.RequestNextShardComparison request =
                querySession.expectMessageClass(ADBJoinQuerySession.RequestNextShardComparison.class);

        assertThat(request.getRequestingShard()).isEqualTo(shard.ref());
        assertThat(request.getRespondTo()).isEqualTo(joinHandler);
    }

    @Test
    public void expectOtherShardBeingRequestedToOpenANewShardJoinSession() {
        TestProbe<ADBQuerySession.Command> querySession = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
        TestProbe<ADBQuerySessionHandler.Command> otherShardJoinHandler = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        List<ADBEntityType> localData = new ArrayList<>();
        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
                                                                    .query(joinQuery)
                                                                    .respondTo(querySession.ref())
                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
                                                                    .transactionId(TRANSACTION_ID)
                                                                    .build();

        ActorRef<ADBJoinQuerySessionHandler.Command> joinHandler = testKit.spawn(ADBQuerySessionHandlerFactory
                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID));

        joinHandler.tell(new ADBJoinQuerySessionHandler.JoinWithShard(otherShardJoinHandler.ref(), GLOBAL_SHARD_ID));

        ADBJoinQuerySessionHandler.OpenInterShardJoinSession request =
                otherShardJoinHandler.expectMessageClass(ADBJoinQuerySessionHandler.OpenInterShardJoinSession.class);

        assertThat(request.getInitiatingSession().path().parent()).isEqualTo(joinHandler.path());
        assertThat(request.getInitiatingShardId()).isEqualTo(GLOBAL_SHARD_ID);
    }

    @Test
    public void expectToRespondToANewOpenJoinShardSessionRequest() {
        TestProbe<ADBQuerySession.Command> querySession = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
        TestProbe<ADBJoinWithShardSession.Command> joinWithShardSession = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        List<ADBEntityType> localData = new ArrayList<>();
        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
                                                                    .query(joinQuery)
                                                                    .respondTo(querySession.ref())
                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
                                                                    .transactionId(TRANSACTION_ID)
                                                                    .build();

        ActorRef<ADBJoinQuerySessionHandler.Command> joinHandler = testKit.spawn(ADBQuerySessionHandlerFactory
                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID));

        joinHandler.tell(new ADBJoinQuerySessionHandler.OpenInterShardJoinSession(joinWithShardSession.ref(),
                GLOBAL_SHARD_ID));

        ADBJoinWithShardSession.RegisterHandler registration =
                joinWithShardSession.expectMessageClass(ADBJoinWithShardSession.RegisterHandler.class);

        assertThat(registration.getSessionHandler().path().parent()).isEqualTo(joinHandler.path());
    }


    @Test
    public void expectHandlerToRequestNextShardComparisonAndDeliverResults() {
        TestProbe<ADBQuerySession.Command> querySession = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        List<ADBEntityType> remoteData = new ArrayList<>();
        remoteData.add(new TestEntity(3, "Test", 1f, true, 1.1, 'a'));
        remoteData.add(new TestEntity(4, "Test", 1f, true, 1.1, 'a'));

        List<ADBEntityType> localData = new ArrayList<>();
        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
                                                                    .query(joinQuery)
                                                                    .respondTo(querySession.ref())
                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
                                                                    .transactionId(TRANSACTION_ID)
                                                                    .build();

        ActorRef<ADBJoinQuerySessionHandler.Command> joinHandler = testKit.spawn(ADBQuerySessionHandlerFactory
                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID));

        joinHandler.tell(new ADBJoinQuerySessionHandler.HandleJoinShardResults(Collections.singleton(
                new ADBPair<>(0, remoteData.get(0)))));

        ADBQuerySession.RegisterQuerySessionHandler handlerRegistration =
                querySession.expectMessageClass(ADBQuerySession.RegisterQuerySessionHandler.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(joinHandler);
        assertThat(handlerRegistration.getShard()).isEqualTo(shard.ref());

        ADBJoinQuerySession.RequestNextShardComparison request =
                querySession.expectMessageClass(ADBJoinQuerySession.RequestNextShardComparison.class);

        assertThat(request.getRequestingShard()).isEqualTo(shard.ref());
        assertThat(request.getRespondTo()).isEqualTo(joinHandler);

        ADBLargeMessageReceiver.InitializeTransfer initializeTransfer =
                initializeTransferTestProbe.expectMessageClass(ADBLargeMessageReceiver.InitializeTransfer.class);

        assertThat(initializeTransfer.getType()).isEqualTo(ADBJoinQuerySession.JoinQueryResults.class);
    }

    @Test
    public void expectConcludeTransactionForNoMoreShardsToJoinWith() {
        TestProbe<ADBQuerySession.Command> querySession = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        List<ADBEntityType> localData = new ArrayList<>();
        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
                                                                    .query(joinQuery)
                                                                    .respondTo(querySession.ref())
                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
                                                                    .transactionId(TRANSACTION_ID)
                                                                    .build();

        ActorRef<ADBJoinQuerySessionHandler.Command> joinHandler = testKit.spawn(ADBQuerySessionHandlerFactory
                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID));

        joinHandler.tell(new ADBJoinQuerySessionHandler.NoMoreShardsToJoinWith(TRANSACTION_ID));

        ADBQuerySession.RegisterQuerySessionHandler handlerRegistration =
                querySession.expectMessageClass(ADBQuerySession.RegisterQuerySessionHandler.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(joinHandler);
        assertThat(handlerRegistration.getShard()).isEqualTo(shard.ref());

        ADBQuerySession.ConcludeTransaction command =
                querySession.expectMessageClass(ADBQuerySession.ConcludeTransaction.class);

        assertThat(command.getTransactionId()).isEqualTo(TRANSACTION_ID);
        assertThat(command.getShard()).isEqualTo(shard.ref());
    }

    @Test
    public void expectJoinSessionHandlerToStopOnTerminateCommand() {
        TestProbe<ADBQuerySession.Command> querySession = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
        TestProbe<ADBQuerySessionHandler.Command> joinHandlerProbe = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        List<ADBEntityType> localData = new ArrayList<>();
        localData.add(new TestEntity(1, "Test", 1f, true, 1.1, 'a'));
        localData.add(new TestEntity(2, "Test", 1f, true, 1.1, 'a'));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ADBShard.QueryEntities queryCommand = ADBShard.QueryEntities.builder()
                                                                    .query(joinQuery)
                                                                    .respondTo(querySession.ref())
                                                                    .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
                                                                    .transactionId(TRANSACTION_ID)
                                                                    .build();

        ActorRef<ADBJoinQuerySessionHandler.Command> joinHandler = testKit.spawn(ADBQuerySessionHandlerFactory
                .createForJoinQuery(queryCommand, shard.ref(), localData, GLOBAL_SHARD_ID));

        joinHandler.tell(new ADBJoinQuerySessionHandler.Terminate(TRANSACTION_ID));

        joinHandlerProbe.expectTerminated(joinHandler);
    }

}