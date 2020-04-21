package de.hpi.julianweise.shard.query_operation;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.select.ADBSelectQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.join.ADBSortedEntityAttributes;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.ADBJoinAttributeComparator;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBSelectQuerySessionHandlerTest {

    private final static int GLOBAL_SHARD_ID = 1;
    private static Map<String, ADBSortedEntityAttributes> ENTITY_ATTRIBUTES = new HashMap<>();


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
    public void returnEmptyResults() {
        int transactionId = 1;
        TestProbe<ADBQuerySession.Command> responseProbe = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardProbe = testKit.createTestProbe();
        TestProbe<ADBJoinAttributeComparator.Command> comparatorPool = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();
        query.addTerm(term);


        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(),
                initializeTransferTestProbe.ref(), query);

        Behavior<ADBQuerySessionHandler.Command> selectBehavior = ADBQuerySessionHandlerFactory.create(message,
                shardProbe.ref(), new ArrayList<>(), GLOBAL_SHARD_ID, ENTITY_ATTRIBUTES, comparatorPool.ref());


        ActorRef<ADBQuerySessionHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQuerySessionHandler.Execute());

        selectHandler.tell(new ADBQuerySessionHandler.WrappedLargeMessageSenderResponse(new ADBLargeMessageSender.TransferCompleted()));

        ADBQuerySession.RegisterQuerySessionHandler handlerRegistration =
                responseProbe.expectMessageClass(ADBQuerySession.RegisterQuerySessionHandler.class);

        ADBQuerySession.ConcludeTransaction conclusion =
                responseProbe.expectMessageClass(ADBQuerySession.ConcludeTransaction.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(selectHandler);
        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

    @Test
    public void returnValidResultSet() {
        int transactionId = 1;
        TestProbe<ADBQuerySession.Command> responseProbe = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardProbe = testKit.createTestProbe();
        TestProbe<ADBJoinAttributeComparator.Command> comparatorPool = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();


        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();
        query.addTerm(term);

        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(),
                initializeTransferTestProbe.ref(), query);

        List<ADBEntityType> dataset = new ArrayList<>();
        dataset.add(new TestEntity(1, "Test", 1f, true, 1.01, 'w'));
        dataset.add(new TestEntity(2, "Test", 1f, true, 1.01, 'w'));

        Behavior<ADBQuerySessionHandler.Command> selectBehavior = ADBQuerySessionHandlerFactory.create(message,
                shardProbe.ref(), dataset, GLOBAL_SHARD_ID, ENTITY_ATTRIBUTES, comparatorPool.ref());

        ActorRef<ADBQuerySessionHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQuerySessionHandler.Execute());

        ADBQuerySession.RegisterQuerySessionHandler handlerRegistration =
                responseProbe.expectMessageClass(ADBQuerySession.RegisterQuerySessionHandler.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(selectHandler);

        ADBLargeMessageReceiver.InitializeTransfer initializeTransfer =
                initializeTransferTestProbe.expectMessageClass(ADBLargeMessageReceiver.InitializeTransfer.class);

        assertThat(initializeTransfer.getType()).isEqualTo(ADBSelectQuerySession.SelectQueryResults.class);
        assertThat(initializeTransfer.getTotalSize()).isGreaterThan(0);

        selectHandler.tell(new ADBQuerySessionHandler.WrappedLargeMessageSenderResponse(new ADBLargeMessageSender.TransferCompleted()));

        ADBQuerySession.ConcludeTransaction conclusion = (ADBQuerySession.ConcludeTransaction) responseProbe
                .receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

}