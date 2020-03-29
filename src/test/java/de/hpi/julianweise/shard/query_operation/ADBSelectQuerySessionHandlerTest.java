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
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.INEQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBSelectQuerySessionHandlerTest {

    private final static int GLOBAL_SHARD_ID = 1;

    public final static String config =
            "actor-db.csv.chunk-size = 1\n" +
            "actor-db.query-response-chunk-size = 1 \n" +
            "actor-db.query-endpoint.hostname = localhost\n" +
            "actor-db.query-endpoint.port = 8080";

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource(config);


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
                shardProbe.ref(), new ArrayList<>(), GLOBAL_SHARD_ID);


        ActorRef<ADBQuerySessionHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQuerySessionHandler.Execute());

        ADBQuerySession.ConcludeTransaction conclusion = (ADBQuerySession.ConcludeTransaction) responseProbe
                .receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

    @Test
    public void returnValidResultSet() {
        int transactionId = 1;
        TestProbe<ADBQuerySession.Command> responseProbe = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardProbe = testKit.createTestProbe();
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
                shardProbe.ref(), dataset, GLOBAL_SHARD_ID);

        ActorRef<ADBQuerySessionHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQuerySessionHandler.Execute());

        ADBSelectQuerySession.SelectQueryResults results = (ADBSelectQuerySession.SelectQueryResults) responseProbe.receiveMessage();
        assertThat(results.getResults().size()).isOne();
        assertThat(results.getResults().get(0)).isEqualTo(dataset.get(0));


        ADBQuerySession.ConcludeTransaction conclusion = (ADBQuerySession.ConcludeTransaction) responseProbe
                .receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

    @Test
    public void resultSetIsSplitIntoChunks() {
        int transactionId = 1;
        TestProbe<ADBQuerySession.Command> responseProbe = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardProbe = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();


        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(INEQUALITY)
                .value(3)
                .build();
        query.addTerm(term);

        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(),
                initializeTransferTestProbe.ref(), query);

        List<ADBEntityType> dataset = new ArrayList<>();
        dataset.add(new TestEntity(1, "Test", 1f, true, 1.01, 'w'));
        dataset.add(new TestEntity(2, "Test", 1f, true, 1.01, 'w'));

        Behavior<ADBQuerySessionHandler.Command> selectBehavior = ADBQuerySessionHandlerFactory.create(message,
                shardProbe.ref(), dataset, GLOBAL_SHARD_ID);

        ActorRef<ADBQuerySessionHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQuerySessionHandler.Execute());

        ADBSelectQuerySession.SelectQueryResults chunk1 = (ADBSelectQuerySession.SelectQueryResults) responseProbe.receiveMessage();
        assertThat(chunk1.getResults().size()).isOne();
        assertThat(chunk1.getResults().get(0)).isEqualTo(dataset.get(0));

        ADBSelectQuerySession.SelectQueryResults chunk2 =
                (ADBSelectQuerySession.SelectQueryResults) responseProbe.receiveMessage();
        assertThat(chunk2.getResults().size()).isOne();
        assertThat(chunk2.getResults().get(0)).isEqualTo(dataset.get(1));


        ADBQuerySession.ConcludeTransaction conclusion =
                (ADBQuerySession.ConcludeTransaction) responseProbe.receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

}