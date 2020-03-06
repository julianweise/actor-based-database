package de.hpi.julianweise.shard.queryOperation;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.shard.ADBShard;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.INEQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBQuerySelectHandlerTest {

    public static String config = "actor-db.csv.chunk-size = 1\n" +
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
        TestProbe<ADBShardInquirer.Command> responseProbe = testKit.createTestProbe();
        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();
        query.addTerm(term);


        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(), query);

        Behavior<ADBQueryOperationHandler.Command> selectBehavior = ADBQueryOperationHandlerFactory.create(message,
                new ArrayList<>());


        ActorRef<ADBQueryOperationHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQueryOperationHandler.Execute());

        ADBShardInquirer.ConcludeTransaction conclusion =
                (ADBShardInquirer.ConcludeTransaction) responseProbe.receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

    @Test
    public void returnValidResultSet() {
        int transactionId = 1;
        TestProbe<ADBShardInquirer.Command> responseProbe = testKit.createTestProbe();
        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();
        query.addTerm(term);

        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(), query);

        List<ADBEntityType> dataset = new ArrayList<>();
        dataset.add(new TestEntity(1, "Test", 1f, true, 1.01, 'w'));
        dataset.add(new TestEntity(2, "Test", 1f, true, 1.01, 'w'));

        Behavior<ADBQueryOperationHandler.Command> selectBehavior = ADBQueryOperationHandlerFactory.create(message,
                dataset);

        ActorRef<ADBQueryOperationHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQueryOperationHandler.Execute());

        ADBShardInquirer.QueryResults results = (ADBShardInquirer.QueryResults) responseProbe.receiveMessage();
        assertThat(results.getTransactionId()).isEqualTo(transactionId);
        assertThat(results.getResults().size()).isOne();
        assertThat(results.getResults().get(0)).isEqualTo(dataset.get(0));


        ADBShardInquirer.ConcludeTransaction conclusion =
                (ADBShardInquirer.ConcludeTransaction) responseProbe.receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

    @Test
    public void resultSetIsSplitIntoChunks() {
        int transactionId = 1;
        TestProbe<ADBShardInquirer.Command> responseProbe = testKit.createTestProbe();
        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(INEQUALITY)
                .value(3)
                .build();
        query.addTerm(term);

        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(), query);

        List<ADBEntityType> dataset = new ArrayList<>();
        dataset.add(new TestEntity(1, "Test", 1f, true, 1.01, 'w'));
        dataset.add(new TestEntity(2, "Test", 1f, true, 1.01, 'w'));

        Behavior<ADBQueryOperationHandler.Command> selectBehavior = ADBQueryOperationHandlerFactory.create(message,
                dataset);

        ActorRef<ADBQueryOperationHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQueryOperationHandler.Execute());

        ADBShardInquirer.QueryResults chunk1 = (ADBShardInquirer.QueryResults) responseProbe.receiveMessage();
        assertThat(chunk1.getTransactionId()).isEqualTo(transactionId);
        assertThat(chunk1.getResults().size()).isOne();
        assertThat(chunk1.getResults().get(0)).isEqualTo(dataset.get(0));

        ADBShardInquirer.QueryResults chunk2 = (ADBShardInquirer.QueryResults) responseProbe.receiveMessage();
        assertThat(chunk2.getTransactionId()).isEqualTo(transactionId);
        assertThat(chunk2.getResults().size()).isOne();
        assertThat(chunk2.getResults().get(0)).isEqualTo(dataset.get(1));


        ADBShardInquirer.ConcludeTransaction conclusion =
                (ADBShardInquirer.ConcludeTransaction) responseProbe.receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

}