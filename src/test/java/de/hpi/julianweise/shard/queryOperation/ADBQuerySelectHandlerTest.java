package de.hpi.julianweise.shard.queryOperation;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.shard.ADBShard;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
        ADBQuery query = new ADBQuery();
        query.addTerm(new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.EQUALITY));

        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(), query);

        Behavior<ADBQueryOperationHandler.Command> selectBehavior = ADBQueryOperationHandlerFactory.create(message,
                new HashMap<>());


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
        ADBQuery query = new ADBQuery();
        query.addTerm(new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.EQUALITY));

        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(), query);

        Map<ADBKey, ADBEntityType> dataset = new HashMap<>();
        dataset.put(new ADBIntegerKey(1), new TestEntity(1, "Test", 1f, true, 1.01, 'w'));
        dataset.put(new ADBIntegerKey(2), new TestEntity(2, "Test", 1f, true, 1.01, 'w'));

        Behavior<ADBQueryOperationHandler.Command> selectBehavior = ADBQueryOperationHandlerFactory.create(message,
                dataset);

        ActorRef<ADBQueryOperationHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQueryOperationHandler.Execute());

        ADBShardInquirer.QueryResults results = (ADBShardInquirer.QueryResults) responseProbe.receiveMessage();
        assertThat(results.getTransactionId()).isEqualTo(transactionId);
        assertThat(results.getResults().size()).isOne();
        assertThat(results.getResults().get(0)).isEqualTo(dataset.get(new ADBIntegerKey(1)));


        ADBShardInquirer.ConcludeTransaction conclusion =
                (ADBShardInquirer.ConcludeTransaction) responseProbe.receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

    @Test
    public void resultSetIsSplitIntoChunks() {
        int transactionId = 1;
        TestProbe<ADBShardInquirer.Command> responseProbe = testKit.createTestProbe();
        ADBQuery query = new ADBQuery();
        query.addTerm(new ADBQuery.ABDQueryTerm(3, "aInteger", ADBQuery.RelationalOperator.INEQUALITY));

        ADBShard.QueryEntities message = new ADBShard.QueryEntities(transactionId, responseProbe.ref(), query);

        Map<ADBKey, ADBEntityType> dataset = new HashMap<>();
        dataset.put(new ADBIntegerKey(1), new TestEntity(1, "Test", 1f, true, 1.01, 'w'));
        dataset.put(new ADBIntegerKey(2), new TestEntity(2, "Test", 1f, true, 1.01, 'w'));

        Behavior<ADBQueryOperationHandler.Command> selectBehavior = ADBQueryOperationHandlerFactory.create(message,
                dataset);

        ActorRef<ADBQueryOperationHandler.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBQueryOperationHandler.Execute());

        ADBShardInquirer.QueryResults chunk1 = (ADBShardInquirer.QueryResults) responseProbe.receiveMessage();
        assertThat(chunk1.getTransactionId()).isEqualTo(transactionId);
        assertThat(chunk1.getResults().size()).isOne();
        assertThat(chunk1.getResults().get(0)).isEqualTo(dataset.get(new ADBIntegerKey(1)));

        ADBShardInquirer.QueryResults chunk2 = (ADBShardInquirer.QueryResults) responseProbe.receiveMessage();
        assertThat(chunk2.getTransactionId()).isEqualTo(transactionId);
        assertThat(chunk2.getResults().size()).isOne();
        assertThat(chunk2.getResults().get(0)).isEqualTo(dataset.get(new ADBIntegerKey(2)));


        ADBShardInquirer.ConcludeTransaction conclusion =
                (ADBShardInquirer.ConcludeTransaction) responseProbe.receiveMessage();

        assertThat(conclusion.getTransactionId()).isEqualTo(transactionId);
    }

}