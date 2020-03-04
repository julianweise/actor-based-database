package main.de.hpi.julianweise.shard;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.ADBShardDistributor;
import de.hpi.julianweise.shard.ADBShardFactory;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBShardTest {

    public static String config = "actor-db.csv.chunk-size = 1\n" +
            "actor-db.query-response-chunk-size = 5" +
            "actor-db.query-endpoint.hostname = localhost\n" +
            "actor-db.query-endpoint.port = 8080";

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource(ADBShardTest.config);

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
    public void expectEntityIsPersistedCorrectly() {
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShardFactory.createDefault(), "shard");
        ADBEntityType entityToPersist = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');

        TestProbe<ADBShardDistributor.Command> probe = testKit.createTestProbe();
        shard.tell(new ADBShard.PersistEntity(probe.ref(), entityToPersist));
        ADBShardDistributor.ConfirmEntityPersisted response = (ADBShardDistributor.ConfirmEntityPersisted)
                probe.receiveMessage();
        assertThat(response.getClass().getCanonicalName())
                .isEqualTo(ADBShardDistributor.ConfirmEntityPersisted.class.getCanonicalName());
        assertThat(response.getEntityPrimaryKey())
                .isEqualTo(entityToPersist.getPrimaryKey());
    }

    @Test
    public void expectQueryIsHandledCorrectly() {
        int transactionId = 1;
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShardFactory.createDefault(), "shard");
        ADBEntityType entityToPersist = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');

        TestProbe<ADBShardDistributor.Command> persistProbe = testKit.createTestProbe();
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist));

        persistProbe.receiveMessage();

        TestProbe<ADBShardInquirer.Command> queryProbe = testKit.createTestProbe();

        ADBQuery query = new ADBQuery();
        query.addTerm(new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.EQUALITY));
        shard.tell(new ADBShard.QueryEntities(transactionId, queryProbe.ref(), query));

        ADBShardInquirer.QueryResults results = (ADBShardInquirer.QueryResults) queryProbe.receiveMessage();
        assertThat(results.getTransactionId()).isEqualTo(transactionId);
        assertThat(results.getResults().size()).isOne();
        assertThat(results.getResults().get(0)).isEqualTo(entityToPersist);

        ADBShardInquirer.ConcludeTransaction transactionConclusion =
                (ADBShardInquirer.ConcludeTransaction) queryProbe.receiveMessage();

        assertThat(transactionConclusion.getTransactionId()).isEqualTo(transactionId);

    }

    @Test
    public void expectQueryResultsGetSplitUpInChunks() {
        int transactionId = 2;
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShardFactory.createDefault(), "shard");
        ADBEntityType entityToPersist = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        ADBEntityType entityToPersist2 = new TestEntity(2, "Test2", 1.01f, true, 12.02, 'w');

        TestProbe<ADBShardDistributor.Command> persistProbe = testKit.createTestProbe();
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist));
        persistProbe.receiveMessage();
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist2));
        persistProbe.receiveMessage();


        TestProbe<ADBShardInquirer.Command> queryProbe = testKit.createTestProbe();

        ADBQuery query = new ADBQuery();
        query.addTerm(new ADBQuery.ABDQueryTerm(1.01f, "cFloat", ADBQuery.RelationalOperator.EQUALITY));
        shard.tell(new ADBShard.QueryEntities(transactionId, queryProbe.ref(), query));

        ADBShardInquirer.QueryResults results = (ADBShardInquirer.QueryResults) queryProbe.receiveMessage();
        assertThat(results.getTransactionId()).isEqualTo(transactionId);
        assertThat(results.getResults().size()).isOne();
        assertThat(results.getResults().get(0)).isEqualTo(entityToPersist);

        ADBShardInquirer.QueryResults results2 = (ADBShardInquirer.QueryResults) queryProbe.receiveMessage();
        assertThat(results2.getTransactionId()).isEqualTo(transactionId);
        assertThat(results2.getResults().size()).isOne();
        assertThat(results2.getResults().get(0)).isEqualTo(entityToPersist2);

        ADBShardInquirer.ConcludeTransaction transactionConclusion =
                (ADBShardInquirer.ConcludeTransaction) queryProbe.receiveMessage();

        assertThat(transactionConclusion.getTransactionId()).isEqualTo(transactionId);

    }
}
