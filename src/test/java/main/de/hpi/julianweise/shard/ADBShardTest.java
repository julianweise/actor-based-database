package main.de.hpi.julianweise.shard;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.ADBShardDistributor;
import de.hpi.julianweise.shard.ADBShardFactory;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.INEQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBShardTest {

    public static String config = "actor-db.csv.chunk-size = 1\n" +
            "actor-db.query-response-chunk-size = 1 \n" +
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
        TestProbe<ADBQuerySession.Command> querySessionProbe = testKit.createTestProbe();
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist));

        persistProbe.receiveMessage();

        TestProbe<ADBShardInquirer.Command> queryProbe = testKit.createTestProbe();

        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();
        query.addTerm(term);
        shard.tell(new ADBShard.QueryEntities(transactionId, querySessionProbe.ref(), query));

        ADBQuerySession.QueryResults results = (ADBQuerySession.QueryResults) querySessionProbe.receiveMessage();
        assertThat(results.getTransactionId()).isEqualTo(transactionId);
        assertThat(results.getResults().size()).isOne();
        assertThat(results.getResults().get(0)).isEqualTo(entityToPersist);

        ADBQuerySession.ConcludeTransaction transactionConclusion =
                (ADBQuerySession.ConcludeTransaction) querySessionProbe.receiveMessage();

        assertThat(transactionConclusion.getTransactionId()).isEqualTo(transactionId);

    }

    @Test
    public void expectQueryResultsGetSplitUpInChunks() {
        int transactionId = 2;
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShardFactory.createDefault(), "shard");
        ADBEntityType entityToPersist = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        ADBEntityType entityToPersist2 = new TestEntity(2, "Test2", 1.01f, true, 12.02, 'w');

        TestProbe<ADBShardDistributor.Command> persistProbe = testKit.createTestProbe();
        TestProbe<ADBQuerySession.Command> querySessionProbe = testKit.createTestProbe();
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist));
        persistProbe.receiveMessage();
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist2));
        persistProbe.receiveMessage();


        TestProbe<ADBShardInquirer.Command> queryProbe = testKit.createTestProbe();

        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(EQUALITY)
                .value(1.01f)
                .build();
        query.addTerm(term);
        shard.tell(new ADBShard.QueryEntities(transactionId, querySessionProbe.ref(), query));

        ADBQuerySession.QueryResults results = (ADBQuerySession.QueryResults) querySessionProbe.receiveMessage();
        assertThat(results.getTransactionId()).isEqualTo(transactionId);
        assertThat(results.getResults().size()).isOne();
        assertThat(results.getResults().get(0)).isEqualTo(entityToPersist);

        ADBQuerySession.QueryResults results2 = (ADBQuerySession.QueryResults) querySessionProbe.receiveMessage();
        assertThat(results2.getTransactionId()).isEqualTo(transactionId);
        assertThat(results2.getResults().size()).isOne();
        assertThat(results2.getResults().get(0)).isEqualTo(entityToPersist2);

        ADBQuerySession.ConcludeTransaction transactionConclusion =
                (ADBQuerySession.ConcludeTransaction) querySessionProbe.receiveMessage();

        assertThat(transactionConclusion.getTransactionId()).isEqualTo(transactionId);

    }

    @Test
    public void expectDataGetSortedAfterTransferConclusion() {
        int transactionId = 1;
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShardFactory.createDefault(), "shard");
        ADBEntityType entityToPersist = new TestEntity(2, "Test2", 1.01f, true, 12.02, 'w');
        ADBEntityType entityToPersist2 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');


        TestProbe<ADBShardDistributor.Command> persistProbe = testKit.createTestProbe();
        TestProbe<ADBQuerySession.Command> querySessionProbe = testKit.createTestProbe();
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist2));
        persistProbe.receiveMessage();
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist));
        persistProbe.receiveMessage();
        shard.tell(new ADBShard.ConcludeTransfer(1));

        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(INEQUALITY)
                .value(3)
                .build();
        query.addTerm(term);
        shard.tell(new ADBShard.QueryEntities(transactionId, querySessionProbe.ref(), query));

        ADBQuerySession.QueryResults results = (ADBQuerySession.QueryResults) querySessionProbe.receiveMessage();
        assertThat(results.getTransactionId()).isEqualTo(transactionId);
        assertThat(results.getResults().get(0).getPrimaryKey()).isEqualTo(new ADBIntegerKey(1));

        ADBQuerySession.QueryResults results2 = (ADBQuerySession.QueryResults) querySessionProbe.receiveMessage();
        assertThat(results2.getResults().get(0).getPrimaryKey()).isEqualTo(new ADBIntegerKey(2));
    }
}
