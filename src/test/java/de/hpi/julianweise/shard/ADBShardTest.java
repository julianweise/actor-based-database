package de.hpi.julianweise.shard;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.select.ADBSelectQuerySession;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBShardTest {

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
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), entityToPersist));

        persistProbe.receiveMessage();

        shard.tell(new ADBShard.ConcludeTransfer(0));

        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();
        query.addTerm(term);
        shard.tell(new ADBShard.QueryEntities(transactionId, querySessionProbe.ref(),
                initializeTransferTestProbe.ref(), query));

        querySessionProbe.receiveMessage();
        ADBLargeMessageReceiver.InitializeTransfer initializeTransfer =
                initializeTransferTestProbe.expectMessageClass(ADBLargeMessageReceiver.InitializeTransfer.class);

        assertThat(initializeTransfer.getType()).isEqualTo(ADBSelectQuerySession.SelectQueryResults.class);
    }
}
