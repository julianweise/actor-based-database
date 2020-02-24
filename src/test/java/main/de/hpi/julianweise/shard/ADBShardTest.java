package main.de.hpi.julianweise.shard;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.ADBShardDistributor;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBShardTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void expectEntityIsPersistedCorrectly() {
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShard.create(), "shard");
        ADBEntityType entityToPersist = new TestEntity(1, "Test", 1.01f);

        TestProbe<ADBShardDistributor.Command> probe = testKit.createTestProbe();
        shard.tell(new ADBShard.PersistEntity(probe.ref(), entityToPersist));
        ADBShardDistributor.ConfirmEntityPersisted response = (ADBShardDistributor.ConfirmEntityPersisted)
                probe.receiveMessage();
        assertThat(response.getClass().getCanonicalName())
                .isEqualTo(ADBShardDistributor.ConfirmEntityPersisted.class.getCanonicalName());
        assertThat(response.getEntityPrimaryKey())
                .isEqualTo(entityToPersist.getPrimaryKey());
    }
}
