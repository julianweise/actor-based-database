package de.hpi.julianweise.slave.partition;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.slave.partition.column.pax.ADBColumn;
import de.hpi.julianweise.slave.partition.column.pax.ADBColumnFactory;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@AllArgsConstructor
public class ADBPartitionFactory {

    private final AtomicInteger partitionIDGenerator = new AtomicInteger(0);

    public Behavior<ADBPartition.Command> createDefault(ObjectList<ADBEntity> data) {
        assert data.size() > 0;
        assert data.size() < ADBPartition.MAX_ELEMENTS : "Maximum 2^20 - 1 elements allowed per partition";

        int newPartitionId = this.getNewPartitionId();

        Map<String, ADBColumn> columns = ADBColumnFactory.createDefault(data, newPartitionId);
        return Behaviors.setup(context -> new ADBPartition(context, newPartitionId, columns,
                ADBEntityFactoryProvider.getInstance().getTargetClass()));
    }

    private int getNewPartitionId() {
        return this.partitionIDGenerator.getAndIncrement();
    }

    public int getLastPartitionId() {
        return this.partitionIDGenerator.get();
    }

}
