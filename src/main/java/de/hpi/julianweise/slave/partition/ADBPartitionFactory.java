package de.hpi.julianweise.slave.partition;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.slave.partition.column.pax.ADBColumn;
import de.hpi.julianweise.slave.partition.column.pax.ADBColumnFactory;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBPartitionFactory {

    private static final AtomicInteger partitionIDGenerator = new AtomicInteger(0);

    public static Behavior<ADBPartition.Command> createDefault(ObjectList<ADBEntity> data, int partitionId) {
        assert data.size() > 0;
        assert data.size() < ADBPartition.MAX_ELEMENTS : "Maximum 2^20 - 1 elements allowed per partition";

        Map<String, ADBColumn> columns = ADBColumnFactory.createDefault(data, partitionId);
        return Behaviors.setup(context -> new ADBPartition(context, partitionId, columns,
                ADBEntityFactoryProvider.getInstance().getTargetClass()));
    }

    public static int getNewPartitionId() {
        return partitionIDGenerator.getAndIncrement();
    }

}
