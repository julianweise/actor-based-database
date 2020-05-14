package de.hpi.julianweise.slave.partition;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBPartitionFactory {

    private static final AtomicInteger partitionIDGenerator = new AtomicInteger(0);

    public static Behavior<ADBPartition.Command> createDefault(List<ADBEntity> data, int partitionId) {
        for(int i = 0; i < data.size(); i++) {
            data.get(i).setInternalID(ADBInternalIDHelper.createID(ADBSlave.ID, partitionId, i));
        }
        return Behaviors.setup(context -> new ADBPartition(context, partitionId, data));
    }

    public static int getNewPartitionId() {
        return partitionIDGenerator.getAndIncrement();
    }

}