package de.hpi.julianweise.slave.partition;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntity;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBPartitionFactory {

    private static final AtomicInteger partitionIDGenerator = new AtomicInteger(0);

    public static Behavior<ADBPartition.Command> createDefault(List<ADBEntity> data) {
        return Behaviors.setup(context -> new ADBPartition(context, data));
    }

    public static int getNewPartitionId() {
        return partitionIDGenerator.getAndIncrement();
    }

}
