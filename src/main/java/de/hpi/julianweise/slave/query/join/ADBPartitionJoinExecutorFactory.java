package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.slave.query.join.node.ADBPartitionJoinTask;

import java.util.UUID;

public class ADBPartitionJoinExecutorFactory {

    public static Behavior<ADBPartitionJoinExecutor.Command> createDefault(ADBPartitionJoinTask joinTask) {
        return Behaviors.setup(ctx -> new ADBPartitionJoinExecutor(ctx, joinTask));
    }

    public static String name(ADBJoinQuery query) {
        return "ADBPartitionJoinExecutor-for-query-" + query.hashCode() + "-" + UUID.randomUUID().toString();
    }
}
