package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.join.ADBJoinQuery;

import java.util.UUID;

public class ADBPartitionJoinExecutorFactory {

    public static Behavior<ADBPartitionJoinExecutor.Command> createDefault(ADBJoinQuery query,
                                                                           ActorRef<ADBPartitionJoinExecutor.Response> respondTo) {
        return Behaviors.setup(ctx -> new ADBPartitionJoinExecutor(ctx, query, respondTo));
    }

    public static String name(ADBJoinQuery query) {
        return "ADBPartitionJoinExecutor-for-query-" + query.hashCode() + "-" + UUID.randomUUID().toString();
    }
}
