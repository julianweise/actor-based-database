package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.List;
import java.util.Map;

public class ADBPartitionJoinExecutorFactory {

    public static Behavior<ADBPartitionJoinExecutor.Command> createDefault(
            ADBJoinQuery query,
            ActorRef<ADBPartition.Command> lPartition,
            Map<String, List<ADBPair<Comparable<Object>, Integer>>> foreignAttributes,
            int lPartitionId,
            int fPartitionId,
            ActorRef<ADBPartitionJoinExecutor.PartitionsJoined> supervisor,
            boolean isReversed) {
        return Behaviors.setup(ctx -> new ADBPartitionJoinExecutor(ctx, query, lPartition, foreignAttributes,
                lPartitionId, fPartitionId, supervisor, isReversed));
    }

    public static String name(int lPartId, int fPartId, ADBJoinQuery query) {
        return "ADBPartitionJoinExecutor-" + lPartId + "-to-" + fPartId + "-for-query-" + query.hashCode();
    }
}
