package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.util.Map;

public class ADBPartitionJoinExecutorFactory {

    public static Behavior<ADBPartitionJoinExecutor.Command> createDefault(
            ADBJoinQuery query,
            ActorRef<ADBPartition.Command> lPartition,
            Map<String, ObjectList<ADBComparable2IntPair>> foreignAttributes,
            ActorRef<ADBPartitionJoinExecutor.PartitionsJoined> supervisor,
            boolean isReversed) {
        return Behaviors.setup(ctx ->
                new ADBPartitionJoinExecutor(ctx, query, lPartition, foreignAttributes, supervisor, isReversed));
    }

    public static String name(int lPartId, int fPartId, ADBJoinQuery query) {
        return "ADBPartitionJoinExecutor-" + lPartId + "-to-" + fPartId + "-for-query-" + query.hashCode();
    }
}
