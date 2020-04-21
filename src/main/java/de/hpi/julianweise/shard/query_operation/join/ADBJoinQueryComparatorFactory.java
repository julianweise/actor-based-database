package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.ADBJoinAttributeComparator;

import java.util.Map;

public class ADBJoinQueryComparatorFactory {

    public static Behavior<ADBJoinQueryComparator.Command> createDefault(ADBJoinQuery query,
                                                                         Map<String, ADBSortedEntityAttributes> localSortedAttributes,
                                                                         ActorRef<ADBJoinWithShardSession.Command> supervisor,
                                                                         ActorRef<ADBJoinAttributeComparator.Command> comparatorPool) {
        return Behaviors.setup(context ->
                new ADBJoinQueryComparator(context, query, localSortedAttributes, comparatorPool, supervisor));
    }
}
