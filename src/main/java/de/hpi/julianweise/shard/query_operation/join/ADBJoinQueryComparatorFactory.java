package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Routers;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.ADBJoinAttributeComparator;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.ADBJoinAttributeComparatorFactory;

import java.util.Map;

public class ADBJoinQueryComparatorFactory {

    private final static int COMPARATOR_POOL_SIZE = 8;

    public static Behavior<ADBJoinQueryComparator.Command> createDefault(ADBJoinQuery query,
                                                                         Map<String, ADBSortedEntityAttributes> localSortedAttributes,
                                                                         ActorRef<ADBJoinWithShardSession.Command> supervisor) {
        return Behaviors.setup(context -> new ADBJoinQueryComparator(context, query, localSortedAttributes,
                ADBJoinQueryComparatorFactory.getComparatorPool(context), supervisor));
    }

    public static ActorRef<ADBJoinAttributeComparator.Command> getComparatorPool(ActorContext<?> context) {
        return context.spawn(Routers.pool(COMPARATOR_POOL_SIZE, Behaviors.supervise(ADBJoinAttributeComparatorFactory
                .createDefault()).onFailure(SupervisorStrategy.restart())), "join-comparator-pool");
    }
}
