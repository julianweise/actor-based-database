package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Routers;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;

import java.util.Map;

public class ADBLocalCompareAttributeSessionFactory {

    private final static int COMPARATOR_POOL_SIZE = 8;

    public static ActorRef<ADBJoinAttributeComparator.Command> getComparatorPool(ActorContext<?> context) {
        return context.spawn(Routers.pool(COMPARATOR_POOL_SIZE, Behaviors.supervise(ADBJoinAttributeComparatorFactory
                .createDefault()).onFailure(SupervisorStrategy.restart())), "join-comparator-pool");
    }

    public static Behavior<ADBLocalCompareAttributesSession.Command> createDefault(
            ActorRef<ADBJoinAttributeComparator.Command> comparatorPool,
            Map<String, ADBSortedEntityAttributes> sortedLocalJoinAttributes,
            ActorRef<ADBLargeMessageActor.Command> respondTo) {
        return Behaviors.setup(context ->
                new ADBLocalCompareAttributesSession(context, comparatorPool, sortedLocalJoinAttributes, respondTo));
    }
}
