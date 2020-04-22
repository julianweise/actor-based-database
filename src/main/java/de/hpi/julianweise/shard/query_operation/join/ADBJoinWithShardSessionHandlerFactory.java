package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.ADBJoinAttributeComparator;

import java.util.List;
import java.util.Map;

public class ADBJoinWithShardSessionHandlerFactory {

    public static Behavior<ADBJoinWithShardSessionHandler.Command> createDefault(
            ActorRef<ADBJoinWithShardSession.Command> session,
            ADBQuery query,
            Map<String, ADBSortedEntityAttributes> sortedJoinAttributes,
            ActorRef<ADBJoinAttributeComparator.Command> comparatorPool,
            List<ADBEntity> data, int localShardId, int remoteShardId) {
        return Behaviors.setup(actorContext -> new ADBJoinWithShardSessionHandler(actorContext, session, query,
                sortedJoinAttributes, comparatorPool, data, localShardId, remoteShardId));
    }

    public static String sessionHandlerName(int transactionId, int localShardId, int targetShardId) {
        return  "ADBJoinWithShardSessionHandler-tx:" + transactionId + "-local:" + localShardId + "-remote:" + targetShardId;
    }
}
