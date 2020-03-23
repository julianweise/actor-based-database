package de.hpi.julianweise.shard.queryOperation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBQuery;

import java.util.List;
import java.util.Map;

public class ADBJoinWithShardSessionHandlerFactory {

    public static Behavior<ADBJoinWithShardSessionHandler.Command> createDefault(
            ActorRef<ADBJoinWithShardSession.Command> session,
            ADBQuery query,
            Map<String, ADBSortedEntityAttributes> sortedJoinAttributes,
            List<ADBEntityType> data) {
        return Behaviors.setup(actorContext -> new ADBJoinWithShardSessionHandler(actorContext, session, query,
                sortedJoinAttributes, data));
    }

    public static String sessionHandlerName(int transactionId, int localShardId, int targetShardId) {
        return  "ADBJoinWithShardSessionHandler-for-@" + transactionId + "-running-on-" + localShardId + "-for-" + targetShardId;
    }
}
