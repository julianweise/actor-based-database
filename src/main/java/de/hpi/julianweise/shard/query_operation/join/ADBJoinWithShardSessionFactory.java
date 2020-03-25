package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBQuery;

import java.util.Map;

public class ADBJoinWithShardSessionFactory {

    public static Behavior<ADBJoinWithShardSession.Command> createDefault(
            ADBQuery query,
            Map<String, ADBSortedEntityAttributes> sortedJoinAttributes,
            ActorRef<ADBJoinQuerySessionHandler.Command> supervisor) {
        return Behaviors.setup(actorContext ->
                new ADBJoinWithShardSession(actorContext, query, sortedJoinAttributes, supervisor));
    }

    public static String sessionName(int transactionId, int localShardId, int targetShardId) {
        return "ADBJoinWithShardSession-for-@" + transactionId + "-running-on-" + localShardId + "-for-" + targetShardId;
    }
}
