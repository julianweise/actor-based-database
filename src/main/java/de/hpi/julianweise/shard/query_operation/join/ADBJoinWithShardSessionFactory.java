package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;

import java.util.Map;

public class ADBJoinWithShardSessionFactory {

    public static Behavior<ADBJoinWithShardSession.Command> createDefault(ADBJoinQuery query,
                                                                          Map<String, ADBSortedEntityAttributes> sortedJoinAttributes,
                                                                          ActorRef<ADBJoinQuerySessionHandler.Command> supervisor,
                                                                          int localShardId,
                                                                          int remoteShardId) {
        return Behaviors.setup(actorContext ->
                new ADBJoinWithShardSession(actorContext, query, sortedJoinAttributes, supervisor, localShardId,
                        remoteShardId));
    }

    public static String sessionName(int transactionId, int localShardId, int targetShardId) {
        return "ADBJoinWithShardSession-tx:" + transactionId + "-local:" + localShardId + "-remote:" + targetShardId;
    }
}
