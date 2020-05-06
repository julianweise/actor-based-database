package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;

public class ADBJoinWithNodeSessionHandlerFactory {

    public static Behavior<ADBJoinWithNodeSessionHandler.Command> createDefault(
            ActorRef<ADBJoinWithNodeSession.Command> session, ADBQuery query, int remoteShardId,
            ActorRef<ADBSlaveQuerySession.Command> localJoinSessionHandler) {
        return Behaviors.setup(actorContext ->
                new ADBJoinWithNodeSessionHandler(actorContext, session, query, localJoinSessionHandler, remoteShardId));
    }

    public static String name(int transactionId, int targetShardId) {
        return  "ADBJoinWithNodeSessionHandler-tx:" + transactionId + "-local:" + ADBSlave.ID + "-remote:" + targetShardId;
    }
}
