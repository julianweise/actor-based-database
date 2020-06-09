package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;

public class ADBJoinWithNodeSessionFactory {

    public static Behavior<ADBJoinWithNodeSession.Command> createDefault(ADBJoinQueryContext joinQueryContext,
                                                                         ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                                                         int remoteNodeId) {
        return Behaviors.setup(context -> new ADBJoinWithNodeSession(context, joinQueryContext, supervisor, remoteNodeId));
    }

    public static String sessionName(int transactionId, int remoteNodeId) {
        return "ADBJoinWithShardSession-tx:" + transactionId + "-local:" + ADBSlave.ID + "-remote:" + remoteNodeId;
    }
}
