package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;

public class ADBJoinWithNodeSessionFactory {

    public static Behavior<ADBJoinWithNodeSession.Command> createDefault(ADBJoinQuery query,
                                                                         ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                                                         int remotePartitionId) {
        return Behaviors.setup(context -> new ADBJoinWithNodeSession(context, query, supervisor, remotePartitionId));
    }

    public static String sessionName(int transactionId, int targetShardId) {
        return "ADBJoinWithShardSession-tx:" + transactionId + "-local:" + ADBSlave.ID + "-remote:" + targetShardId;
    }
}
