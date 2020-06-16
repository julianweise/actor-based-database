package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;

public class ADBJoinWithNodeSessionFactory {

    public static Behavior<ADBJoinWithNodeSession.Command> createDefault(ADBJoinQueryContext joinQueryContext,
                                                                         ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                                                         ADBJoinNodesContext joinNodesContext) {
        return Behaviors.setup(context -> new ADBJoinWithNodeSession(context, joinQueryContext, supervisor, joinNodesContext));
    }

    public static String sessionName(int transactionId, ADBJoinNodesContext context) {
        return "ADBJoinWithNodeSession-tx:" + transactionId + "-left:" + context.getLeftNodeId() + "-right:" +
                context.getRightNodeId();
    }
}
