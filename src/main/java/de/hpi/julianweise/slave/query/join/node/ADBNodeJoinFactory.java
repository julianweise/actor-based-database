package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;

public class ADBNodeJoinFactory {

    public static Behavior<ADBNodeJoin.Command> createDefault(ADBJoinQueryContext joinQueryContext,
                                                              ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                                              ADBNodeJoinContext joinNodesContext) {
        return Behaviors.setup(context -> new ADBNodeJoin(context, joinQueryContext, supervisor, joinNodesContext));
    }

    public static String sessionName(ADBNodeJoinContext ctx) {
        return String.format("NodeJoin:transactionId:%s-executor:%s-left:%s-right:%s", ctx.getTransactionId(),
                ctx.getExecutorNodeId(), ctx.getLeftNodeId(), ctx.getRightNodeId());
    }
}