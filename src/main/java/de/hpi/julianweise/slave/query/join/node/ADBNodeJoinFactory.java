package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;

import java.util.UUID;

public class ADBNodeJoinFactory {

    public static Behavior<ADBNodeJoin.Command> createDefault(ADBJoinQueryContext joinQueryContext,
                                                              ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                                              ADBNodeJoinContext joinNodesContext) {
        return Behaviors.setup(context -> new ADBNodeJoin(context, joinQueryContext, supervisor, joinNodesContext));
    }

    public static String sessionName(ADBNodeJoinContext ctx) {
        return ADBNodeJoinFactory.sessionName(ctx, "");
    }

    public static String sessionName(ADBNodeJoinContext ctx, String postfix) {
        return String.format("NodeJoin:transactionId:%s-executor:%s-left:%s-right:%s-%s-%s", ctx.getTransactionId(),
                ctx.getExecutorNodeId(), ctx.getLeftNodeId(), ctx.getRightNodeId(), UUID.randomUUID().toString(),
                postfix);
    }
}
