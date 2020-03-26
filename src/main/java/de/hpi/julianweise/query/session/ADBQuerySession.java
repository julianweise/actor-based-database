package de.hpi.julianweise.query.session;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandler;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ADBQuerySession extends AbstractBehavior<ADBQuerySession.Command> {

    protected final int transactionId;
    protected final ActorRef<ADBShardInquirer.Command> parent;
    protected final List<ActorRef<ADBShard.Command>> shards;
    protected final Map<ActorRef<ADBShard.Command>, ActorRef<ADBQuerySessionHandler.Command>> shardToSessionMapping =
            new HashMap<>();

    public interface Command extends CborSerializable {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class UpdateShardToHandlerMapping implements ADBQuerySession.Command {
        private ActorRef<ADBShard.Command> shard;
        private ActorRef<ADBQuerySessionHandler.Command> sessionHandler;

    }
    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    public static class ConcludeTransaction implements Command {
        private ActorRef<ADBShard.Command> shard;
        private int transactionId;

    }
    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public abstract static class QueryResults implements Command {
        private int transactionId;
        private int globalShardId;

    }

    public static ServiceKey<ADBQuerySession.Command> getServiceKeyFor(int transactionId) {
        return ServiceKey.create(ADBQuerySession.Command.class, "ADBQuerySession-" + transactionId);
    }

    public ADBQuerySession(ActorContext<Command> context, List<ActorRef<ADBShard.Command>> shards,
                           int transactionId, ActorRef<ADBShardInquirer.Command> parent) {
        super(context);
        this.shards = shards;
        this.transactionId = transactionId;
        this.parent = parent;
        context.getSystem().receptionist().tell(Receptionist.register(ADBQuerySession.getServiceKeyFor(transactionId),
                this.getContext().getSelf()));
        this.getContext().getLog().info(String.format("Started new QuerySession %d for %s",
                this.transactionId, this.getQuerySessionName()));

    }

    protected ReceiveBuilder<Command> createReceiveBuilder() {
        return newReceiveBuilder()
                .onMessage(UpdateShardToHandlerMapping.class, this::handleUpdateShardToHandlerMapping);
    }

    protected Behavior<ADBQuerySession.Command> handleUpdateShardToHandlerMapping(UpdateShardToHandlerMapping command) {
        this.shardToSessionMapping.put(command.shard, command.sessionHandler);
        return Behaviors.same();
    }


    protected Behavior<ADBQuerySession.Command> concludeTransaction() {
        this.getContext().getLog().info(String.format("Concluding QuerySession for transaction %d handling %s",
                this.transactionId, this.getQuerySessionName()));
        return Behaviors.stopped();
    }

    protected abstract String getQuerySessionName();
}
