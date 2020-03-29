package de.hpi.julianweise.query.session;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandler;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiverFactory;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
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
    protected final ActorRef<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferWrapper;

    public interface Command {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class UpdateShardToHandlerMapping implements ADBQuerySession.Command, CborSerializable {
        private ActorRef<ADBShard.Command> shard;
        private ActorRef<ADBQuerySessionHandler.Command> sessionHandler;

    }
    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    public static class ConcludeTransaction implements Command, CborSerializable {
        private ActorRef<ADBShard.Command> shard;
        private int transactionId;

    }
    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public abstract static class QueryResults implements Command, ADBLargeMessageSender.LargeMessage {
        private int transactionId;
        private int globalShardId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class InitializeTransferWrapper implements Command, CborSerializable {
        private ADBLargeMessageReceiver.InitializeTransfer initializeTransfer;
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
        this.initializeTransferWrapper = this.getContext().messageAdapter(
                ADBLargeMessageReceiver.InitializeTransfer.class, InitializeTransferWrapper::new);

    }

    protected ReceiveBuilder<Command> createReceiveBuilder() {
        return newReceiveBuilder()
                .onMessage(UpdateShardToHandlerMapping.class, this::handleUpdateShardToHandlerMapping)
                .onMessage(InitializeTransferWrapper.class, this::handleInitializeTransfer);
    }

    protected Behavior<ADBQuerySession.Command> handleUpdateShardToHandlerMapping(UpdateShardToHandlerMapping command) {
        this.shardToSessionMapping.put(command.shard, command.sessionHandler);
        return Behaviors.same();
    }

    protected Behavior<ADBQuerySession.Command> handleInitializeTransfer(InitializeTransferWrapper wrapper) {
        this.getContext().spawn(ADBLargeMessageReceiverFactory.createDefault(this.getContext().classicActorContext().self(),
                wrapper.getInitializeTransfer().getType(), wrapper.getInitializeTransfer().getRespondTo()),
                ADBLargeMessageReceiverFactory.receiverName(this.getContext().getSelf(),
                        wrapper.getInitializeTransfer().getType()))
            .tell(wrapper.getInitializeTransfer());
        return Behaviors.same();
    }

    protected Behavior<ADBQuerySession.Command> concludeTransaction() {
        this.getContext().getLog().info(String.format("Concluding QuerySession for transaction %d handling %s",
                this.transactionId, this.getQuerySessionName()));
        return Behaviors.stopped();
    }

    protected abstract String getQuerySessionName();
}
