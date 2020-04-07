package de.hpi.julianweise.query.session;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandler;
import de.hpi.julianweise.shard.query_operation.join.ADBJoinQuerySessionHandler;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiverFactory;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ADBQuerySession extends AbstractBehavior<ADBQuerySession.Command> {

    protected final int transactionId;
    protected final ActorRef<ADBShardInquirer.Command> parent;
    protected final List<ActorRef<ADBShard.Command>> shards;
    protected final Map<ActorRef<ADBShard.Command>, ActorRef<ADBQuerySessionHandler.Command>> sessionHandlers =
            new HashMap<>();
    protected final ActorRef<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferWrapper;
    protected final Set<ActorRef<ADBJoinQuerySessionHandler.Command>> completedSessions = new HashSet<>();
    protected final AtomicInteger expectedPartialResults = new AtomicInteger();


    public interface Command {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class RegisterQuerySessionHandler implements ADBQuerySession.Command, CborSerializable {
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

    public ADBQuerySession(ActorContext<Command> context, List<ActorRef<ADBShard.Command>> shards,
                           int transactionId, ActorRef<ADBShardInquirer.Command> parent) {
        super(context);
        this.shards = shards;
        this.transactionId = transactionId;
        this.parent = parent;
        this.getContext().getLog().info(String.format("Started new QuerySession %d for %s",
                this.transactionId, this.getQuerySessionName()));
        this.initializeTransferWrapper = this.getContext().messageAdapter(
                ADBLargeMessageReceiver.InitializeTransfer.class, InitializeTransferWrapper::new);

    }

    protected ReceiveBuilder<Command> createReceiveBuilder() {
        return newReceiveBuilder()
                .onMessage(RegisterQuerySessionHandler.class, this::handleRegisterQuerySessionHandler)
                .onMessage(InitializeTransferWrapper.class, this::handleInitializeTransfer)
                .onMessage(ConcludeTransaction.class, this::handleConcludeTransaction);
    }

    protected Behavior<ADBQuerySession.Command> handleRegisterQuerySessionHandler(RegisterQuerySessionHandler command) {
        this.sessionHandlers.put(command.shard, command.sessionHandler);
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

    protected Behavior<ADBQuerySession.Command> concludeSession() {
        this.getContext().getLog().info(String.format("Concluding QuerySession for transaction %d handling %s",
                this.transactionId, this.getQuerySessionName()));
        return Behaviors.stopped();
    }

    protected Behavior<ADBQuerySession.Command> handleConcludeTransaction(ConcludeTransaction command) {
        this.completedSessions.add(this.sessionHandlers.get(command.getShard()));
        return this.conditionallyConcludeTransaction();
    }

    protected Behavior<ADBQuerySession.Command> conditionallyConcludeTransaction() {
        if (this.shards.size() == this.completedSessions.size() && this.expectedPartialResults.get() < 1) {
            this.completedSessions.forEach(handler -> handler.tell(new ADBQuerySessionHandler.Terminate(this.transactionId)));
            this.submitResults();
            return this.concludeSession();
        }
        return Behaviors.same();
    }

    protected abstract String getQuerySessionName();
    protected abstract void submitResults();
}
