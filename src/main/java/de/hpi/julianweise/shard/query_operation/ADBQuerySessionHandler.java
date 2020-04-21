package de.hpi.julianweise.shard.query_operation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSenderFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ADBQuerySessionHandler extends AbstractBehavior<ADBQuerySessionHandler.Command> {

    protected final ActorRef<ADBQuerySession.Command> session;
    protected final ADBQuery query;
    protected final List<ADBEntityType> data;
    protected final ActorRef<ADBShard.Command> shard;
    protected final int globalShardId;
    protected final int transactionId;
    protected final ActorRef<ADBLargeMessageSender.Response> largeMessageSenderWrapping;
    protected final ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver;
    protected final AtomicInteger openTransferSessions = new AtomicInteger(0);

    public interface Command extends CborSerializable {
    }

    @AllArgsConstructor
    @Getter
    public static class WrappedLargeMessageSenderResponse implements Command {
        private ADBLargeMessageSender.Response response;
    }

    @NoArgsConstructor
    public static class Execute implements Command {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class Terminate implements Command {
        private int transactionId;
    }

    public ADBQuerySessionHandler(ActorContext<ADBQuerySessionHandler.Command> context,
                                  ActorRef<ADBShard.Command> shard,
                                  ActorRef<ADBQuerySession.Command> session,
                                  ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver,
                                  int transactionId, ADBQuery query,
                                  final List<ADBEntityType> data,
                                  int globalShardId) {
        super(context);
        ADBQueryPerformanceSampler.log(true, this.getClass().getSimpleName(), "Query Session");
        this.data = data;
        this.shard = shard;
        this.session = session;
        this.transactionId = transactionId;
        this.globalShardId = globalShardId;
        this.query = query;
        this.clientLargeMessageReceiver = clientLargeMessageReceiver;

        this.session.tell(new ADBQuerySession.RegisterQuerySessionHandler(this.shard, this.getContext().getSelf()));

        this.largeMessageSenderWrapping = context.messageAdapter(ADBLargeMessageSender.Response.class,
                WrappedLargeMessageSenderResponse::new);
        this.getContext().getLog().info(String.format("Started QuerySessionHandler for transaction %d to handle %s",
                this.transactionId, this.getQuerySessionName()));
    }

    protected ReceiveBuilder<ADBQuerySessionHandler.Command> createReceiveBuilder() {
        return newReceiveBuilder()
                .onMessage(WrappedLargeMessageSenderResponse.class, this::handleLargeMessageSenderResponse)
                .onMessage(Terminate.class, this::handleTerminate);
    }

    protected Behavior<ADBQuerySessionHandler.Command> handleLargeMessageSenderResponse(WrappedLargeMessageSenderResponse response) {
        return Behaviors.same();
    }

    private Behavior<Command> handleTerminate(Terminate command) {
        this.getContext().getLog().info("Going to shut down " + this.getQuerySessionName() + " Session for transaction #"
                + command.getTransactionId());
        ADBQueryPerformanceSampler.log(false, this.getClass().getSimpleName(), "Query Session");
        ADBQueryPerformanceSampler.concludeSampler(this.globalShardId, this.transactionId);
        return Behaviors.stopped();
    }

    protected void concludeTransaction() {
        this.session.tell(new ADBQuerySession.ConcludeTransaction(this.shard, transactionId));
        this.getContext().getLog().info(String.format("Concluding QuerySessionHandler for transaction %d handling %s",
                this.transactionId, this.getQuerySessionName()));
    }

    protected void sendToSession(ADBLargeMessageSender.LargeMessage message, int numberOfElements) {
        this.openTransferSessions.incrementAndGet();
        ActorRef<ADBLargeMessageSender.Command> receiver =
                this.getContext().spawn(ADBLargeMessageSenderFactory.createDefault(message,
                this.largeMessageSenderWrapping),
                ADBLargeMessageSenderFactory.senderName(this.getContext().getSelf(), this.clientLargeMessageReceiver,
                        message.getClass(), numberOfElements + ""));
        receiver.tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.clientLargeMessageReceiver),
                message.getClass()));
    }

    protected abstract String getQuerySessionName();
}
