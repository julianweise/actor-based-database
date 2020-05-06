package de.hpi.julianweise.slave.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSenderFactory;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ADBSlaveQuerySession extends AbstractBehavior<ADBSlaveQuerySession.Command> {

    protected final ActorRef<ADBMasterQuerySession.Command> session;
    protected final ADBQuery query;
    protected final int transactionId;
    protected final ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientResultReceiver;
    protected final AtomicInteger openTransferSessions = new AtomicInteger(0);

    public interface Command {
    }

    @AllArgsConstructor
    @Getter
    public static class MessageSenderResponse implements Command, CborSerializable {
        private final ADBLargeMessageSender.Response response;
    }

    @NoArgsConstructor
    public static class Execute implements Command {}

    @NoArgsConstructor
    public static class Terminate implements Command, KryoSerializable {}

    public ADBSlaveQuerySession(ActorContext<ADBSlaveQuerySession.Command> context,
                                ActorRef<ADBMasterQuerySession.Command> session,
                                ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientResultReceiver,
                                int transactionId, ADBQuery query) {
        super(context);
        ADBQueryPerformanceSampler.log(true, this.getClass().getSimpleName(), "Query Session");
        this.session = session;
        this.transactionId = transactionId;
        this.query = query;
        this.clientResultReceiver = clientResultReceiver;

        this.session.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(ADBQueryManager.getInstance(),
                getContext().getSelf()));

        this.getContext().getLog().info("Started QuerySession " + transactionId  + " for " + this.getQuerySessionName());

    }

    protected ReceiveBuilder<ADBSlaveQuerySession.Command> createReceiveBuilder() {
        return newReceiveBuilder()
                .onMessage(MessageSenderResponse.class, this::handleLargeMessageSenderResponse)
                .onMessage(Terminate.class, this::handleTerminate);
    }

    protected Behavior<ADBSlaveQuerySession.Command> handleLargeMessageSenderResponse(MessageSenderResponse response) {
        this.openTransferSessions.decrementAndGet();
        return Behaviors.same();
    }

    protected void concludeTransaction() {
        this.session.tell(new ADBMasterQuerySession.ConcludeTransaction(this.getContext().getSelf()));
        this.getContext().getLog().info("Asking master to conclude session TX#" + transactionId + " handling " + getQuerySessionName());
        ADBQueryPerformanceSampler.log(false, this.getClass().getSimpleName(), "Query Session");
        ADBQueryPerformanceSampler.concludeSampler(ADBSlave.ID, this.transactionId);
    }

    protected void sendToSession(ADBLargeMessageSender.LargeMessage message) {
        this.openTransferSessions.incrementAndGet();
        String receiverName = ADBLargeMessageSenderFactory.name(this.getContext().getSelf(),
                this.clientResultReceiver, message.getClass(), "TX-" + this.transactionId + "-results");
        val respondTo = getContext().messageAdapter(ADBLargeMessageSender.Response.class, MessageSenderResponse::new);
        val receiver = this.getContext().spawn(ADBLargeMessageSenderFactory
                .createDefault(message, respondTo), receiverName);
        receiver.tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.clientResultReceiver),
                message.getClass()));
    }

    private Behavior<Command> handleTerminate(Terminate command) {
        this.getContext().getLog().info("Terminating " + this.getQuerySessionName());
        return Behaviors.stopped();
    }

    protected abstract String getQuerySessionName();
}
