package de.hpi.julianweise.slave.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ADBSlaveQuerySession extends AbstractBehavior<ADBSlaveQuerySession.Command> {

    protected final ActorRef<ADBMasterQuerySession.Command> session;
    protected final ADBQueryContext queryContext;
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
                                ADBQueryContext queryContext) {
        super(context);
        this.session = session;
        this.queryContext = queryContext;

        this.session.tell(new ADBMasterQuerySession.RegisterQuerySessionHandler(ADBPartitionManager.getInstance(),
                getContext().getSelf()));

        this.getContext().getLog().info("Started QuerySession " + queryContext.transactionId  + " for " + this.getQuerySessionName());
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
        this.getContext().getLog().debug("Asking master to conclude session TX#" + queryContext.transactionId + " handling " + getQuerySessionName());
    }

    protected void sendToSession(ADBLargeMessageSender.LargeMessage message) {
        this.openTransferSessions.incrementAndGet();
        val respondTo = getContext().messageAdapter(ADBLargeMessageSender.Response.class, MessageSenderResponse::new);
        ADBLargeMessageActor.sendMessage(this.getContext(), Adapter.toClassic(this.session), respondTo, message);
    }

    private Behavior<Command> handleTerminate(Terminate command) {
        this.getContext().getLog().info("Terminating " + this.getQuerySessionName());
        return Behaviors.stopped();
    }

    protected abstract String getQuerySessionName();
}
