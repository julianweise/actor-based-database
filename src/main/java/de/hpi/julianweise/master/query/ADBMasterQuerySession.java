package de.hpi.julianweise.master.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiverFactory;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ADBMasterQuerySession extends AbstractBehavior<ADBMasterQuerySession.Command> {

    protected final int transactionId;
    protected final ActorRef<ADBPartitionInquirer.Command> parent;
    protected final List<ActorRef<ADBQueryManager.Command>> queryManagers;
    protected final Map<ActorRef<ADBQueryManager.Command>, ActorRef<ADBSlaveQuerySession.Command>> managerToHandlers;
    protected final Map<ActorRef<ADBSlaveQuerySession.Command>, ActorRef<ADBQueryManager.Command>> handlersToManager;
    protected final Set<ActorRef<ADBLargeMessageReceiver.Command>> openReceiverSessions;
    protected final long startTime;
    private final Set<ActorRef<ADBSlaveQuerySession.Command>> completedSessions = new ObjectArraySet<>();


    public interface Command {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class RegisterQuerySessionHandler implements ADBMasterQuerySession.Command, CborSerializable {
        private ActorRef<ADBQueryManager.Command> queryManager;
        private ActorRef<ADBSlaveQuerySession.Command> sessionHandler;
    }

    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    public static class ConcludeTransaction implements Command, CborSerializable {
        private ActorRef<ADBSlaveQuerySession.Command> slaveQuerySession;
    }

    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public abstract static class QueryResults implements Command, ADBLargeMessageSender.LargeMessage {
        private int transactionId;
        private int nodeId;
    }

    @AllArgsConstructor
    private static class ReceiverTerminated implements Command {
        ActorRef<ADBLargeMessageReceiver.Command> receiver;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class InitializeTransferWrapper implements Command, CborSerializable {
        private ADBLargeMessageReceiver.InitializeTransfer initializeTransfer;
    }

    public ADBMasterQuerySession(ActorContext<Command> context, List<ActorRef<ADBQueryManager.Command>> queryManagers,
                                 int transactionId, ActorRef<ADBPartitionInquirer.Command> parent) {
        super(context);
        this.startTime = System.nanoTime();
        this.queryManagers = queryManagers;
        this.transactionId = transactionId;
        this.parent = parent;
        this.managerToHandlers = new Object2ObjectHashMap<>();
        this.handlersToManager = new Object2ObjectHashMap<>();
        this.openReceiverSessions = new ObjectOpenHashSet<>(queryManagers.size());
        this.getContext().getLog().info("Started QuerySession " + transactionId  + " for " + this.getQuerySessionName());
    }

    protected ReceiveBuilder<Command> createReceiveBuilder() {
        return newReceiveBuilder()
                .onMessage(RegisterQuerySessionHandler.class, this::handleRegisterQuerySessionHandler)
                .onMessage(InitializeTransferWrapper.class, this::handleInitializeTransfer)
                .onMessage(ReceiverTerminated.class, this::handleReceiverTerminated)
                .onMessage(ConcludeTransaction.class, this::handleConcludeTransaction);
    }

    protected Behavior<ADBMasterQuerySession.Command> handleRegisterQuerySessionHandler(RegisterQuerySessionHandler command) {
        this.managerToHandlers.put(command.queryManager, command.sessionHandler);
        this.handlersToManager.put(command.sessionHandler, command.queryManager);
        return Behaviors.same();
    }

    protected Behavior<ADBMasterQuerySession.Command> handleInitializeTransfer(InitializeTransferWrapper wrapper) {
        ActorRef<ADBLargeMessageReceiver.Command> receiver = this.getContext().spawn(ADBLargeMessageReceiverFactory
                        .createDefault(this.getContext().classicActorContext().self(),
                wrapper.getInitializeTransfer().getType(), wrapper.getInitializeTransfer().getRespondTo()),
                ADBLargeMessageReceiverFactory.receiverName(this.getContext().getSelf(),
                        wrapper.getInitializeTransfer().getType()));
        this.openReceiverSessions.add(receiver);
        this.getContext().watchWith(receiver, new ReceiverTerminated(receiver));
        receiver.tell(wrapper.getInitializeTransfer());
        return Behaviors.same();
    }

    protected Behavior<ADBMasterQuerySession.Command> concludeSession() {
        this.completedSessions.forEach(session -> session.tell(new ADBSlaveQuerySession.Terminate()));
        this.getContext().getLog().info("Concluding QuerySession  TX#" + transactionId + " handling " + this.getQuerySessionName());
        this.getContext().getLog().info(String.format("[PERFORMANCE] Time: %s ms", this.calculateElapsedTime() * 1e-6));
        return Behaviors.stopped();
    }

    private long calculateElapsedTime() {
        return System.nanoTime() - this.startTime;
    }

    private Behavior<ADBMasterQuerySession.Command> handleConcludeTransaction(ConcludeTransaction command) {
        this.getContext().getLog().info(command.slaveQuerySession + " concludes session");
        this.completedSessions.add(command.getSlaveQuerySession());
        return this.conditionallyConcludeTransaction();
    }

    protected Behavior<ADBMasterQuerySession.Command> conditionallyConcludeTransaction() {
        if (this.queryManagers.size() == this.completedSessions.size() && this.openReceiverSessions.size() < 1) {
            this.submitResults();
            return this.concludeSession();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleReceiverTerminated(ReceiverTerminated command) {
        this.openReceiverSessions.remove(command.receiver);
        this.conditionallyConcludeTransaction();
        return Behaviors.same();
    }

    protected abstract String getQuerySessionName();
    protected abstract void submitResults();
}
