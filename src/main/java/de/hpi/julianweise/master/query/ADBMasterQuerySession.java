package de.hpi.julianweise.master.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;
import java.util.Set;

public abstract class ADBMasterQuerySession extends ADBLargeMessageActor {

    protected final int transactionId;
    protected final ActorRef<ADBPartitionInquirer.Command> parent;
    protected final ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers;
    protected final ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers;
    protected final Map<ActorRef<ADBPartitionManager.Command>, ActorRef<ADBSlaveQuerySession.Command>> managerToHandlers;
    protected final Map<ActorRef<ADBSlaveQuerySession.Command>, ActorRef<ADBPartitionManager.Command>> handlersToManager;
    protected final Set<ActorRef<ADBSlaveQuerySession.Command>> completedSessions = new ObjectArraySet<>();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class RegisterQuerySessionHandler implements ADBMasterQuerySession.Command, CborSerializable {
        private ActorRef<ADBPartitionManager.Command> partitionManager;
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

    public ADBMasterQuerySession(ActorContext<Command> context,
                                 ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers,
                                 ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers,
                                 int transactionId, ActorRef<ADBPartitionInquirer.Command> parent) {
        super(context);
        this.queryManagers = queryManagers;
        this.partitionManagers = partitionManagers;
        this.transactionId = transactionId;
        this.parent = parent;
        this.managerToHandlers = new Object2ObjectOpenHashMap<>();
        this.handlersToManager = new Object2ObjectOpenHashMap<>();
        this.getContext().getLog().info("Started QuerySession " + transactionId  + " for " + this.getQuerySessionName());
    }

    protected ReceiveBuilder<Command> createReceiveBuilder() {
        return newReceiveBuilder()
                .onMessage(RegisterQuerySessionHandler.class, this::handleRegisterQuerySessionHandler)
                .onMessage(ConcludeTransaction.class, this::handleConcludeTransaction);
    }

    protected Behavior<ADBMasterQuerySession.Command> handleRegisterQuerySessionHandler(RegisterQuerySessionHandler command) {
        this.managerToHandlers.put(command.partitionManager, command.sessionHandler);
        this.handlersToManager.put(command.sessionHandler, command.partitionManager);
        return Behaviors.same();
    }

    protected Behavior<ADBMasterQuerySession.Command> concludeSession() {
        this.completedSessions.forEach(session -> session.tell(new ADBSlaveQuerySession.Terminate()));
        this.getContext().getLog().info("Concluding QuerySession  TX#" + transactionId + " handling " + this.getQuerySessionName());
        return Behaviors.stopped();
    }

    protected Behavior<ADBMasterQuerySession.Command> handleConcludeTransaction(ConcludeTransaction command) {
        this.getContext().getLog().info(command.slaveQuerySession + " concludes session");
        this.completedSessions.add(command.getSlaveQuerySession());
        return this.conditionallyConcludeTransaction();
    }

    protected Behavior<ADBMasterQuerySession.Command> conditionallyConcludeTransaction() {
        if (this.queryManagers.size() == this.completedSessions.size() && this.openReceiverSessions.size() < 1 && this.isFinalized()) {
            this.submitResults();
            return this.concludeSession();
        }
        return Behaviors.same();
    }

    protected void handleReceiverTerminated() {
        this.conditionallyConcludeTransaction();
    }

    protected abstract String getQuerySessionName();
    protected abstract void submitResults();
    protected abstract boolean isFinalized();
}
