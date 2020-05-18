package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutor;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutorFactory;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBJoinWithNodeSession extends ADBLargeMessageActor {

    private final ActorRef<ADBSlaveQuerySession.Command> supervisor;
    private final ADBJoinQuery query;
    private final int remoteNodeId;
    private final AtomicInteger remainingResults = new AtomicInteger(0);
    private ActorRef<ADBJoinWithNodeSessionHandler.Command> sessionHandler;
    private List<ActorRef<ADBPartition.Command>> localPartitions;
    private List<ADBPartitionHeader> localHeaders;
    private Map<Integer, Map<String, List<ADBComparable2IntPair>>> foreignAttributes;

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class RegisterHandler implements Command, CborSerializable {
        private ActorRef<ADBJoinWithNodeSessionHandler.Command> sessionHandler;
    }

    @AllArgsConstructor
    private static class AllPartitionsHeaderWrapper implements Command {
        private final ADBPartitionManager.AllPartitionsAndHeaders response;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class ForeignNodeAttributes implements ADBLargeMessageSender.LargeMessage {
        private Map<Integer, Map<String, List<ADBComparable2IntPair>>> joinAttributes;
        private Map<Integer, int[]> fPartitionIdLeft;
        private Map<Integer, int[]> fPartitionIdRight;
    }

    @AllArgsConstructor
    public static class PartitionsJoinedWrapper implements Command {
        ADBPartitionJoinExecutor.PartitionsJoined joinResults;
    }

    @AllArgsConstructor
    public static class SemiMaterializedTuplesWrapper implements Command {
        ADBPartition.SemiMaterializedTuples result;
    }

    @AllArgsConstructor
    private static class CheckConclude implements Command {}

    @AllArgsConstructor
    private static class RequestForeignJoinAttributes implements Command {}

    public ADBJoinWithNodeSession(ActorContext<Command> context, ADBJoinQuery query,
                                  ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                  int remoteNodeId) {
        super(context);
        this.query = query;
        this.supervisor = supervisor;
        this.remoteNodeId = remoteNodeId;

        val respondTo = getContext().messageAdapter(ADBPartitionManager.AllPartitionsAndHeaders.class,
                AllPartitionsHeaderWrapper::new);
        assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";
        ADBPartitionManager.getInstance().tell(new ADBPartitionManager.RequestAllPartitionsAndHeaders(respondTo));
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(RegisterHandler.class, this::handleRegisterHandler)
                   .onMessage(RequestForeignJoinAttributes.class, this::handleRequestForeignJoinAttributes)
                   .onMessage(AllPartitionsHeaderWrapper.class, this::handleAllPartitionsHeader)
                   .onMessage(ForeignNodeAttributes.class, this::handleForeignAttributes)
                   .onMessage(PartitionsJoinedWrapper.class, this::handlePartitionsJoined)
                   .onMessage(CheckConclude.class, this::handleCheckConclude)
                   .build();
    }

    private Behavior<Command> handleRegisterHandler(RegisterHandler command) {
        this.sessionHandler = command.sessionHandler;
        this.getContext().getLog().info("[Create] On node#" + ADBSlave.ID + " to join node#" + remoteNodeId);
        this.getContext().getSelf().tell(new RequestForeignJoinAttributes());
        return Behaviors.same();
    }

    private Behavior<Command> handleAllPartitionsHeader(AllPartitionsHeaderWrapper wrapper) {
        this.getContext().getLog().debug("Received {} relevant headers", wrapper.response.getHeaders().size());
        this.localPartitions = wrapper.response.getPartitions();
        this.localHeaders = wrapper.response.getHeaders();
        if (wrapper.response.getHeaders().size() < 1) {
            this.getContext().getLog().warn("No relevant partition headers present. Concluding session.");
            this.getContext().getSelf().tell(new CheckConclude());
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestForeignJoinAttributes(RequestForeignJoinAttributes command) {
        if (this.localHeaders == null) {
            this.getContext().getLog().warn("Local headers not yet present. Unable to proceed join for the moment.");
            this.getContext().scheduleOnce(Duration.ofMillis(10), this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.sessionHandler.tell(new ADBJoinWithNodeSessionHandler.RequestJoinAttributes(this.localHeaders));
        return Behaviors.same();
    }

    private Behavior<Command> handleForeignAttributes(ForeignNodeAttributes command) {
        assert this.foreignAttributes == null : "JoinWithNode session already received valid foreign attributes!";
        this.foreignAttributes = command.getJoinAttributes();
        for (Map.Entry<Integer, int[]> mapping : command.fPartitionIdLeft.entrySet()) {
            for (int fPartitionId : mapping.getValue()) {
                this.remainingResults.incrementAndGet();
                this.spawnExecutor(mapping.getKey(), fPartitionId, false);
            }
        }
        if (ADBSlave.ID == this.remoteNodeId) {
            this.supervisor.tell(new ADBSlaveJoinSession.RequestNextPartition());
            return Behaviors.same();
        }
        for (Map.Entry<Integer, int[]> mapping : command.fPartitionIdRight.entrySet()) {
            for (int fPartitionId : mapping.getValue()) {
                this.remainingResults.incrementAndGet();
                this.spawnExecutor(mapping.getKey(), fPartitionId, true);
            }
        }
        this.supervisor.tell(new ADBSlaveJoinSession.RequestNextPartition());
        return Behaviors.same();
    }

    private void spawnExecutor(int lPartId, int fPartId, boolean isReversed) {
        ADBJoinQuery joinQuery = isReversed ? this.query.getReverse() : this.query;
        val name = ADBPartitionJoinExecutorFactory.name(lPartId, fPartId, joinQuery);
        val respondTo = getContext().messageAdapter(ADBPartitionJoinExecutor.PartitionsJoined.class, PartitionsJoinedWrapper::new);
        val behavior = ADBPartitionJoinExecutorFactory.createDefault(joinQuery, this.localPartitions.get(lPartId),
                this.foreignAttributes.get(fPartId), respondTo, isReversed);
        this.getContext().spawn(behavior, name);
    }

    private Behavior<Command> handlePartitionsJoined(PartitionsJoinedWrapper wrapper) {
        this.supervisor.tell(new ADBSlaveJoinSession.HandleJoinShardResults(wrapper.joinResults.getJoinTuples()));
        this.remainingResults.decrementAndGet();
        this.getContext().getSelf().tell(new CheckConclude());
        return Behaviors.same();
    }

    private Behavior<Command> handleCheckConclude(CheckConclude command) {
        if (this.sessionHandler == null) {
            this.getContext().getLog().warn("Session handler not yet present. Unable to proceed with join conclusion.");
            this.getContext().scheduleOnce(Duration.ofMillis(1), this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        if (this.remainingResults.get() > 0) {
            return Behaviors.same();
        }
        this.getContext().getLog().info("Concluding session.");
        this.sessionHandler.tell(new ADBJoinWithNodeSessionHandler.ConcludeSession());
        this.supervisor.tell(new ADBSlaveJoinSession.FinalizedInterNodeJoin());
        return Behaviors.stopped();
    }

    @Override
    protected Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response) {
        return Behaviors.same();
    }

}
