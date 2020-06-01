package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.ADBPartitionManager.AllPartitionsAndHeaders;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutor;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutor.PartitionsJoined;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutorFactory;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBJoinWithNodeSession extends ADBLargeMessageActor {

    private final ActorRef<ADBSlaveQuerySession.Command> supervisor;
    private final int remoteNodeId;
    private final ADBJoinQueryContext joinQueryContext;
    private final AtomicInteger runningExecutors = new AtomicInteger(0);
    private ActorRef<ADBJoinWithNodeSessionHandler.Command> sessionHandler;
    private ObjectList<ActorRef<ADBPartition.Command>> localPartitions;
    private ObjectList<ADBPartitionHeader> localHeaders;
    private final ObjectArrayFIFOQueue<ADBPartitionJoinTask> joinTasks = new ObjectArrayFIFOQueue<>();

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class RegisterHandler implements Command, CborSerializable {
        private ActorRef<ADBJoinWithNodeSessionHandler.Command> sessionHandler;
    }

    @AllArgsConstructor
    private static class AllPartitionsHeaderWrapper implements Command {
        private final AllPartitionsAndHeaders response;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class ForeignNodeAttributes implements ADBLargeMessageSender.LargeMessage {
        private Int2ObjectMap<Map<String, ObjectList<ADBEntityEntry>>> joinAttributes;
        private Int2ObjectMap<int[]> fPartitionIdLeft;
        private Int2ObjectMap<int[]> fPartitionIdRight;
    }

    @AllArgsConstructor
    public static class PartitionsJoinedWrapper implements Command {
        ADBPartitionJoinExecutor.PartitionsJoined joinResults;
    }

    @AllArgsConstructor
    private static class Conclude implements Command {
    }

    @AllArgsConstructor
    private static class RequestForeignJoinAttributes implements Command {
    }

    public ADBJoinWithNodeSession(ActorContext<Command> context,
                                  ADBJoinQueryContext joinQueryContext,
                                  ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                  int remoteNodeId) {
        super(context);
        ADBQueryPerformanceSampler.log(true, "ADBJoinWithNodeSession", "start", this.hashCode());
        this.supervisor = supervisor;
        this.joinQueryContext = joinQueryContext;
        this.remoteNodeId = remoteNodeId;

        val respondTo = getContext().messageAdapter(AllPartitionsAndHeaders.class, AllPartitionsHeaderWrapper::new);
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
                   .onMessage(Conclude.class, this::handleConclude)
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
            this.getContext().getSelf().tell(new Conclude());
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
        val messageAdapter = getContext().messageAdapter(PartitionsJoined.class, PartitionsJoinedWrapper::new);
        for (Int2ObjectMap.Entry<int[]> mapping : command.fPartitionIdLeft.int2ObjectEntrySet()) {
            for (int fPartitionId : mapping.getValue()) {
                this.joinTasks.enqueue(ADBPartitionJoinTask
                        .builder()
                        .reversed(false)
                        .localPartition(this.localPartitions.get(mapping.getIntKey()))
                        .query(this.joinQueryContext.getQuery())
                        .foreignAttributes(command.getJoinAttributes().get(fPartitionId))
                        .respondTo(messageAdapter)
                        .build());
            }
        }
        if (ADBSlave.ID == this.remoteNodeId) {
            this.triggerExecution(Settings.SettingsProvider.get(getContext().getSystem()).PARALLEL_PARTITION_JOINS);
            return Behaviors.same();
        }
        for (Int2ObjectMap.Entry<int[]> mapping : command.fPartitionIdRight.int2ObjectEntrySet()) {
            for (int fPartitionId : mapping.getValue()) {
                this.joinTasks.enqueue(ADBPartitionJoinTask
                        .builder()
                        .reversed(true)
                        .localPartition(this.localPartitions.get(mapping.getIntKey()))
                        .query(this.joinQueryContext.getQuery().getReverse())
                        .foreignAttributes(command.getJoinAttributes().get(fPartitionId))
                        .respondTo(messageAdapter)
                        .build());
            }
        }
        this.triggerExecution(Settings.SettingsProvider.get(getContext().getSystem()).PARALLEL_PARTITION_JOINS);
        return Behaviors.same();
    }

    private Behavior<Command> handlePartitionsJoined(PartitionsJoinedWrapper wrapper) {
        this.supervisor.tell(new ADBSlaveJoinSession.HandleJoinShardResults(wrapper.joinResults.getJoinTuples()));
        this.triggerExecution(1);
        if (this.runningExecutors.decrementAndGet() < 1 && this.joinTasks.isEmpty()) {
            this.getContext().getSelf().tell(new Conclude());
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleConclude(Conclude command) {
        if (this.sessionHandler == null) {
            this.getContext().getLog().warn("Session handler not yet present. Unable to proceed with join conclusion.");
            this.getContext().scheduleOnce(Duration.ofMillis(1), this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.getContext().getLog().info("Concluding session.");
        this.sessionHandler.tell(new ADBJoinWithNodeSessionHandler.ConcludeSession());
        ADBQueryPerformanceSampler.log(false, "ADBJoinWithNodeSession", "stop", this.hashCode());
        this.supervisor.tell(new ADBSlaveJoinSession.RequestNextPartitions());
        return Behaviors.stopped();
    }

    private void triggerExecution(int maxExecutions) {
        for (int i = 0; i < maxExecutions && !this.joinTasks.isEmpty(); i++) {
            this.runningExecutors.incrementAndGet();
            this.spawnExecutor(this.joinTasks.dequeue());
        }
    }

    private void spawnExecutor(ADBPartitionJoinTask task) {
        val behavior = ADBPartitionJoinExecutorFactory.createDefault(task);
        this.getContext().spawn(behavior, ADBPartitionJoinExecutorFactory.name(task.getQuery()));
    }
}
