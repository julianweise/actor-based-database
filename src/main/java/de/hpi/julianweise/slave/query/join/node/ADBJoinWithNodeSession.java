package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.ADBPartitionManager.AllPartitionsHeaders;
import de.hpi.julianweise.slave.partition.ADBPartitionManager.RelevantPartitionsJoinQuery;
import de.hpi.julianweise.slave.partition.ADBPartitionManager.RequestPartitionsForJoinQuery;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;
import de.hpi.julianweise.slave.query.join.ADBPartialJoinResult;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutor;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutor.PartitionsJoined;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutorFactory;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;

public class ADBJoinWithNodeSession extends ADBLargeMessageActor {

    private final ActorRef<ADBSlaveQuerySession.Command> supervisor;
    private final ADBJoinNodesContext joinNodesContext;
    private final ADBJoinQueryContext joinQueryContext;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final ObjectArrayFIFOQueue<ADBPartitionJoinTask> joinTasks = new ObjectArrayFIFOQueue<>();
    private final ObjectArrayFIFOQueue<ActorRef<ADBPartitionJoinExecutor.Command>> executorsIdle = new ObjectArrayFIFOQueue<>();
    private final ObjectArrayFIFOQueue<ActorRef<ADBPartitionJoinExecutor.Command>> executorsPrepared = new ObjectArrayFIFOQueue<>();
    private final AtomicInteger activeExecutors = new AtomicInteger(0);
    private ObjectList<ADBPartitionHeader> leftPartitionHeaders;
    private Int2ObjectOpenHashMap<ADBPartitionHeader> rightPartitionHeaders;
    private int actualNumberOfRemotePartitionHeaderResponses = 0;
    private boolean requestedNextNodeComparison = false;


    @AllArgsConstructor
    public static class AllPartitionsHeaderWrapper implements Command {
        private final AllPartitionsHeaders response;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Builder
    public static class RelevantJoinPartitions implements Command, KryoSerializable {
        private int lPartitionId;
        private int[] fPartitionIdLeft;
        private int[] fPartitionIdRight;
    }

    @AllArgsConstructor
    public static class ExecutorResponseWrapper implements Command {
        ADBPartitionJoinExecutor.Response response;
    }

    @AllArgsConstructor
    public static class RelevantPartitionsWrapper implements Command {
        RelevantPartitionsJoinQuery response;
    }

    @AllArgsConstructor
    private static class Conclude implements Command { }

    public ADBJoinWithNodeSession(ActorContext<Command> context,
                                  ADBJoinQueryContext joinQueryContext,
                                  ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                  ADBJoinNodesContext joinNodesContext) {
        super(context);
        this.supervisor = supervisor;
        this.joinQueryContext = joinQueryContext;
        this.joinNodesContext = joinNodesContext;
        val respondTo = getContext().messageAdapter(AllPartitionsHeaders.class, AllPartitionsHeaderWrapper::new);
        this.joinNodesContext.getLeft().tell(new ADBPartitionManager.RequestAllPartitionHeaders(respondTo));
        this.getContext().getLog().info("[Create] On node#" + ADBSlave.ID + " to execute: " + this.joinNodesContext);
        this.setUpExecutors();
    }

    @Override
    public Receive<Command> createReceive() {
        return this.newReceiveBuilder()
                   .onMessage(AllPartitionsHeaderWrapper.class, this::handleAllPartitionHeaders)
                   .onMessage(ExecutorResponseWrapper.class, this::handleExecutorResponse)
                   .onMessage(RelevantPartitionsWrapper.class, this::handleRelevantForeignPartitions)
                   .onMessage(Conclude.class, this::handleConclude)
                   .build();
    }

    private Behavior<Command> handleAllPartitionHeaders(AllPartitionsHeaderWrapper wrapper) {
        if (wrapper.response.getHeaders().size() < 1) {
            this.getContext().getLog().warn("No relevant partition headers present. Concluding session.");
            this.getContext().getSelf().tell(new Conclude());
        }
        this.leftPartitionHeaders = wrapper.response.getHeaders();
        val respondTo = getContext().messageAdapter(RelevantPartitionsJoinQuery.class, RelevantPartitionsWrapper::new);
        for (ADBPartitionHeader header : wrapper.response.getHeaders()) {
            joinNodesContext.getRight().tell(new RequestPartitionsForJoinQuery(Adapter.toClassic(respondTo), header,
                    joinQueryContext.getQuery()));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleRelevantForeignPartitions(RelevantPartitionsWrapper wrapper) {
        this.rightPartitionHeaders = wrapper.response.getPartitionHeaders();
        this.generateLeftSideJoinTasks(wrapper.response.getLPartitionIdsLeft(), wrapper.response.getFPartitionId());
        if (joinNodesContext.getLeftNodeId() != joinNodesContext.getRightNodeId()) {
            generateRightSideJoinTasks(wrapper.response.getLPartitionIdsRight(), wrapper.response.getFPartitionId());
        }
        this.actualNumberOfRemotePartitionHeaderResponses++;
        this.nextExecutionRound();
        return Behaviors.same();
    }

    private void generateLeftSideJoinTasks(int[] foreignPartitionIds, int localPartitionId) {
        for (int fPartitionId : foreignPartitionIds) {
            val leftHeader = this.leftPartitionHeaders.get(localPartitionId);
            assert leftHeader.getId() == localPartitionId : "Incorrect header selected. Check id and sorting!";
            this.joinTasks.enqueue(ADBPartitionJoinTask
                    .builder()
                    .leftPartitionId(fPartitionId)
                    .leftPartitionManager(this.joinNodesContext.getRight())
                    .leftHeader(this.rightPartitionHeaders.get(fPartitionId))
                    .rightPartitionId(localPartitionId)
                    .rightPartitionManager(this.joinNodesContext.getLeft())
                    .rightHeader(this.leftPartitionHeaders.get(localPartitionId))
                    .build());
        }
    }

    private void generateRightSideJoinTasks(int[] foreignPartitionIds, int localPartitionId) {
        for (int fPartitionId : foreignPartitionIds) {
            val leftHeader = this.leftPartitionHeaders.get(localPartitionId);
            assert leftHeader.getId() == localPartitionId : "Incorrect header selected. Check id and sorting!";
            this.joinTasks.enqueue(ADBPartitionJoinTask
                    .builder()
                    .leftPartitionId(localPartitionId)
                    .leftPartitionManager(this.joinNodesContext.getLeft())
                    .leftHeader(leftHeader)
                    .rightPartitionId(fPartitionId)
                    .rightPartitionManager(this.joinNodesContext.getRight())
                    .rightHeader(this.rightPartitionHeaders.get(fPartitionId))
                    .build());
        }
    }

    private Behavior<Command> handleExecutorResponse(ExecutorResponseWrapper wrapper) {
        if (wrapper.response instanceof ADBPartitionJoinExecutor.PartitionsJoined) {
            ADBPartitionJoinExecutor.PartitionsJoined message = ((PartitionsJoined)wrapper.response);
            this.handleJoinResults(message.getRespondTo(), message.getJoinTuples());
        } else if (wrapper.response instanceof ADBPartitionJoinExecutor.JoinTaskPrepared) {
            this.executorsPrepared.enqueue(((ADBPartitionJoinExecutor.JoinTaskPrepared) wrapper.response).getRespondTo());
        }
        this.nextExecutionRound();
        return Behaviors.same();
    }

    private void handleJoinResults(ActorRef<ADBPartitionJoinExecutor.Command> executor,
                                   ADBPartialJoinResult joinTuples) {
        getContext().getLog().debug("Received " + joinTuples.size() + " results from task");
        this.executorsIdle.enqueue(executor);
        this.activeExecutors.decrementAndGet();
        this.supervisor.tell(new ADBSlaveJoinSession.JoinPartitionsResults(joinTuples));
    }

    private void setUpExecutors() {
        val respondTo = getContext().messageAdapter(ADBPartitionJoinExecutor.Response.class, ExecutorResponseWrapper::new);
        for (int i = 0; i < Settings.SettingsProvider.get(getContext().getSystem()).PARALLEL_PARTITION_JOINS; i++) {
            val behavior = ADBPartitionJoinExecutorFactory.createDefault(this.joinQueryContext.getQuery(), respondTo);
            val executor = getContext().spawn(behavior, ADBPartitionJoinExecutorFactory.name(joinQueryContext.getQuery()));
            this.executorsIdle.enqueue(executor);
        }
    }

    private void nextExecutionRound() {
        while(!executorsPrepared.isEmpty() && this.activeExecutors.get() <= this.settings.NUMBER_OF_THREADS - 1) {
            this.executeNextTask();
        }
        while (!this.joinTasks.isEmpty() && !this.executorsIdle.isEmpty()) {
            this.prepareNextTask();
        }
        if (this.isReadyToConclude()) {
            this.getContext().getSelf().tell(new Conclude());
        }
        if (this.isReadyToPrepareNextNodeComparison() && !requestedNextNodeComparison) {
            this.supervisor.tell(new ADBSlaveJoinSession.RequestNextPartitions());
            this.requestedNextNodeComparison = true;
        }
    }

    private void executeNextTask() {
        executorsPrepared.dequeue().tell(new ADBPartitionJoinExecutor.Execute());
        this.getContext().getLog().debug("Executing next task");
        this.activeExecutors.incrementAndGet();
    }

    private void prepareNextTask() {
        ADBPartitionJoinTask task = this.joinTasks.dequeue();
        ActorRef<ADBPartitionJoinExecutor.Command> executor = this.executorsIdle.dequeue();
        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));
    }

    private Behavior<Command> handleConclude(Conclude command) {
        this.getContext().getLog().info("Concluding session for context: " + this.joinNodesContext);
        return Behaviors.stopped();
    }

    private boolean isReadyToConclude() {
        return executorsIdle.size() == settings.PARALLEL_PARTITION_JOINS &&
                executorsPrepared.isEmpty() &&
                joinTasks.isEmpty() &&
                this.activeExecutors.get() == 0 &&
                this.isAllRelevantHeadersProcessed();
    }

    private boolean isAllRelevantHeadersProcessed() {
        return this.actualNumberOfRemotePartitionHeaderResponses == this.leftPartitionHeaders.size();
    }

    private boolean isReadyToPrepareNextNodeComparison() {
        return this.isAllRelevantHeadersProcessed() && this.joinTasks.size() <= settings.THRESHOLD_NEXT_NODE_COMPARISON;
    }

    @Override
    protected void handleReceiverTerminated() {
    }
}
