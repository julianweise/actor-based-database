package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
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
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ADBNodeJoin extends ADBLargeMessageActor {

    private final ActorRef<ADBSlaveQuerySession.Command> supervisor;
    private final ADBNodeJoinContext context;
    private final ADBJoinQueryContext joinQueryContext;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final ObjectArrayFIFOQueue<ADBPartitionJoinTask> joinTasks = new ObjectArrayFIFOQueue<>();
    private final ObjectArrayFIFOQueue<ActorRef<ADBPartitionJoinExecutor.Command>> executorsIdle = new ObjectArrayFIFOQueue<>();
    private final ObjectArrayFIFOQueue<ActorRef<ADBPartitionJoinExecutor.Command>> executorsPrepared = new ObjectArrayFIFOQueue<>();
    private final AtomicInteger activeExecutors = new AtomicInteger(0);
    private ObjectList<ADBPartitionHeader> leftPartitionHeaders;
    private Int2ObjectOpenHashMap<ADBPartitionHeader> rightPartitionHeaders;
    private int actualNumberOfRemotePartitionHeaderResponses = 0;
    private int initialWorkflowSize;
    private boolean requestedNextNodeComparison = false;


    @AllArgsConstructor
    public static class Execute implements Command {}

    @AllArgsConstructor
    public static class AllPartitionsHeaderWrapper implements Command {
        private final AllPartitionsHeaders response;
    }

    public static class TakeOverWork implements Command {
        ObjectList<ADBPartitionJoinTask> joinTasks;
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
    @NoArgsConstructor
    @Getter
    public static class HandOverWork implements Command, CborSerializable {
        ActorRef<ADBSlaveQuerySession.Command> respondTo;
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

    public ADBNodeJoin(ActorContext<Command> context,
                       ADBJoinQueryContext joinQueryContext,
                       ActorRef<ADBSlaveQuerySession.Command> supervisor,
                       ADBNodeJoinContext joinNodesContext) {
        super(context);
        this.supervisor = supervisor;
        this.joinQueryContext = joinQueryContext;
        this.context = joinNodesContext;
        this.setUpExecutors();
        this.getContext().getLog().info("[Start] {}", this.context);
    }

    @Override
    public Receive<Command> createReceive() {
        return this.newReceiveBuilder()
                   .onMessage(Execute.class, this::handleExecute)
                   .onMessage(TakeOverWork.class, this::handleTakeOverWork)
                   .onMessage(AllPartitionsHeaderWrapper.class, this::handleAllPartitionHeaders)
                   .onMessage(ExecutorResponseWrapper.class, this::handleExecutorResponse)
                   .onMessage(RelevantPartitionsWrapper.class, this::handleRelevantForeignPartitions)
                   .onMessage(HandOverWork.class, this::handleHandOverWork)
                   .onMessage(Conclude.class, this::handleConclude)
                   .build();
    }

    private Behavior<Command> handleExecute(Execute command) {
        val respondTo = getContext().messageAdapter(AllPartitionsHeaders.class, AllPartitionsHeaderWrapper::new);
        this.context.getLeft().tell(new ADBPartitionManager.RequestAllPartitionHeaders(respondTo));
        return Behaviors.same();
    }

    private Behavior<Command> handleTakeOverWork(TakeOverWork command) {
        command.joinTasks.forEach(this.joinTasks::enqueue);
        this.nextExecutionRound();
        return Behaviors.same();
    }

    private Behavior<Command> handleAllPartitionHeaders(AllPartitionsHeaderWrapper wrapper) {
        if (wrapper.response.getHeaders().size() < 1) {
            this.getContext().getLog().warn("No relevant partition headers present. Concluding session.");
            this.getContext().getSelf().tell(new Conclude());
        }
        this.leftPartitionHeaders = wrapper.response.getHeaders();
        val respondTo = getContext().messageAdapter(RelevantPartitionsJoinQuery.class, RelevantPartitionsWrapper::new);
        for (ADBPartitionHeader header : wrapper.response.getHeaders()) {
            context.getRight().tell(new RequestPartitionsForJoinQuery(Adapter.toClassic(respondTo), header,
                    joinQueryContext.getQuery()));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleRelevantForeignPartitions(RelevantPartitionsWrapper wrapper) {
        this.rightPartitionHeaders = wrapper.response.getPartitionHeaders();
        this.generateLeftSideJoinTasks(wrapper.response.getLPartitionIdsLeft(), wrapper.response.getFPartitionId());
        if (context.getLeftNodeId() != context.getRightNodeId()) {
            generateRightSideJoinTasks(wrapper.response.getLPartitionIdsRight(), wrapper.response.getFPartitionId());
        }
        this.actualNumberOfRemotePartitionHeaderResponses++;
        this.nextExecutionRound();
        return Behaviors.same();
    }

    private Behavior<Command> handleHandOverWork(HandOverWork command) {
        int halfOfPreparedTasks = (int) (this.executorsPrepared.size() * this.settings.WORK_STEALING_AMOUNT);
        int halfOfTasks = (int) (this.joinTasks.size() * this.settings.WORK_STEALING_AMOUNT);
        int numberOfTasksToSteal = Math.min(this.joinTasks.size(), halfOfPreparedTasks + halfOfTasks);
        List<ADBPartitionJoinTask> tasksToSteal = IntStream.range(0, numberOfTasksToSteal)
                                                           .mapToObj(i -> this.joinTasks.dequeue())
                                                           .collect(Collectors.toList());
        int MAX_SIZE = 50;
        int completePartitions = tasksToSteal.size() / MAX_SIZE;
        for (int i = 0; i < completePartitions; i++) {
            val transferPartition = tasksToSteal.subList(i * MAX_SIZE, (i + 1) * MAX_SIZE);
            command.respondTo.tell(new ADBSlaveJoinSession.TakeOverWork(this.context, transferPartition, false));
        }
        val transferPartition = tasksToSteal.subList(completePartitions * MAX_SIZE, tasksToSteal.size());
        command.respondTo.tell(new ADBSlaveJoinSession.TakeOverWork(this.context, transferPartition, true));
        return Behaviors.same();
    }

    private void generateLeftSideJoinTasks(int[] foreignPartitionIds, int localPartitionId) {
        for (int fPartitionId : foreignPartitionIds) {
            val leftHeader = this.leftPartitionHeaders.get(localPartitionId);
            assert leftHeader.getId() == localPartitionId : "Incorrect header selected. Check id and sorting!";
            this.initialWorkflowSize++;
            this.joinTasks.enqueue(ADBPartitionJoinTask
                    .builder()
                    .leftPartitionId(fPartitionId)
                    .leftPartitionManager(this.context.getRight())
                    .leftHeader(this.rightPartitionHeaders.get(fPartitionId))
                    .rightPartitionId(localPartitionId)
                    .rightPartitionManager(this.context.getLeft())
                    .rightHeader(this.leftPartitionHeaders.get(localPartitionId))
                    .build());
        }
    }

    private void generateRightSideJoinTasks(int[] foreignPartitionIds, int localPartitionId) {
        for (int fPartitionId : foreignPartitionIds) {
            val leftHeader = this.leftPartitionHeaders.get(localPartitionId);
            assert leftHeader.getId() == localPartitionId : "Incorrect header selected. Check id and sorting!";
            this.initialWorkflowSize++;
            this.joinTasks.enqueue(ADBPartitionJoinTask
                    .builder()
                    .leftPartitionId(localPartitionId)
                    .leftPartitionManager(this.context.getLeft())
                    .leftHeader(leftHeader)
                    .rightPartitionId(fPartitionId)
                    .rightPartitionManager(this.context.getRight())
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

    private void handleJoinResults(ActorRef<ADBPartitionJoinExecutor.Command> executor, ADBPartialJoinResult joinTuples) {
        this.executorsIdle.enqueue(executor);
        this.activeExecutors.decrementAndGet();
        this.supervisor.tell(new ADBSlaveJoinSession.JoinPartitionsResults(joinTuples));
    }

    private void setUpExecutors() {
        ActorRef<ADBPartitionJoinExecutor.Response> respondTo =
                getContext().messageAdapter(ADBPartitionJoinExecutor.Response.class, ExecutorResponseWrapper::new);
        for (int i = 0; i < this.numberOfExecutors(); i++) {
            this.executorsIdle.enqueue(this.spawnExecutor(respondTo));
        }
    }

    private ActorRef<ADBLargeMessageActor.Command> spawnExecutor(ActorRef<ADBPartitionJoinExecutor.Response> rspTo) {
        val behavior = ADBPartitionJoinExecutorFactory.createDefault(this.joinQueryContext.getQuery(), rspTo);
        return this.getContext().spawn(behavior, ADBPartitionJoinExecutorFactory.name(joinQueryContext.getQuery()));
    }

    private void nextExecutionRound() {
        while(!executorsPrepared.isEmpty() && this.activeExecutors.get() <= this.settings.NUMBER_OF_THREADS) {
            this.executeNextTask();
        }
        while (!this.joinTasks.isEmpty() && !this.executorsIdle.isEmpty()) {
            this.prepareNextTask();
        }
        if (this.isReadyToConclude()) {
            this.getContext().getSelf().tell(new Conclude());
        }
        if (this.isReadyToPrepareNextNodeComparison() && !this.requestedNextNodeComparison) {
            this.getContext().getLog().info("Current process {}%: Trigger next Inter-Node-Join", this.process() * 100);
            this.supervisor.tell(new ADBSlaveJoinSession.RequestNextPartitions());
            this.requestedNextNodeComparison = true;
        }
    }

    private void executeNextTask() {
        executorsPrepared.dequeue().tell(new ADBPartitionJoinExecutor.Execute());
        this.activeExecutors.incrementAndGet();
    }

    private void prepareNextTask() {
        ADBPartitionJoinTask task = this.joinTasks.dequeue();
        ActorRef<ADBPartitionJoinExecutor.Command> executor = this.executorsIdle.dequeue();
        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));
    }

    private Behavior<Command> handleConclude(Conclude command) {
        this.getContext().getLog().info("[Stop] {}", this.context);
        return Behaviors.stopped();
    }

    private boolean isReadyToConclude() {
        return executorsIdle.size() == this.numberOfExecutors() &&
                executorsPrepared.isEmpty() &&
                joinTasks.isEmpty() &&
                this.activeExecutors.get() == 0 &&
                this.isAllRelevantHeadersProcessed();
    }

    private boolean isAllRelevantHeadersProcessed() {
        return this.actualNumberOfRemotePartitionHeaderResponses == this.leftPartitionHeaders.size();
    }

    private boolean isReadyToPrepareNextNodeComparison() {
        return this.isAllRelevantHeadersProcessed() && this.process() >= this.settings.THRESHOLD_NEXT_NODE_COMPARISON;
    }

    private float process() {
        if (!this.isAllRelevantHeadersProcessed()) {
            return 0;
        }
        if (this.initialWorkflowSize == 0) {
            return 1;
        }
        return 1 - ((float) this.joinTasks.size() / this.initialWorkflowSize);
    }

    private int numberOfExecutors() {
        return this.settings.NUMBER_OF_THREADS * 2;
    }

    @Override
    protected void handleReceiverTerminated() {}
}
