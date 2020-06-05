package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.ADBPartitionManager.AllPartitionsHeaders;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutor;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutor.PartitionsJoined;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutorFactory;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBJoinWithNodeSession extends ADBLargeMessageActor {

    private final ActorRef<ADBSlaveQuerySession.Command> supervisor;
    private final int remoteNodeId;
    private final ADBJoinQueryContext joinQueryContext;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final ObjectArrayFIFOQueue<ADBPartitionJoinTask> joinTasks = new ObjectArrayFIFOQueue<>();
    private final ObjectArrayFIFOQueue<ActorRef<ADBPartitionJoinExecutor.Command>> executorsIdle = new ObjectArrayFIFOQueue<>();
    private final ObjectArrayFIFOQueue<ActorRef<ADBPartitionJoinExecutor.Command>> executorsPrepared = new ObjectArrayFIFOQueue<>();
    private final AtomicInteger activeExecutors = new AtomicInteger(0);
    private int expectedNumberOfRemotePartitionHeaderResponses = 0;
    private int actualNumberOfRemotePartitionHeaderResponses = 0;

    private ActorRef<ADBPartitionManager.Command> foreignPartitionManager;
    private ActorRef<ADBJoinWithNodeSessionHandler.Command> sessionHandler;

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class RegisterHandler implements Command, CborSerializable {
        private ActorRef<ADBJoinWithNodeSessionHandler.Command> sessionHandler;
        private ActorRef<ADBPartitionManager.Command> foreignPartitionManager;
    }

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
    private static class Conclude implements Command {
    }

    public ADBJoinWithNodeSession(ActorContext<Command> context,
                                  ADBJoinQueryContext joinQueryContext,
                                  ActorRef<ADBSlaveQuerySession.Command> supervisor,
                                  int remoteNodeId) {
        super(context);
        this.supervisor = supervisor;
        this.joinQueryContext = joinQueryContext;
        this.remoteNodeId = remoteNodeId;

        val respondTo = getContext().messageAdapter(AllPartitionsHeaders.class, AllPartitionsHeaderWrapper::new);
        assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";
        ADBPartitionManager.getInstance().tell(new ADBPartitionManager.RequestAllPartitionHeaders(respondTo));
        this.setUpExecutors();
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(RegisterHandler.class, this::handleRegisterHandler)
                   .onMessage(AllPartitionsHeaderWrapper.class, this::handleAllPartitionHeaders)
                   .onMessage(RelevantJoinPartitions.class, this::handleRelevantForeignPartitions)
                   .onMessage(ExecutorResponseWrapper.class, this::handleExecutorResponse)
                   .onMessage(Conclude.class, this::handleConclude)
                   .build();
    }

    private Behavior<Command> handleRegisterHandler(RegisterHandler command) {
        this.sessionHandler = command.sessionHandler;
        this.foreignPartitionManager = command.foreignPartitionManager;
        this.getContext().getLog().info("[Create] On node#" + ADBSlave.ID + " to join node#" + remoteNodeId);
        return Behaviors.same();
    }

    private Behavior<Command> handleAllPartitionHeaders(AllPartitionsHeaderWrapper wrapper) {
        if (this.sessionHandler == null) {
            this.getContext().scheduleOnce(Duration.ofMillis(10), this.getContext().getSelf(), wrapper);
            return Behaviors.same();
        }
        if (wrapper.response.getHeaders().size() < 1) {
            this.getContext().getLog().warn("No relevant partition headers present. Concluding session.");
            this.getContext().getSelf().tell(new Conclude());
        }
        this.expectedNumberOfRemotePartitionHeaderResponses = wrapper.response.getHeaders().size();
        for (ADBPartitionHeader header : wrapper.response.getHeaders()) {
            this.sessionHandler.tell(new ADBJoinWithNodeSessionHandler.ProvideRelevantJoinPartitions(header));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleRelevantForeignPartitions(RelevantJoinPartitions command) {
        this.generateLeftSideJoinTasks(command.fPartitionIdLeft, command.lPartitionId);
        if (ADBSlave.ID != this.remoteNodeId) {
            this.generateRightSideJoinTasks(command.fPartitionIdRight, command.lPartitionId);
        }
        this.actualNumberOfRemotePartitionHeaderResponses++;
        this.getContext().getLog().debug("Generated " + this.joinTasks.size() + " join tasks");
        this.nextExecutionRound();
        return Behaviors.same();
    }

    private void generateLeftSideJoinTasks(int[] foreignPartitionIds, int localPartitionId) {
        for (int fPartitionId : foreignPartitionIds) {
            this.joinTasks.enqueue(ADBPartitionJoinTask
                    .builder()
                    .leftPartitionId(fPartitionId)
                    .leftPartitionManager(this.foreignPartitionManager)
                    .rightPartitionId(localPartitionId)
                    .rightPartitionManager(ADBPartitionManager.getInstance())
                    .build());
        }
    }

    private void generateRightSideJoinTasks(int[] foreignPartitionIds, int localPartitionId) {
        for (int fPartitionId : foreignPartitionIds) {
            this.joinTasks.enqueue(ADBPartitionJoinTask
                    .builder()
                    .leftPartitionId(localPartitionId)
                    .leftPartitionManager(ADBPartitionManager.getInstance())
                    .rightPartitionId(fPartitionId)
                    .rightPartitionManager(this.foreignPartitionManager)
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
                                   ObjectList<ADBKeyPair> joinTuples) {
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
        this.getContext().getLog().info("Concluding session.");
        this.sessionHandler.tell(new ADBJoinWithNodeSessionHandler.ConcludeSession());
        this.supervisor.tell(new ADBSlaveJoinSession.RequestNextPartitions());
        return Behaviors.stopped();
    }

    private boolean isReadyToConclude() {
        return executorsIdle.size() == settings.PARALLEL_PARTITION_JOINS &&
                executorsPrepared.isEmpty() &&
                joinTasks.isEmpty() &&
                this.activeExecutors.get() == 0 &&
                actualNumberOfRemotePartitionHeaderResponses == expectedNumberOfRemotePartitionHeaderResponses;
    }
}
