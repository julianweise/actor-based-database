package de.hpi.julianweise.utility.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Optional;

public class JoinExecutionPlan extends AbstractBehavior<JoinExecutionPlan.Command> {

    public interface Command {
    }

    public interface Response {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class GetNextJoinNodePair implements Command {
        private ActorRef<ADBPartitionManager.Command> requestingManager;
        private ActorRef<Response> responseTo;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Getter
    public static class NextJoinNodePair implements JoinExecutionPlan.Response {
        private ActorRef<ADBPartitionManager.Command> requestingPartitionManager;
        private ActorRef<ADBPartitionManager.Command> leftQueryManager;
        private ActorRef<ADBPartitionManager.Command> rightQueryManager;
        private boolean hasNode;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class StealWork implements Response {
        private ActorRef<ADBPartitionManager.Command> requestingPartitionManager;
        private ActorRef<ADBPartitionManager.Command> target;
    }

    public static Behavior<Command> createDefault(
            ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers) {
        return Behaviors.setup(context -> new JoinExecutionPlan(context, partitionManagers));
    }

    private final ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers;
    private final ObjectList<ADBPair<ActorRef<ADBPartitionManager.Command>, ActorRef<ADBPartitionManager.Command>>> joinTasks;
    private final Object2IntMap<ActorRef<ADBPartitionManager.Command>> dataAccesses = new Object2IntOpenHashMap<>();
    private final Object2IntMap<ActorRef<ADBPartitionManager.Command>> joinExecutions = new Object2IntOpenHashMap<>();
    private final JoinExecutionPlanHistory history;

    public JoinExecutionPlan(ActorContext<Command> context,
                             ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers) {
        super(context);
        this.partitionManagers = partitionManagers;
        this.history = new JoinExecutionPlanHistory();
        this.joinTasks = this.initializeJoinTasks();
        this.initializeCounter();
    }

    private ObjectList<ADBPair<ActorRef<ADBPartitionManager.Command>, ActorRef<ADBPartitionManager.Command>>> initializeJoinTasks() {
        ObjectList<ADBPair<ActorRef<ADBPartitionManager.Command>, ActorRef<ADBPartitionManager.Command>>> joinTasks =
                new ObjectArrayList<>(this.partitionManagers.size() * 2);
        for (int a = 0; a < this.partitionManagers.size(); a++) {
            for (int b = a + 1; b < this.partitionManagers.size(); b++) {
                joinTasks.add(new ADBPair<>(this.partitionManagers.get(a), this.partitionManagers.get(b)));
            }
        }
        return joinTasks;
    }

    private void initializeCounter() {
        for (ActorRef<ADBPartitionManager.Command> manager : this.partitionManagers) {
            this.dataAccesses.put(manager, 0);
            this.joinExecutions.put(manager, 0);
        }
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(GetNextJoinNodePair.class, this::handleGetNextJoinNodePair)
                .build();
    }

    private Behavior<Command> handleGetNextJoinNodePair(GetNextJoinNodePair command) {
        this.history.logFinalizedNodeJoin(this.partitionManagers.indexOf(command.requestingManager));
        if (this.joinTasks.parallelStream().anyMatch(pair -> pair.contains(command.requestingManager))) {
            this.returnLocalJoinTask(command);
        } else {
            this.returnForeignJoinTask(command);
        }
        return Behaviors.same();
    }

    private void returnLocalJoinTask(GetNextJoinNodePair command) {
        ActorRef<ADBPartitionManager.Command> target = this.joinTasks
                .parallelStream()
                .filter(pair -> pair.contains(command.requestingManager))
                .map(pair -> pair.getKey().equals(command.requestingManager) ? pair.getValue() : pair.getKey())
                .min(this::sortJoinTasksByMinimalDataAccessOfTarget)
                .orElse(null);

        this.logJoinExecution(command.requestingManager, target, command.requestingManager);
        this.sendNextJoinPair(command.requestingManager, target, command.requestingManager, command.responseTo);
    }

    private void logJoinExecution(ActorRef<ADBPartitionManager.Command> left,
                                  ActorRef<ADBPartitionManager.Command> right,
                                  ActorRef<ADBPartitionManager.Command> executor) {
        this.history.logNodeJoin(partitionManagers.indexOf(executor), partitionManagers.indexOf(left),
                partitionManagers.indexOf(right));
        this.getContext().getLog().info("[Overall Process]: {}/{}", this.joinTasks.size(),
                this.partitionManagers.size() * (this.partitionManagers.size() + 1) / 2);
        this.dataAccesses.put(left, this.dataAccesses.getInt(left) + 1);
        this.dataAccesses.put(right, this.dataAccesses.getInt(right) + 1);
        this.joinExecutions.put(executor, this.joinExecutions.getInt(executor) + 1);
        this.joinTasks.remove(new ADBPair<>(left, right));
        this.joinTasks.remove(new ADBPair<>(right, left));
    }

    private int sortJoinTasksByMinimalDataAccessOfTarget(ActorRef<ADBPartitionManager.Command> a,
                                                         ActorRef<ADBPartitionManager.Command> b) {
        if (this.dataAccesses.getInt(a) == this.dataAccesses.getInt(b)) {
            return this.sortJoinTasksByMaximumExecutionOfTarget(a, b);
        }
        return Integer.compare(this.dataAccesses.getInt(a), this.dataAccesses.getInt(b));
    }

    private int sortJoinTasksByMaximumExecutionOfTarget(ActorRef<ADBPartitionManager.Command> a,
                                                        ActorRef<ADBPartitionManager.Command> b) {
        return Integer.compare(this.joinExecutions.getInt(a), this.joinExecutions.getInt(b)) * (-1);
    }

    private void returnForeignJoinTask(GetNextJoinNodePair command) {
        Optional<ADBPair<ActorRef<ADBPartitionManager.Command>, ActorRef<ADBPartitionManager.Command>>> foreignTask =
                this.joinTasks.parallelStream().max(this::sortJoinTasksByExpectedDelayUntilStart);
        if (foreignTask.isPresent()) {
            this.logJoinExecution(foreignTask.get().getKey(), foreignTask.get().getValue(), command.requestingManager);
            this.sendNextJoinPair(foreignTask.get().getKey(), foreignTask.get().getValue(), command.requestingManager, command.responseTo);
        } else {
            command.responseTo.tell(NextJoinNodePair.builder()
                                                    .hasNode(false)
                                                    .requestingPartitionManager(command.requestingManager)
                                                    .build());
        }
    }

    private void stealWork(GetNextJoinNodePair command) {
        int targetNodeId = this.history.getLastNodeHandlingAJoin(partitionManagers.indexOf(command.requestingManager));
        command.responseTo.tell(new StealWork(partitionManagers.get(targetNodeId), command.requestingManager));
    }

    private void sendNextJoinPair(ActorRef<ADBPartitionManager.Command> left,
                                  ActorRef<ADBPartitionManager.Command> right,
                                  ActorRef<ADBPartitionManager.Command> executor,
                                  ActorRef<Response> respondTo) {
        respondTo.tell(NextJoinNodePair.builder()
                                       .hasNode(true)
                                       .requestingPartitionManager(executor)
                                       .leftQueryManager(left)
                                       .rightQueryManager(right)
                                       .build());
    }

    private int sortJoinTasksByExpectedDelayUntilStart(
            ADBPair<ActorRef<ADBPartitionManager.Command>, ActorRef<ADBPartitionManager.Command>> joinTaskA,
            ADBPair<ActorRef<ADBPartitionManager.Command>, ActorRef<ADBPartitionManager.Command>> joinTaskB) {
        return Float.compare(this.getExpectedDelayUntilStart(joinTaskB), this.getExpectedDelayUntilStart(joinTaskA));
    }

    private float getExpectedDelayUntilStart(ADBPair<ActorRef<ADBPartitionManager.Command>,
            ActorRef<ADBPartitionManager.Command>> joinTask) {
        int nodeIdA = this.partitionManagers.indexOf(joinTask.getKey());
        int nodeIdB = this.partitionManagers.indexOf(joinTask.getValue());
        long currDurationA = this.history.getCurrentJoinDuration(nodeIdA).orElse(0L);
        float averageDurationA = this.history.getAverageNodeJoinExecutionTime(nodeIdA);
        long currDurationB = this.history.getCurrentJoinDuration(nodeIdB).orElse(0L);
        float averageDurationB = this.history.getAverageNodeJoinExecutionTime(nodeIdB);
        return Math.min(Math.max(0f, currDurationA - averageDurationA), Math.max(0f, currDurationB - averageDurationB));
    }


}
