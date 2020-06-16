package de.hpi.julianweise.utility.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import it.unimi.dsi.fastutil.ints.AbstractInt2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JoinExecutionPlan extends AbstractBehavior<JoinExecutionPlan.Command> {

    public interface Command {
    }

    public interface Response {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class GetNextNodeJoinPair implements Command {
        private ActorRef<ADBPartitionManager.Command> requestingPartitionManager;
        private ActorRef<NextNodeToJoinWith> responseTo;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Getter
    public static class NextNodeToJoinWith implements Response {
        private ActorRef<ADBPartitionManager.Command> requestingPartitionManager;
        private ActorRef<ADBPartitionManager.Command> leftQueryManager;
        private ActorRef<ADBPartitionManager.Command> rightQueryManager;
        private boolean hasNode;
    }

    public static Behavior<JoinExecutionPlan.Command> createDefault(ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers) {
        return Behaviors.setup(context -> new JoinExecutionPlan(context, partitionManagers));
    }


    private final ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers;

    private final BitSet[] distributionMap;
    private final Int2ObjectMap<AtomicInteger> dataAccesses = new Int2ObjectOpenHashMap<>();
    private final Int2ObjectMap<AtomicInteger> dataRequests = new Int2ObjectOpenHashMap<>();
    private final AtomicInteger finalizedComparisons = new AtomicInteger(0);

    public JoinExecutionPlan(ActorContext<Command> context,
                             ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers) {
        super(context);
        this.partitionManagers = partitionManagers;
        this.distributionMap = this.initializeDistributionMap(this.partitionManagers.size());
        IntStream.range(0, partitionManagers.size()).forEach(index -> dataAccesses.put(index, new AtomicInteger()));
        IntStream.range(0, partitionManagers.size()).forEach(index -> dataRequests.put(index, new AtomicInteger()));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(GetNextNodeJoinPair.class, this::handleGetNextNodeJoinPair)
                .build();
    }

    private BitSet[] initializeDistributionMap(int numberOfNodes) {
        BitSet[] map = new BitSet[numberOfNodes];
        for (int i = 0; i < numberOfNodes; i++) {
            map[i] = new BitSet(numberOfNodes);
            for (int j = 0; j <= i; j++) {
                map[i].set(j);
            }
        }
        return map;
    }

    public Behavior<Command> handleGetNextNodeJoinPair(GetNextNodeJoinPair command) {
        int nodeIndex = this.getIndexOfNode(command.requestingPartitionManager);
        int nodeIndexMinimalAccesses = this.getNodeIndexWithMinimalAccessesForNode(nodeIndex);
        getContext().getLog().info(String.format("[DistributionPlan] Node #%d requested new Node to join. Suggested " +
                        "Node # %d",
                nodeIndex, nodeIndexMinimalAccesses));
        getContext().getLog().info("[Overall Process]: {}/{}", this.finalizedComparisons.incrementAndGet(),
                this.partitionManagers.size() * (this.partitionManagers.size() + 1) / 2);
        if (nodeIndexMinimalAccesses < 0) {
            command.responseTo.tell(NextNodeToJoinWith.builder().hasNode(false).requestingPartitionManager(command.requestingPartitionManager).build());
            return Behaviors.same();
        }
        this.distributionMap[nodeIndex].set(nodeIndexMinimalAccesses);
        this.distributionMap[nodeIndexMinimalAccesses].set(nodeIndex);
        this.dataAccesses.get(nodeIndex).incrementAndGet();
        this.dataAccesses.get(nodeIndexMinimalAccesses).incrementAndGet();
        this.dataRequests.get(nodeIndex).incrementAndGet();
        command.responseTo.tell(NextNodeToJoinWith.builder()
                                                  .hasNode(true)
                                                  .requestingPartitionManager(command.requestingPartitionManager)
                                                  .rightQueryManager(this.partitionManagers.get(nodeIndexMinimalAccesses))
                                                  .leftQueryManager(command.requestingPartitionManager)
                                                  .build());
        return Behaviors.same();
    }

    private int getIndexOfNode(ActorRef<ADBPartitionManager.Command> node) {
        return this.partitionManagers.indexOf(node);
    }

    private int getNodeIndexWithMinimalAccessesForNode(int index) {
        IntList relevantAccesses = this.dataAccesses
                .int2ObjectEntrySet().stream()
                .filter(eS -> this.notCompared(eS.getIntKey(), index))
                .map(eS -> eS.getValue().get())
                .collect(Collectors.toCollection(IntArrayList::new));
        int minDataAccesses = relevantAccesses.size() > 0 ? Collections.min(relevantAccesses) : Integer.MAX_VALUE;
        return this.dataAccesses.int2ObjectEntrySet()
                                .stream()
                                .sorted(Comparator.comparingInt(Int2ObjectMap.Entry::getIntKey))
                                .filter(eS -> this.notCompared(eS.getIntKey(), index))
                                .filter(eS1 -> eS1.getValue().get() <= minDataAccesses)
                                .min((eS1, eS2) -> this.dataRequests.get(eS2.getIntKey()).get() - this.dataRequests.get(eS1.getIntKey()).get())
                                .orElseGet(() -> new AbstractInt2ObjectMap.BasicEntry<>(-1, new AtomicInteger(-1)))
                                .getIntKey();
    }

    private boolean notCompared(int indexA, int indexB) {
        return !(this.distributionMap[indexA].get(indexB) && this.distributionMap[indexB].get(indexA));
    }
}
