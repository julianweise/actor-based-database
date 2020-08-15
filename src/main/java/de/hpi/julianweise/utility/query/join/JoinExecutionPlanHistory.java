package de.hpi.julianweise.utility.query.join;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.Comparator;
import java.util.Optional;
import java.util.OptionalLong;

public class JoinExecutionPlanHistory {

    private final Int2ObjectHashMap<HistoryEntry> runningNodeJoins = new Int2ObjectHashMap<>();

    @Getter
    @AllArgsConstructor
    private abstract static class HistoryEntry {
        protected final long timestamp = (long) Math.floor(System.nanoTime() * 1e-6);
        protected final int executionNodeId;

        abstract public String toString();
    }

    @Getter
    private static class NextNodeHistoryEntry extends HistoryEntry {
        protected final int leftNodeId;
        protected final int rightNodeId;

        public NextNodeHistoryEntry(int executionNodeId, int leftNodeId, int rightNodeId) {
            super(executionNodeId);
            this.leftNodeId = leftNodeId;
            this.rightNodeId = rightNodeId;
        }

        public String toString() {
            return "[" + this.getTimestamp() + "-#" + this.executionNodeId + "] #" + leftNodeId + " : " + "#" + rightNodeId;
        }
    }

    @Getter
    private static class NextWorkStealingEntry extends HistoryEntry {

        protected final int targetNodeId;

        public NextWorkStealingEntry(int executionNodeId, int targetNodeId) {
            super(executionNodeId);
            this.targetNodeId = targetNodeId;
        }

        public String toString() {
            return String.format("[%s-#%d] Stealing from #%d", this.getTimestamp(), executionNodeId, targetNodeId);
        }
    }

    @Getter
    private static class FinalizedNodeJoinHistoryEntry extends HistoryEntry {

        public FinalizedNodeJoinHistoryEntry(int executionNodeId) {
            super(executionNodeId);
        }

        public String toString() {
            return "[" + this.getTimestamp() + "-#" + this.executionNodeId + "]";
        }
    }

    private final ObjectList<HistoryEntry> history = new ObjectArrayList<>();
    private final long startTime;

    public JoinExecutionPlanHistory() {
        this.startTime = (long) Math.floor(System.nanoTime() * 1e-6);
    }

    public void logNodeJoin(int executionNodeId, int leftNodeId, int rightNodeId) {
        NextNodeHistoryEntry entry = new NextNodeHistoryEntry(leftNodeId, rightNodeId, executionNodeId);
        this.runningNodeJoins.put(executionNodeId, entry);
        this.history.add(entry);
    }

    public void logFinalizedNodeJoin(int executionNodeId) {
        this.runningNodeJoins.remove(executionNodeId);
        this.history.add(new FinalizedNodeJoinHistoryEntry(executionNodeId));
    }

    public void logWorkStealing(int executionNodeId, int targetNodeId) {
        this.runningNodeJoins.put(executionNodeId, new NextWorkStealingEntry(targetNodeId, executionNodeId));
        this.history.add(new NextWorkStealingEntry(targetNodeId, executionNodeId));
    }

    public float getAverageNodeJoinExecutionTime(int nodeId) {
        long[] relevantEntries = this.history.stream()
                                             .filter(entry -> entry instanceof NextNodeHistoryEntry)
                                             .filter(entry -> entry.executionNodeId == nodeId)
                                             .sorted(Comparator.comparingLong(n -> n.timestamp))
                                             .mapToLong(HistoryEntry::getTimestamp)
                                             .map(timestamp -> timestamp - this.startTime)
                                             .toArray();
        long accumulatedDifferences = 0;
        for (int i = 0; i < relevantEntries.length; i++) {
            if (i == 0) {
                accumulatedDifferences = relevantEntries[i];
                continue;
            }
            accumulatedDifferences += (relevantEntries[i] - relevantEntries[i - 1]);
        }
        return (float) accumulatedDifferences / relevantEntries.length;
    }

    public Optional<Long> getCurrentJoinDuration(int nodeId) {
        long lastJoinStartTimestamp = this.history.stream()
                                                  .filter(entry -> entry instanceof NextNodeHistoryEntry)
                                                  .filter(entry -> entry.executionNodeId == nodeId)
                                                  .mapToLong(HistoryEntry::getTimestamp)
                                                  .max().orElse(0);
        OptionalLong lastJoinConclusionTimestamp = this.history
                .stream()
                .filter(entry -> entry instanceof FinalizedNodeJoinHistoryEntry)
                .filter(entry -> entry.executionNodeId == nodeId)
                .mapToLong(HistoryEntry::getTimestamp)
                .max();

        if (!lastJoinConclusionTimestamp.isPresent()) {
            return Optional.of((long) Math.ceil(System.nanoTime() * 1e-6));
        } else if (lastJoinStartTimestamp <= lastJoinConclusionTimestamp.getAsLong()) {
            return Optional.empty();
        }
        return Optional.of(lastJoinStartTimestamp - lastJoinConclusionTimestamp.getAsLong());
    }

    public int getLastNodeHandlingAJoin(int excludeNodeId) {
        if (this.runningNodeJoins.isEmpty()) {
            return -1;
        }
        Optional<HistoryEntry> lastNodeJoin = this.runningNodeJoins
                .values().stream()
                .filter(entry -> entry instanceof NextNodeHistoryEntry)
                .filter(entry -> (entry.executionNodeId != excludeNodeId))
                .filter(entry -> this.runningNodeJoins.values().stream()
                                                      .filter(entry2 -> entry2 instanceof NextWorkStealingEntry)
                                                      .noneMatch(entry2 -> entry.executionNodeId == ((NextWorkStealingEntry) entry2).targetNodeId))
                .min((e1, e2) -> Long.compare(e2.getTimestamp(), e1.getTimestamp()));
        return lastNodeJoin.map(HistoryEntry::getExecutionNodeId).orElseGet(() -> this.getLastStealingJoin(excludeNodeId).map(HistoryEntry::getExecutionNodeId).orElse(-1));
    }

    private Optional<HistoryEntry> getLastStealingJoin(int excludeNodeId) {
        return this.runningNodeJoins
                .values().stream()
                .filter(entry -> entry instanceof NextWorkStealingEntry)
                .filter(entry -> (entry.executionNodeId != excludeNodeId))
                .filter(entry -> this.runningNodeJoins.values().stream()
                                                      .filter(entry2 -> entry2 instanceof NextWorkStealingEntry)
                                                      .noneMatch(entry2 -> entry.executionNodeId == ((NextWorkStealingEntry) entry2).targetNodeId))
                .min((e1, e2) -> Long.compare(e2.getTimestamp(), e1.getTimestamp()));
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        this.history.stream()
                    .sorted(Comparator.comparingLong(e -> e.timestamp))
                    .forEach(e -> stringBuilder.append(e.toString()).append("\n"));
        return stringBuilder.toString();
    }
}
