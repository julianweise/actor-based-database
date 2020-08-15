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

    private Int2ObjectHashMap<NextNodeHistoryEntry> runningNodeJoins = new Int2ObjectHashMap<>();

    @Getter
    private abstract static class HistoryEntry {
        private final long timestamp = (long) Math.floor(System.nanoTime() * 1e-6);

        abstract public String toString();
    }

    @AllArgsConstructor
    @Getter
    private static class NextNodeHistoryEntry extends HistoryEntry {
        private final int leftNodeId;
        private final int rightNodeId;
        private final int executionNodeId;

        public String toString() {
            return "[" + this.getTimestamp() + "-#" + executionNodeId + "] #" + leftNodeId + " : " + "#" + rightNodeId;
        }
    }

    @AllArgsConstructor
    @Getter
    private static class NextWorkStealingEntry extends HistoryEntry {
        private final int targetNodeId;
        private final int executionNodeId;

        public String toString() {
            return String.format("[%s-#%d] Stealing from #%d", this.getTimestamp(), executionNodeId, targetNodeId);
        }
    }

    @AllArgsConstructor
    @Getter
    private static class FinalizedNodeJoinHistoryEntry extends HistoryEntry {
        private final int nodeId;

        public String toString() {
            return "[" + this.getTimestamp() + "-#" + this.nodeId + "]";
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
        this.runningNodeJoins.put(executionNodeId, new NextNodeHistoryEntry(targetNodeId, targetNodeId,
                executionNodeId));
        this.history.add(new NextWorkStealingEntry(targetNodeId, executionNodeId));
    }

    public float getAverageNodeJoinExecutionTime(int nodeId) {
        long[] relevantEntries = this.history.stream()
                                             .filter(entry -> entry instanceof NextNodeHistoryEntry)
                                             .filter(entry -> ((NextNodeHistoryEntry) entry).executionNodeId == nodeId)
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
                                                  .filter(entry -> ((NextNodeHistoryEntry) entry).executionNodeId == nodeId)
                                                  .mapToLong(HistoryEntry::getTimestamp)
                                                  .max().orElse(0);
        OptionalLong lastJoinConclusionTimestamp = this.history
                .stream()
                .filter(entry -> entry instanceof FinalizedNodeJoinHistoryEntry)
                .filter(entry -> ((FinalizedNodeJoinHistoryEntry) entry).nodeId == nodeId)
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
        Optional<NextNodeHistoryEntry> lastNodeJoin = this.runningNodeJoins.values().stream()
                .filter(entry -> (entry.executionNodeId != excludeNodeId))
                .sorted((e1, e2) -> Long.compare(e2.getTimestamp(), e1.getTimestamp())) // sort descending
                .filter(entry -> this.history.stream()
                                             .filter(entry2 -> entry2 instanceof NextWorkStealingEntry)
                                             .filter(entry2 -> ((NextWorkStealingEntry) entry2).targetNodeId == entry.executionNodeId)
                                            .count() <= 2)
                .filter(entry -> this.history.stream()
                                             .filter(entry2 -> entry2 instanceof NextWorkStealingEntry)
                                             .noneMatch(entry2 -> ((NextWorkStealingEntry) entry2).executionNodeId == entry.executionNodeId && ((NextWorkStealingEntry) entry2).targetNodeId == excludeNodeId))
                .min((e1, e2) -> Long.compare(this.history.stream()
                                             .filter(entry2 -> entry2 instanceof NextWorkStealingEntry)
                                             .filter(entry2 -> ((NextWorkStealingEntry) entry2).targetNodeId == e1.executionNodeId)
                                             .count(), this.history.stream()
                                                                   .filter(entry2 -> entry2 instanceof NextWorkStealingEntry)
                                                                   .filter(entry2 -> ((NextWorkStealingEntry) entry2).targetNodeId == e2.executionNodeId).count()));
        return lastNodeJoin.map(historyEntry -> (historyEntry).executionNodeId).orElse(-1);
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        this.history.stream()
                    .sorted(Comparator.comparingLong(e -> e.timestamp))
                    .forEach(e -> stringBuilder.append(e.toString()).append("\n"));
        return stringBuilder.toString();
    }
}
