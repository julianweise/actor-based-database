package de.hpi.julianweise.utility.query.join;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Comparator;
import java.util.Optional;
import java.util.OptionalLong;

public class JoinExecutionPlanHistory {

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
        this.history.add(new NextNodeHistoryEntry(leftNodeId, rightNodeId, executionNodeId));
    }

    public void logFinalizedNodeJoin(int executionNodeId) {
        this.history.add(new FinalizedNodeJoinHistoryEntry(executionNodeId));
    }

    public void logWorkStealing(int executionNodeId, int targetNodeId) {
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
        Optional<HistoryEntry> lastNodeJoin = this.history
                .stream()
                .filter(entry -> entry instanceof NextNodeHistoryEntry)
                .filter(entry -> ((NextNodeHistoryEntry) entry).executionNodeId != excludeNodeId)
                .filter(entry -> this.history.stream()
                                             .filter(entry2 -> entry2 instanceof NextWorkStealingEntry)
                                             .noneMatch(entry2 -> ((NextWorkStealingEntry) entry2).targetNodeId == ((NextNodeHistoryEntry) entry).executionNodeId))
                .filter(entry -> this.history.stream()
                                             .filter(entry2 -> entry2 instanceof NextWorkStealingEntry)
                                             .noneMatch(entry2 -> ((NextWorkStealingEntry) entry2).executionNodeId == ((NextNodeHistoryEntry) entry).executionNodeId && ((NextWorkStealingEntry) entry2).targetNodeId == excludeNodeId))

                .max(Comparator.comparingLong(e -> e.timestamp));
        return lastNodeJoin.map(historyEntry -> ((NextNodeHistoryEntry) historyEntry).executionNodeId).orElse(-1);
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        this.history.stream()
                    .sorted(Comparator.comparingLong(e -> e.timestamp))
                    .forEach(e -> stringBuilder.append(e.toString()).append("\n"));
        return stringBuilder.toString();
    }
}
