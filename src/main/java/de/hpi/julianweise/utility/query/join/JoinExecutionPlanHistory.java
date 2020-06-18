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
        private final int nodeId;
        private final int joinNode;

        public String toString() {
            return "[" + this.getTimestamp() + "] #" + this.nodeId + ":" + "#" + this.joinNode;
        }
    }

    @AllArgsConstructor
    @Getter
    private static class FinalizedNodeJoinHistoryEntry extends HistoryEntry {
        private final int nodeId;

        public String toString() {
            return "[" + this.getTimestamp() + "] #" + this.nodeId;
        }
    }

    private final ObjectList<HistoryEntry> history = new ObjectArrayList<>();
    private final int transactionId;
    private final long startTime;

    public JoinExecutionPlanHistory(int transactionId) {
        this.startTime = (long) Math.floor(System.nanoTime() * 1e-6);
        this.transactionId = transactionId;
    }

    public void logNodeJoin(int nodeId, int joinNode) {
        this.history.add(new NextNodeHistoryEntry(nodeId, joinNode));
    }

    public void logFinalizedNodeJoin(int nodeId) {
        this.history.add(new FinalizedNodeJoinHistoryEntry(nodeId));
    }

    public float getAverageNodeJoinExecutionTime(int nodeId) {
        long[] relevantEntries = this.history.stream()
                                             .filter(entry -> entry instanceof NextNodeHistoryEntry)
                                             .filter(entry -> ((NextNodeHistoryEntry) entry).nodeId == nodeId)
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
                                                  .filter(entry -> ((NextNodeHistoryEntry) entry).nodeId == nodeId)
                                                  .mapToLong(HistoryEntry::getTimestamp)
                                                  .max().orElse(0);
        OptionalLong lastJoinConclusionTimestamp = this.history.stream()
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

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        this.history.stream()
                    .sorted(Comparator.comparingLong(e -> e.timestamp))
                    .forEach(e -> stringBuilder.append(e.toString()).append("\n"));
        return stringBuilder.toString();
    }
}
