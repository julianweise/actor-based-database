package de.hpi.julianweise.slave.query.join.column.intersect;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("UnstableApiUsage")
public class ADBJoinCandidateIntersector extends AbstractBehavior<ADBJoinCandidateIntersector.Command> {

    public static final boolean USE_BLOOM_FILTER = false;
    private List<ADBKeyPair> joinCandidates;
    private BloomFilter<ADBKeyPair> bloomFilter;

    public interface Command {}

    public interface Result {}
    public enum JoinCandidateFunnel implements Funnel<ADBKeyPair> {
        INSTANCE;
        public void funnel(ADBKeyPair candidate, PrimitiveSink into) {
            into.putInt(candidate.getKey())
                .putInt(candidate.getValue());
        }

    }
    @AllArgsConstructor
    public static class Intersect implements Command {
        List<ADBKeyPair> candidates;
    }

    @AllArgsConstructor
    public static class ReturnResults implements Command {
        private final ActorRef<Results> respondTo;
    }

    @AllArgsConstructor
    @Getter
    public static class Results implements Result {
        ActorRef<Command> intersector;
        List<ADBKeyPair> candidates;
    }

    public ADBJoinCandidateIntersector(ActorContext<Command> context) {
        super(context);
        this.initializeBloomFilter();
    }

    private void initializeBloomFilter() {
        if (!USE_BLOOM_FILTER) {
            return;
        }
        this.bloomFilter = BloomFilter.create(
                JoinCandidateFunnel.INSTANCE,
                this.joinCandidates.size(),
                0.01);
        for (ADBKeyPair joinCandidate : this.joinCandidates) {
            this.bloomFilter.put(joinCandidate);
        }
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Intersect.class, this::handleIntersect)
                .onMessage(ReturnResults.class, this::handleReturnResults)
                .build();
    }

    private Behavior<Command> handleIntersect(Intersect command) {
        command.candidates.sort(ADBJoinCandidateIntersector::comparingJoinCandidates);
        if (this.joinCandidates == null) {
            this.joinCandidates = command.candidates;
            return Behaviors.same();
        }
        this.intersect(command.candidates);
        return Behaviors.same();
    }

    private void intersect(List<ADBKeyPair> candidates) {
        // Filtering an array by copying elements into a new list is the cheapest procedure performance-wise
        List<ADBKeyPair> resultSet = new ArrayList<>(Math.min(joinCandidates.size(), candidates.size()));

        for (int a = 0, b = 0; a < this.joinCandidates.size() && b < candidates.size(); b++) {
            ADBKeyPair lastElement = resultSet.size() > 0 ? resultSet.get(resultSet.size() - 1) : null;
            if (this.joinCandidates.get(a).equals(candidates.get(b))) {
                if (lastElement == null || !lastElement.equals(this.joinCandidates.get(a))) resultSet.add(this.joinCandidates.get(a));
                a++;
                continue;
            }
            if (ADBJoinCandidateIntersector.comparingJoinCandidates(this.joinCandidates.get(a), candidates.get(b)) < 0) {
                a++;
                b--;
            }
        }
        this.joinCandidates = resultSet;
    }

    private ADBKeyPair[] filterCandidates(List<ADBKeyPair> newJoinCandidates) {
        if (!USE_BLOOM_FILTER) {
            return newJoinCandidates.toArray(new ADBKeyPair[0]);
        }
        int resultPointer = 0;
        ADBKeyPair[] filtered = new ADBKeyPair[Math.min(newJoinCandidates.size(), this.joinCandidates.size())];
        for (ADBKeyPair joinCandidate : newJoinCandidates) {
            if (this.bloomFilter.mightContain(joinCandidate)) {
                filtered[resultPointer++] = joinCandidate;
            }
        }
        return filtered;
    }

    public static int comparingJoinCandidates(ADBKeyPair a, ADBKeyPair b) {
        if (a.getKey() - b.getKey() == 0) return a.getValue() - b.getValue();
        return a.getKey() - b.getKey();
    }

    private Behavior<Command> handleReturnResults(ReturnResults command) {
        command.respondTo.tell(new Results(this.getContext().getSelf(), this.joinCandidates));
        return Behaviors.stopped();
    }
}
