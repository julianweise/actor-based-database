package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("UnstableApiUsage")
public class ADBJoinAttributeIntersector extends AbstractBehavior<ADBJoinAttributeIntersector.Command> {

    public static boolean USE_BLOOM_FILTER = true;
    private List<Pair<Integer, Integer>> joinCandidates;
    private BloomFilter<Pair<Integer, Integer>> bloomFilter;

    public interface Command {}

    public interface Result {}
    public enum JoinCandidateFunnel implements Funnel<Pair<Integer, Integer>> {
        INSTANCE;
        public void funnel(Pair<Integer, Integer> candidate, PrimitiveSink into) {
            into.putInt(candidate.getKey())
                .putInt(candidate.getValue());
        }

    }
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class Intersect implements Command {
        List<Pair<Integer, Integer>> candidates;

    }
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class ReturnResults implements Command {
        ActorRef<Result> respondTo;

    }
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class Results implements Result {
        List<Pair<Integer, Integer>> candidates;

    }

    public ADBJoinAttributeIntersector(ActorContext<Command> context, List<Pair<Integer, Integer>> initialCandidates) {
        super(context);
        this.joinCandidates = initialCandidates;
        this.joinCandidates.sort(ADBJoinAttributeIntersector::comparingJoinCandidates);
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
        // There is no performance penalty for using enhanced-for loops instead of classic for loops
        // Indeed, there might even be a small performance advantage as size of the underling array has to be
        // collected only once
        for (Pair<Integer, Integer> joinCandidate : this.joinCandidates) {
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
        List<Pair<Integer, Integer>> filteredCandidates = this.filterCandidates(command.getCandidates());
        filteredCandidates.sort(ADBJoinAttributeIntersector::comparingJoinCandidates);
        // Filtering an array by copying elements into a new list is the cheapest procedure performance-wise
        List<Pair<Integer, Integer>> resultSet = new ArrayList<>(Math.min(joinCandidates.size(), filteredCandidates.size()));

        for (int a = 0, b = 0; a < this.joinCandidates.size() && b < filteredCandidates.size(); b++) {
            Pair<Integer, Integer> lastElement = resultSet.size() > 0 ? resultSet.get(resultSet.size() - 1) : null;
            if (this.joinCandidates.get(a).equals(filteredCandidates.get(b))) {
                if (lastElement == null || !lastElement.equals(this.joinCandidates.get(a))) resultSet.add(this.joinCandidates.get(a));
                a++;
                continue;
            }
            if (ADBJoinAttributeIntersector.comparingJoinCandidates(this.joinCandidates.get(a), filteredCandidates.get(b)) < 0) {
                a++;
                b--;
            }
        }
        this.joinCandidates = resultSet;
        return Behaviors.same();
    }

    private List<Pair<Integer, Integer>> filterCandidates(List<Pair<Integer, Integer>> joinCandidates) {
        if (!USE_BLOOM_FILTER) {
            return joinCandidates;
        }
        List<Pair<Integer, Integer>> filteredCandidates = new ArrayList<>();
        for (Pair<Integer, Integer> joinCandidate : joinCandidates) {
            if (this.bloomFilter.mightContain(joinCandidate)) {
                filteredCandidates.add(joinCandidate);
            }
        }
        return filteredCandidates;
    }

    public static int comparingJoinCandidates(Pair<Integer, Integer> a, Pair<Integer, Integer> b) {
        if (a.getKey() - b.getKey() == 0) {
            return a.getValue() - b.getValue();
        }
        return a.getKey() - b.getKey();
    }

    private Behavior<Command> handleReturnResults(ReturnResults command) {
        command.getRespondTo().tell(new Results(this.joinCandidates));
        return Behaviors.same();
    }

}
