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
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("UnstableApiUsage")
public class ADBJoinAttributeIntersector extends AbstractBehavior<ADBJoinAttributeIntersector.Command> {

    public static boolean USE_BLOOM_FILTER = false;
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
    @NoArgsConstructor
    @Getter
    public static class Intersect implements Command {
        List<ADBKeyPair> candidates;

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
        List<ADBKeyPair> candidates;
    }

    public ADBJoinAttributeIntersector(ActorContext<Command> context, List<ADBKeyPair> initialCandidates) {
        super(context);
        this.joinCandidates = initialCandidates;
        this.joinCandidates.sort(ADBJoinAttributeIntersector::comparingJoinCandidates);
        this.initializeBloomFilter();

        this.getContext().getLog().info("New Intersector has been created containing " + this.joinCandidates.size() + " candidates");
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
        this.getContext().getLog().info("Intersecting " + this.joinCandidates.size() + " candidates with " + command.getCandidates().size() + " recently received candidates");
        List<ADBKeyPair> filteredCandidates = this.filterCandidates(command.getCandidates());
        filteredCandidates.sort(ADBJoinAttributeIntersector::comparingJoinCandidates);
        // Filtering an array by copying elements into a new list is the cheapest procedure performance-wise
        List<ADBKeyPair> resultSet = new ArrayList<>(Math.min(joinCandidates.size(), filteredCandidates.size()));

        for (int a = 0, b = 0; a < this.joinCandidates.size() && b < filteredCandidates.size(); b++) {
            ADBKeyPair lastElement = resultSet.size() > 0 ? resultSet.get(resultSet.size() - 1) : null;
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
        this.getContext().getLog().info(resultSet.size() + " candidates remain");
        this.joinCandidates = resultSet;
        return Behaviors.same();
    }

    private List<ADBKeyPair> filterCandidates(List<ADBKeyPair> joinCandidates) {
        if (!USE_BLOOM_FILTER) {
            return joinCandidates;
        }
        List<ADBKeyPair> filteredCandidates = new ArrayList<>();
        for (ADBKeyPair joinCandidate : joinCandidates) {
            if (this.bloomFilter.mightContain(joinCandidate)) {
                filteredCandidates.add(joinCandidate);
            }
        }
        return filteredCandidates;
    }

    public static int comparingJoinCandidates(ADBKeyPair a, ADBKeyPair b) {
        int keyComparison = a.getKey() - b.getKey();
        if (keyComparison == 0) {
            return a.getValue() - b.getValue();
        }
        return keyComparison;
    }

    private Behavior<Command> handleReturnResults(ReturnResults command) {
        this.getContext().getLog().info("Intersecting returned " + this.joinCandidates.size() + " remaining " +
            "candidates");
        command.getRespondTo().tell(new Results(this.joinCandidates));
        return Behaviors.same();
    }

}
