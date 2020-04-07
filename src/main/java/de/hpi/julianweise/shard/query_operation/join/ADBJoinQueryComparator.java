package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ADBJoinQueryComparator extends AbstractBehavior<ADBJoinQueryComparator.Command> {

    private final Map<String, List<ADBJoinQueryTerm>> groupedQueryTerms;
    private final Map<String, ADBSortedEntityAttributes> localSortedAttributes;
    private final ActorRef<ADBJoinAttributeComparator.Command> comparatorPool;
    private final ActorRef<ADBJoinWithShardSession.Command> supervisor;
    private final ActorRef<ADBJoinAttributeIntersector.Result> intersectorResultWrapper;
    private ActorRef<ADBJoinAttributeIntersector.Command> intersector;


    public interface Command {}

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class ProcessForeignAttributes implements Command {
        private String attributeName;
        private List<ADBPair<Comparable<?>, Integer>> attributeValues;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class CompareTermResults implements Command {
        private List<ADBKeyPair> candidates;
        private ADBJoinQueryTerm term;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class IntersectorResultWrapper implements Command {
        ADBJoinAttributeIntersector.Result result;
    }

    public ADBJoinQueryComparator(ActorContext<Command> context, ADBJoinQuery query,
                                  Map<String, ADBSortedEntityAttributes> localSortedAttributes,
                                  ActorRef<ADBJoinAttributeComparator.Command> comparatorPool,
                                  ActorRef<ADBJoinWithShardSession.Command> supervisor) {
        super(context);
        this.groupedQueryTerms = this.groupQueryTermsByLeftHandSide(query);
        this.localSortedAttributes = localSortedAttributes;
        this.comparatorPool = comparatorPool;
        this.supervisor = supervisor;
        this.intersectorResultWrapper = this.getContext().messageAdapter(ADBJoinAttributeIntersector.Result.class, IntersectorResultWrapper::new);
    }

    private Map<String, List<ADBJoinQueryTerm>> groupQueryTermsByLeftHandSide(ADBJoinQuery query) {
        return query.getTerms().stream().collect(Collectors.groupingBy(ADBJoinQueryTerm::getLeftHandSideAttribute));
    }


    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ProcessForeignAttributes.class, this::handleProcessForeignAttributes)
                .onMessage(CompareTermResults.class, this::handleCompareTermResults)
                .onMessage(IntersectorResultWrapper.class, this::handleIntersectorResults)
                .build();
    }

    private Behavior<Command> handleProcessForeignAttributes(ProcessForeignAttributes command) {
        this.getContext().getLog().info("Comparing " + command.attributeName + " attributes");
        if (!this.groupedQueryTerms.containsKey(command.attributeName)) {
            return Behaviors.same();
        }
        for(ADBJoinQueryTerm term : this.groupedQueryTerms.get(command.attributeName)) {
            this.getContext().spawn(ADBJoinTermComparatorFactory.createDefault(term, this.getContext().getSelf(),
                    this.comparatorPool), "JoinTermComparatorFor-" + term.hashCode())
                .tell(new ADBJoinTermComparator.CompareTerm(
                        command.attributeValues,
                        this.localSortedAttributes.get(term.getRightHandSideAttribute()).getAllWithOriginalIndex()));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleCompareTermResults(CompareTermResults results) {
        this.logRetrieval(results.getTerm());
        if (this.intersector == null) {
            this.getContext().getLog().info("Received " + results.candidates.size() + " results from term " +
                    results.getTerm() + " for setting up intersector");
            this.intersector = this.getContext().spawn(ADBJoinAttributeIntersectorFactory
                            .createDefault(results.candidates), "intersector");
        } else {
            intersector.tell(new ADBJoinAttributeIntersector.Intersect(results.candidates));
            this.getContext().getLog().info("Received " + results.candidates.size() + " results from term " +
                    results.getTerm() + " for matching against already existing intersector.");
        }
        if (this.groupedQueryTerms.size() < 1) {
            this.intersector.tell(new ADBJoinAttributeIntersector.ReturnResults(this.intersectorResultWrapper));
        }
        return Behaviors.same();
    }

    private void logRetrieval(ADBJoinQueryTerm term) {
        this.groupedQueryTerms.get(term.getLeftHandSideAttribute()).remove(term);
        if (this.groupedQueryTerms.get(term.getLeftHandSideAttribute()).size() < 1) {
            this.groupedQueryTerms.remove(term.getLeftHandSideAttribute());
        }
    }

    private Behavior<Command> handleIntersectorResults(IntersectorResultWrapper result) {
        if (result.result instanceof ADBJoinAttributeIntersector.Results) {
            List<ADBKeyPair> results = ((ADBJoinAttributeIntersector.Results) result.getResult()).getCandidates();
            this.supervisor.tell(new ADBJoinWithShardSessionHandler.ForeignAttributesCompared(results,
                    this.getContext().getSelf()));
        }
        return Behaviors.stopped();
    }
}
