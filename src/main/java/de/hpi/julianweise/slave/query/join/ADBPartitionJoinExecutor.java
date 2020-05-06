package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.attribute_comparison.strategies.ADBOffsetAttributeComparisonStrategy;
import de.hpi.julianweise.slave.query.join.attribute_comparison.strategies.ADBPrimitiveAttributeComparisonStrategy;
import de.hpi.julianweise.slave.query.join.column.intersect.ADBJoinCandidateIntersector;
import de.hpi.julianweise.slave.query.join.column.intersect.ADBJoinCandidateIntersectorFactory;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.slave.worker_pool.workload.JoinQueryWorkload;
import de.hpi.julianweise.slave.worker_pool.workload.Workload;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBPartitionJoinExecutor extends AbstractBehavior<ADBPartitionJoinExecutor.Command> {

    private final Map<String, List<ADBPair<Comparable<Object>, Integer>>> foreignAttributes;
    private final ActorRef<ADBJoinCandidateIntersector.Command> intersector;
    private final ActorRef<PartitionsJoined> supervisor;
    private final AtomicInteger receivedResults = new AtomicInteger(0);
    private final ADBJoinQuery query;
    private final int localPartitionId;
    private final int foreignPartitionId;
    private final boolean reversed;

    public interface Command {
    }

    public interface Response {
    }

    @AllArgsConstructor
    @Getter
    public static class IntersectorResultWrapper implements Command {
        ADBJoinCandidateIntersector.Results results;
    }

    @AllArgsConstructor
    @Getter
    public static class PartitionJoinAttributesWrapper implements Command {
        ADBPartition.JoinAttributes response;
    }

    @AllArgsConstructor
    public static class JoinResponseWrapper implements Command {
        GenericWorker.Response response;
    }

    @AllArgsConstructor
    @Builder
    @Getter
    public static class PartitionsJoined implements Response {
        private final boolean reversed;
        private final int localPartitionId;
        private final int foreignPartitionId;
        private final List<ADBKeyPair> joinTuples;
    }

    public ADBPartitionJoinExecutor(ActorContext<Command> context,
                                    ADBJoinQuery query,
                                    ActorRef<ADBPartition.Command> localPartition,
                                    Map<String, List<ADBPair<Comparable<Object>, Integer>>> foreignAttributes,
                                    int localPartitionId,
                                    int foreignPartitionId,
                                    ActorRef<PartitionsJoined> supervisor,
                                    boolean reversed) {
        super(context);
        this.query = query;
        this.supervisor = supervisor;
        this.localPartitionId = localPartitionId;
        this.foreignPartitionId = foreignPartitionId;
        this.reversed = reversed;
        this.foreignAttributes = foreignAttributes;
        this.intersector = this.spawnIntersector();

        val resTo = getContext().messageAdapter(ADBPartition.JoinAttributes.class, PartitionJoinAttributesWrapper::new);
        localPartition.tell(new ADBPartition.RequestJoinAttributes(resTo, this.query, localPartitionId));
    }

    private ActorRef<ADBJoinCandidateIntersector.Command> spawnIntersector() {
        String name = ADBJoinCandidateIntersectorFactory.getName(this.query, this.localPartitionId);
        return this.getContext().spawn(ADBJoinCandidateIntersectorFactory.createDefault(), name);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PartitionJoinAttributesWrapper.class, this::handlePartitionJoinAttributes)
                .onMessage(JoinResponseWrapper.class, this::handleJoinResponse)
                .onMessage(IntersectorResultWrapper.class, this::handleIntersectorResults)
                .build();
    }

    private Behavior<Command> handlePartitionJoinAttributes(PartitionJoinAttributesWrapper wrapper) {
        this.join(wrapper.getResponse().getAttributes());
        return Behaviors.same();
    }

    private void join(Map<String, List<ADBPair<Comparable<Object>, Integer>>> localAttributeValues) {
        for (ADBJoinQueryTerm term : this.query.getTerms()) {
            val resultRef = this.getContext().messageAdapter(GenericWorker.Response.class, JoinResponseWrapper::new);
            Workload workload = JoinQueryWorkload
                    .builder()
                    .operator(term.getOperator())
                    .leftSideValues(this.foreignAttributes.get(term.getLeftHandSideAttribute()))
                    .rightSideValues(localAttributeValues.get(term.getRightHandSideAttribute()))
                    .strategy(new ADBOffsetAttributeComparisonStrategy())
                    .foreignPartitionId(this.foreignPartitionId)
                    .build();

            ADBQueryManager.getWorkerPool().tell(new GenericWorker.WorkloadMessage(resultRef, workload));
        }
    }

    private Behavior<Command> handleJoinResponse(JoinResponseWrapper wrapper) {
        this.receivedResults.incrementAndGet();
        JoinQueryWorkload.Results joinResults = (JoinQueryWorkload.Results) wrapper.response;
        this.intersector.tell(new ADBJoinCandidateIntersector.Intersect(joinResults.getResults()));
        if (this.receivedResults.get() == this.query.getTerms().size()) {
            val respondTo = getContext().messageAdapter(ADBJoinCandidateIntersector.Results.class, IntersectorResultWrapper::new);
            this.intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(respondTo));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleIntersectorResults(IntersectorResultWrapper result) {
        this.supervisor.tell(PartitionsJoined.builder()
                                             .foreignPartitionId(this.foreignPartitionId)
                                             .reversed(this.reversed)
                                             .localPartitionId(this.localPartitionId)
                                             .joinTuples(result.getResults().getCandidates())
                                             .build());
        return Behaviors.stopped();
    }
}
