package de.hpi.julianweise.slave.query.join.steps;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.zaxxer.sparsebits.SparseBitSet;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.ADBPartialJoinResult;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModel;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.slave.worker_pool.workload.JoinQueryColumnWorkload;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBColumnJoinStepExecutor extends AbstractBehavior<ADBColumnJoinStepExecutor.Command> {

    public interface Command {
    }

    public interface Response {}

    @AllArgsConstructor
    public static class Execute implements Command {
    }

    @AllArgsConstructor
    private static class GenericWorkerResponseWrapper implements Command {
        private final GenericWorker.Response result;
    }

    @AllArgsConstructor
    @Getter
    public static class StepExecuted implements Response {
        private final ADBPartialJoinResult results;
    }

    private final Map<String, ObjectList<ADBEntityEntry>> left;
    private final Map<String, ObjectList<ADBEntityEntry>> right;
    private final Object2IntMap<String> leftOriginalSizes;
    private final Object2IntMap<String> rightOriginalSies;
    private final ObjectList<ADBJoinPredicateCostModel> costModels;
    private final ActorRef<StepExecuted> respondTo;
    private final AtomicInteger intersectsPerformed = new AtomicInteger(0);
    private SparseBitSet[] resultSet;

    public ADBColumnJoinStepExecutor(ActorContext<Command> context,
                                     Map<String, ObjectList<ADBEntityEntry>> left,
                                     Map<String, ObjectList<ADBEntityEntry>> right,
                                     Object2IntMap<String> leftOriginalSizes,
                                     Object2IntMap<String> rightOriginalSizes,
                                     ObjectList<ADBJoinPredicateCostModel> costModels,
                                     ActorRef<StepExecuted> respondTo) {
        super(context);
        this.left = left;
        this.right = right;
        this.leftOriginalSizes = leftOriginalSizes;
        this.rightOriginalSies = rightOriginalSizes;
        this.costModels = costModels;
        this.respondTo = respondTo;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Execute.class, this::handleExecute)
                .onMessage(GenericWorkerResponseWrapper.class, this::handleWorkerResponse)
                .build();
    }

    private Behavior<Command> handleExecute(Execute command) {
        val respondTo = getContext().messageAdapter(GenericWorker.Response.class, GenericWorkerResponseWrapper::new);
        for (ADBJoinPredicateCostModel costModel : this.costModels) {
            val workload = JoinQueryColumnWorkload
                    .builder()
                    .left(this.left.get(costModel.getPredicate().getLeftHandSideAttribute()))
                    .right(this.right.get(costModel.getPredicate().getRightHandSideAttribute()))
                    .leftOriginalSize(this.leftOriginalSizes.getInt(costModel.getPredicate().getLeftHandSideAttribute()))
                    .rightOriginalSize(this.rightOriginalSies.getInt(costModel.getPredicate().getRightHandSideAttribute()))
                    .costModel(costModel)
                    .build();
            ADBQueryManager.getWorkerPool().tell(new GenericWorker.WorkloadMessage(respondTo, workload));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleWorkerResponse(GenericWorkerResponseWrapper wrapper) {
        if (wrapper.result instanceof JoinQueryColumnWorkload.Results) {
            this.intersectsPerformed.getAndIncrement();
            if (this.resultSet == null) {
                this.resultSet = ((JoinQueryColumnWorkload.Results) wrapper.result).getResults();
                return Behaviors.same();
            }
            this.intersect(((JoinQueryColumnWorkload.Results) wrapper.result).getResults());
            if (this.intersectsPerformed.get() == this.costModels.size()) {
                this.respondTo.tell(new StepExecuted(this.mapResults()));
                return Behaviors.stopped();
            }
        }
        return Behaviors.same();
    }

    private void intersect(SparseBitSet[] b) {
        assert this.resultSet != null : "ResultSet (BitMatrix) has to be initialized fist (should not be null)";
        assert this.resultSet.length == b.length : "BitSetMatrices for intersections have to be of equal length";
        for(int i = 0; i < this.resultSet.length; i++) {
            this.resultSet[i].and(b[i]);
        }
    }

    private ADBPartialJoinResult mapResults() {
        val leftList = this.left.get(this.costModels.get(0).getPredicate().getLeftHandSideAttribute());
        int leftNodeId = ADBInternalIDHelper.getNodeId(leftList.get(0).getId());
        int leftPartitionId = ADBInternalIDHelper.getPartitionId(leftList.get(0).getId());
        val rightList = this.right.get(this.costModels.get(0).getPredicate().getRightHandSideAttribute());
        int rightNodeId = ADBInternalIDHelper.getNodeId(rightList.get(0).getId());
        int rightPartitionId = ADBInternalIDHelper.getPartitionId(rightList.get(0).getId());
        ADBPartialJoinResult results = new ADBPartialJoinResult(Arrays.stream(resultSet).mapToInt(SparseBitSet::cardinality).sum());
        for(int a = 0; a < this.resultSet.length; a++) {
            for (int b = this.resultSet[a].nextSetBit(0); b >= 0; b = this.resultSet[a].nextSetBit(b+1)) {
                int leftId = ADBInternalIDHelper.createID(leftNodeId, leftPartitionId, a);
                int rightId = ADBInternalIDHelper.createID(rightNodeId, rightPartitionId, b);
                results.addResult(leftId, rightId);
            }
        }
        return results;
    }
}
