package de.hpi.julianweise.slave.query.join.steps;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.zaxxer.sparsebits.SparseBitSet;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModel;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.slave.worker_pool.workload.JoinQueryColumnWorkload;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
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
        private final ObjectList<ADBKeyPair> results;
    }

    private final Map<String, ObjectList<ADBComparable2IntPair>> left;
    private final Map<String, ObjectList<ADBComparable2IntPair>> right;
    private final ObjectList<ADBJoinTermCostModel> costModels;
    private final ActorRef<StepExecuted> respondTo;
    private final AtomicInteger intersectsPerformed = new AtomicInteger(0);
    private SparseBitSet[] resultSet;

    public ADBColumnJoinStepExecutor(ActorContext<Command> context,
                                     Map<String, ObjectList<ADBComparable2IntPair>> left,
                                     Map<String, ObjectList<ADBComparable2IntPair>> right,
                                     ObjectList<ADBJoinTermCostModel> costModels,
                                     ActorRef<StepExecuted> respondTo) {
        super(context);
        this.left = left;
        this.right = right;
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
        for (ADBJoinTermCostModel costModel : this.costModels) {
            val workload = JoinQueryColumnWorkload
                    .builder()
                    .left(this.left.get(costModel.getTerm().getLeftHandSideAttribute()))
                    .right(this.right.get(costModel.getTerm().getRightHandSideAttribute()))
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

    private ObjectList<ADBKeyPair> mapResults() {
        val leftList = this.left.get(this.costModels.get(0).getTerm().getLeftHandSideAttribute());
        int leftNodeId = ADBInternalIDHelper.getNodeId(leftList.get(0).getValue());
        int leftPartitionId = ADBInternalIDHelper.getPartitionId(leftList.get(0).getValue());
        val rightList = this.right.get(this.costModels.get(0).getTerm().getRightHandSideAttribute());
        int rightNodeId = ADBInternalIDHelper.getNodeId(rightList.get(0).getValue());
        int rightPartitionId = ADBInternalIDHelper.getPartitionId(rightList.get(0).getValue());
        ObjectList<ADBKeyPair> results = new ObjectArrayList<>(Arrays.stream(resultSet).mapToInt(SparseBitSet::cardinality).sum());
        for(int a = 0; a < this.resultSet.length; a++) {
            for (int b = this.resultSet[a].nextSetBit(0); b >= 0; b = this.resultSet[a].nextSetBit(b+1)) {
                int leftId = ADBInternalIDHelper.createID(leftNodeId, leftPartitionId, a);
                int rightId = ADBInternalIDHelper.createID(rightNodeId, rightPartitionId, b);
                results.add(new ADBKeyPair(leftId, rightId));
            }
        }
        return results;
    }
}
