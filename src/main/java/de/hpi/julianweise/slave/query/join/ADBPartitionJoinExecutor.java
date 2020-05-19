package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.google.common.primitives.Floats;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.meta.ADBSortedEntityAttributes2Factory;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModel;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModelFactory;
import de.hpi.julianweise.slave.query.join.steps.ADBColumnJoinStepExecutor;
import de.hpi.julianweise.slave.query.join.steps.ADBColumnJoinStepExecutorFactory;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.slave.worker_pool.workload.JoinQueryRowWorkload;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.util.Map;

public class ADBPartitionJoinExecutor extends AbstractBehavior<ADBPartitionJoinExecutor.Command> {

    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final Map<String, ObjectList<ADBComparable2IntPair>> foreignAttributes;

    private final ActorRef<PartitionsJoined> supervisor;
    private final ADBJoinQuery query;
    private final boolean reversed;

    private Map<String, ObjectList<ADBComparable2IntPair>> localAttributes;
    private ObjectList<ADBJoinTermCostModel> costModels;
    private int costModelsProcessed = 0;

    public interface Command {
    }

    public interface Response {
    }

    @AllArgsConstructor
    @Getter
    public static class PartitionJoinAttributesWrapper implements Command {
        ADBPartition.JoinAttributes response;
    }

    @AllArgsConstructor
    public static class GenericWorkerResponseWrapper implements Command {
        GenericWorker.Response response;
    }

    @AllArgsConstructor
    public static class ADBColumnJoinExecutorWrapper implements Command {
        ADBColumnJoinStepExecutor.StepExecuted response;
    }

    @AllArgsConstructor
    @Builder
    @Getter
    public static class PartitionsJoined implements Response {
        private final boolean reversed;
        private final ObjectList<ADBKeyPair> joinTuples;
    }

    public ADBPartitionJoinExecutor(ActorContext<Command> context,
                                    ADBJoinQuery query,
                                    ActorRef<ADBPartition.Command> localPartition,
                                    Map<String, ObjectList<ADBComparable2IntPair>> foreignAttributes,
                                    ActorRef<PartitionsJoined> supervisor,
                                    boolean reversed) {
        super(context);
        this.query = query;
        this.supervisor = supervisor;
        this.reversed = reversed;
        this.foreignAttributes = foreignAttributes;

        val resTo = getContext().messageAdapter(ADBPartition.JoinAttributes.class, PartitionJoinAttributesWrapper::new);
        localPartition.tell(new ADBPartition.RequestJoinAttributes(resTo, this.query));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PartitionJoinAttributesWrapper.class, this::handlePartitionJoinAttributes)
                .onMessage(GenericWorkerResponseWrapper.class, this::handleGenericWorkerResponse)
                .onMessage(ADBColumnJoinExecutorWrapper.class, this::handleADBColumnJoinExecutorWrapper)
                .build();
    }

    private Behavior<Command> handlePartitionJoinAttributes(PartitionJoinAttributesWrapper wrapper) {
        this.localAttributes = wrapper.response.getAttributes();
        this.costModels = this.getCostModels();
        return this.execute();
    }

    private ObjectList<ADBJoinTermCostModel> getCostModels() {
        ObjectList<ADBJoinTermCostModel> costModels = new ObjectArrayList<>(this.query.getTerms().size());
        for (int i = 0; i < this.query.getTerms().size(); i++) {
            ADBJoinQueryTerm term = this.query.getTerms().get(i);
            val left = this.foreignAttributes.get(this.query.getTerms().get(i).getLeftHandSideAttribute());
            val right = this.localAttributes.get(this.query.getTerms().get(i).getRightHandSideAttribute());
            costModels.add(ADBJoinTermCostModelFactory.calc(term, i, left, right));
        }
        costModels.sort((m1, m2) -> Floats.compare(m1.getRelativeCost(), m2.getRelativeCost()));
        return costModels;
    }

    private Behavior<Command> execute() {
        if (this.costModels.size() < 2) {
            ObjectList<ADBKeyPair> results = costModels.get(0).getJoinCandidates(foreignAttributes, localAttributes);
            this.costModelsProcessed = this.costModels.size();
            return this.returnResults(results);
        }
        if (this.costModels.get(0).getRelativeCost() <= this.settings.JOIN_STRATEGY_LOWER_BOUND) {
            ObjectList<ADBKeyPair> candidates = costModels.get(0).getJoinCandidates(foreignAttributes, localAttributes);
            this.costModelsProcessed = this.costModels.size();
            this.joinRowBased(candidates, this.costModels.subList(1, this.costModels.size()));
        } else if (this.costModels.get(0).getRelativeCost() <= this.settings.JOIN_STRATEGY_UPPER_BOUND) {
            this.costModelsProcessed = this.costModels.size();
            this.joinColumnBased(this.costModels);
        } else {
            ObjectList<ADBKeyPair> candidates = costModels.get(0).getJoinCandidates(foreignAttributes, localAttributes);
            this.costModelsProcessed = this.costModels.size();
            this.joinRowBased(candidates, this.costModels.subList(1, this.costModels.size()));
        }
        return Behaviors.same();
    }

    private void joinRowBased(ObjectList<ADBKeyPair> joinCandidates, ObjectList<ADBJoinTermCostModel> costModels) {
        val leftAttributes = ADBSortedEntityAttributes2Factory.resortByIndex(this.foreignAttributes, costModels);
        val rightAttributes = ADBSortedEntityAttributes2Factory.resortByIndex(this.localAttributes, costModels);
        val workload = new JoinQueryRowWorkload(joinCandidates, leftAttributes, rightAttributes, costModels);
        val respondTo = getContext().messageAdapter(GenericWorker.Response.class, GenericWorkerResponseWrapper::new);
        ADBQueryManager.getWorkerPool().tell(new GenericWorker.WorkloadMessage(respondTo, workload));
    }

    private void joinColumnBased(ObjectList<ADBJoinTermCostModel> costModels) {
        val respondTo = getContext().messageAdapter(ADBColumnJoinStepExecutor.StepExecuted.class,
                ADBColumnJoinExecutorWrapper::new);
        this.getContext().spawn(ADBColumnJoinStepExecutorFactory
                .createDefault(this.foreignAttributes, this.localAttributes, costModels, respondTo), "ColumnJoinStep")
            .tell(new ADBColumnJoinStepExecutor.Execute());
    }

    private Behavior<Command> handleGenericWorkerResponse(GenericWorkerResponseWrapper wrapper) {
        if (wrapper.response instanceof JoinQueryRowWorkload.Results) {
            ObjectList<ADBKeyPair> results = ((JoinQueryRowWorkload.Results) wrapper.response).getResults();
            return this.returnResults(results);
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleADBColumnJoinExecutorWrapper(ADBColumnJoinExecutorWrapper wrapper) {
        ObjectList<ADBKeyPair> results = wrapper.response.getResults();
        if (this.costModelsProcessed == this.costModels.size()) {
            return this.returnResults(results);
        }
        this.joinRowBased(results, this.costModels.subList(this.costModelsProcessed, this.costModels.size()));
        this.costModelsProcessed = this.costModels.size();
        return Behaviors.same();
    }

    private Behavior<Command> returnResults(ObjectList<ADBKeyPair> results) {
        this.supervisor.tell(new PartitionsJoined(this.reversed, results));
        return Behaviors.stopped();
    }
}
