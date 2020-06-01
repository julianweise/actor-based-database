package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.google.common.primitives.Floats;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.meta.ADBSortedEntityAttributesFactory;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModel;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModelFactory;
import de.hpi.julianweise.slave.query.join.node.ADBPartitionJoinTask;
import de.hpi.julianweise.slave.query.join.steps.ADBColumnJoinStepExecutor;
import de.hpi.julianweise.slave.query.join.steps.ADBColumnJoinStepExecutorFactory;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.slave.worker_pool.workload.JoinQueryRowWorkload;
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
    private final ADBPartitionJoinTask joinTask;

    private Map<String, ObjectList<ADBEntityEntry>> localAttributes;
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

    public ADBPartitionJoinExecutor(ActorContext<Command> context, ADBPartitionJoinTask joinTask) {
        super(context);
        this.joinTask = joinTask;

        ADBQueryPerformanceSampler.log(true, "ADBPartitionJoinExecutor", "start", this.hashCode());

        val resTo = getContext().messageAdapter(ADBPartition.JoinAttributes.class, PartitionJoinAttributesWrapper::new);
        joinTask.getLocalPartition().tell(new ADBPartition.RequestJoinAttributes(resTo, joinTask.getQuery()));
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
        ADBJoinQuery  query = this.joinTask.getQuery();
        ObjectList<ADBJoinTermCostModel> costModels = new ObjectArrayList<>(query.getPredicates().size());
        for (int i = 0; i < query.getPredicates().size(); i++) {
            ADBJoinQueryPredicate predicate = query.getPredicates().get(i);
            val left = this.joinTask.getForeignAttributes().get(predicate.getLeftHandSideAttribute());
            val right = this.localAttributes.get(predicate.getRightHandSideAttribute());
            costModels.add(ADBJoinTermCostModelFactory.calc(predicate, i, left, right));
        }
        costModels.sort((m1, m2) -> Floats.compare(m1.getRelativeCost(), m2.getRelativeCost()));
        return costModels;
    }

    private Behavior<Command> execute() {
        this.getContext().getLog().debug("[JOIN COST MODEL] Cheapest predicate: " + this.costModels.get(0));
        Map<String, ObjectList<ADBEntityEntry>> foreignAttributes = this.joinTask.getForeignAttributes();
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
        val leftAttributes = ADBSortedEntityAttributesFactory.resortByIndex(joinTask.getForeignAttributes(), costModels);
        val rightAttributes = ADBSortedEntityAttributesFactory.resortByIndex(this.localAttributes, costModels);
        val workload = new JoinQueryRowWorkload(joinCandidates, leftAttributes, rightAttributes, costModels);
        val respondTo = getContext().messageAdapter(GenericWorker.Response.class, GenericWorkerResponseWrapper::new);
        ADBQueryManager.getWorkerPool().tell(new GenericWorker.WorkloadMessage(respondTo, workload));
    }

    private void joinColumnBased(ObjectList<ADBJoinTermCostModel> costModels) {
        val respondTo = getContext().messageAdapter(ADBColumnJoinStepExecutor.StepExecuted.class,
                ADBColumnJoinExecutorWrapper::new);
        this.getContext().spawn(ADBColumnJoinStepExecutorFactory
                .createDefault(joinTask.getForeignAttributes(), this.localAttributes, costModels, respondTo), "ColumnJoinStep")
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
        this.joinTask.getRespondTo().tell(new PartitionsJoined(joinTask.isReversed(), results));
        ADBQueryPerformanceSampler.log(false, "ADBPartitionJoinExecutor", "stop", this.hashCode());
        return Behaviors.stopped();
    }
}
