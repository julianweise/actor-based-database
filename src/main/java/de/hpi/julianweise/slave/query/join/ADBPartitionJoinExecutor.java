package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager.RedirectToPartition;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModel;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModelFactory;
import de.hpi.julianweise.slave.query.join.filter.ADBJoinPartitionFilterStrategy;
import de.hpi.julianweise.slave.query.join.filter.ADBMinMaxFilterStrategy;
import de.hpi.julianweise.slave.query.join.node.ADBPartitionJoinTask;
import de.hpi.julianweise.slave.query.join.steps.ADBColumnJoinStepExecutor;
import de.hpi.julianweise.slave.query.join.steps.ADBColumnJoinStepExecutor.StepExecuted;
import de.hpi.julianweise.slave.query.join.steps.ADBColumnJoinStepExecutorFactory;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.slave.worker_pool.workload.JoinQueryRowWorkload;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class ADBPartitionJoinExecutor extends ADBLargeMessageActor {

    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final ADBJoinQuery joinQuery;
    private final ActorRef<Response> respondTo;

    private ADBPartitionJoinTask joinTask;
    private ADBJoinPartitionFilterStrategy filter;
    private Map<String, ObjectList<ADBEntityEntry>> leftAttributes;
    private Map<String, ObjectList<ADBEntityEntry>> rightAttributes;
    private Map<String, ADBColumnSorted> leftColumns;
    private Map<String, ADBColumnSorted> rightColumns;
    private Object2IntMap<String> leftOriginalSizes;
    private Object2IntMap<String> rightOriginalSizes;
    private ObjectList<ADBJoinPredicateCostModel> costModels;
    private int costModelsProcessed = 0;

    public interface Response {
    }

    @AllArgsConstructor
    public static class Prepare implements Command {
        private final ADBPartitionJoinTask joinTask;
    }

    @AllArgsConstructor
    public static class GenericWorkerResponseWrapper implements Command {
        GenericWorker.Response response;
    }

    @AllArgsConstructor
    public static class Execute implements Command {
    }

    @AllArgsConstructor
    public static class ADBColumnJoinExecutorWrapper implements Command {
        ADBColumnJoinStepExecutor.StepExecuted response;
    }

    @AllArgsConstructor
    @Getter
    public static class JoinTaskPrepared implements Response {
        ActorRef<ADBPartitionJoinExecutor.Command> respondTo;
    }

    @AllArgsConstructor
    @Builder
    @Getter
    public static class PartitionsJoined implements Response {
        private final ADBPartialJoinResult joinTuples;
        private final ActorRef<ADBPartitionJoinExecutor.Command> respondTo;
    }

    public ADBPartitionJoinExecutor(ActorContext<Command> context, ADBJoinQuery query, ActorRef<Response> res) {
        super(context);
        this.joinQuery = query;
        this.respondTo = res;
    }

    @Override
    public Receive<Command> createReceive() {
        return super.newReceiveBuilder()
                    .onMessage(Prepare.class, this::handlePrepare)
                    .onMessage(GenericWorkerResponseWrapper.class, this::handleGenericWorkerResponse)
                    .onMessage(ADBColumnJoinExecutorWrapper.class, this::handleADBColumnJoinExecutorWrapper)
                    .onMessage(Execute.class, this::handleExecute)
                    .onMessage(ADBPartition.MultipleAttributes.class, this::handleMultipleAttributes)
                    .build();
    }

    private Behavior<Command> handlePrepare(Prepare command) {
        assert this.joinTask == null && this.isNotPrepared() : "This executor is already prepared!";
        this.joinTask = command.joinTask;
        this.filter = new ADBMinMaxFilterStrategy(this.joinTask, this.joinQuery);
        this.requestLeftSideAttributes();
        this.requestRightSideAttributes();
        return Behaviors.same();
    }

    private void requestLeftSideAttributes() {
        val attributes = this.joinQuery.getAllLeftHandSideFields();
        val leftCommand = ADBPartition.RequestMultipleAttributesFiltered
                .builder()
                .respondTo(getContext().getSelf())
                .attributes(attributes)
                .minValues(Arrays.stream(attributes).map(filter::getMinValueForLeft).toArray(ADBEntityEntry[]::new))
                .maxValues(Arrays.stream(attributes).map(filter::getMaxValueForLeft).toArray(ADBEntityEntry[]::new))
                .isLeft(true)
                .build();
        joinTask.getLeftPartitionManager().tell(new RedirectToPartition(joinTask.getLeftPartitionId(), leftCommand));
    }

    private void requestRightSideAttributes() {
        val attributes = this.joinQuery.getAllRightHandSideFields();
        val rightCommand = ADBPartition.RequestMultipleAttributesFiltered
                .builder()
                .respondTo(getContext().getSelf())
                .attributes(attributes)
                .minValues(Arrays.stream(attributes).map(filter::getMinValueForRight).toArray(ADBEntityEntry[]::new))
                .maxValues(Arrays.stream(attributes).map(filter::getMaxValueForRight).toArray(ADBEntityEntry[]::new))
                .isLeft(false)
                .build();
        joinTask.getRightPartitionManager().tell(new RedirectToPartition(joinTask.getRightPartitionId(), rightCommand));
    }

    private Behavior<Command> handleMultipleAttributes(ADBPartition.MultipleAttributes response) {
        if (response.isLeft()) {
            this.leftOriginalSizes = response.getOriginalSize();
            this.leftColumns = response.getAttributes();
            this.leftAttributes = response
                    .getAttributes().entrySet().parallelStream()
                    .collect(Collectors.toMap(Map.Entry::getKey, pair -> pair.getValue().materializeSorted()));
        }
        if (!response.isLeft()) {
            this.rightOriginalSizes = response.getOriginalSize();
            this.rightColumns = response.getAttributes();
            this.rightAttributes = response
                    .getAttributes().entrySet().parallelStream()
                    .collect(Collectors.toMap(Map.Entry::getKey, pair -> pair.getValue().materializeSorted()));
        }
        if (this.leftAttributes != null && this.rightAttributes != null) {
            this.respondTo.tell(new JoinTaskPrepared(this.getContext().getSelf()));
        }
        return Behaviors.same();
    }

    private boolean isNotPrepared() {
        return this.leftAttributes == null || this.rightAttributes == null;
    }

    private Behavior<Command> handleExecute(Execute command) {
        if (this.isNotPrepared()) {
            this.getContext().scheduleOnce(Duration.ofMillis(50), this.getContext().getSelf(), command);
            this.getContext().getLog().warn("Missing attributes - rescheduling execution");
            return Behaviors.same();
        }
        if (this.leftAttributes.values().stream().anyMatch(entriesL -> entriesL.size() < 1) || this.rightAttributes.values().stream().anyMatch(entriesR -> entriesR.size() < 1)) {
            this.returnResults(new ADBPartialJoinResult(0));
            return Behaviors.same();
        }
        this.costModels = this.getSortedCostModels(this.joinQuery);
        this.execute();
        return Behaviors.same();
    }

    private ObjectList<ADBJoinPredicateCostModel> getSortedCostModels(ADBJoinQuery query) {
        ObjectList<ADBJoinPredicateCostModel> costModels = query.getPredicates().parallelStream()
                                                                .map(this::getCostModel)
                                                                .collect(new ObjectArrayListCollector<>());
        costModels.sort(Comparator.comparingInt(ADBJoinPredicateCostModel::getCost));
        return costModels;
    }

    private ADBJoinPredicateCostModel getCostModel(ADBJoinQueryPredicate predicate) {
        val leftValues = this.leftAttributes.get(predicate.getLeftHandSideAttribute());
        val rightValues = this.rightAttributes.get(predicate.getRightHandSideAttribute());
        return ADBJoinPredicateCostModelFactory.calc(predicate, leftValues, rightValues);
    }

    private void execute() {
        this.getContext().getLog().debug("Cheapest predicate {} ", this.costModels.get(0));
        this.getContext().getLog().debug("Most expensive predicate {} ", this.costModels.get(costModels.size() - 1));
        if (this.costModels.get(0).getCost() == 0) {
            this.handleNoJoinResultsExpected();
            return;
        }
        if (this.costModels.size() < 2) {
            this.handleOnlyTwoJoinPredicates();
            return;
        }
        if (this.costModels.get(0).getRelativeCost() <= this.settings.JOIN_STRATEGY_LOWER_BOUND) {
            this.handleRowBasedJoin();
        } else if (this.costModels.get(0).getRelativeCost() <= this.settings.JOIN_STRATEGY_UPPER_BOUND) {
            this.handleColumnBasedJoin();
        } else {
            this.handleRowBasedJoin();
        }
    }

    private void handleNoJoinResultsExpected() {
        this.returnResults(new ADBPartialJoinResult(0));
    }

    private void handleOnlyTwoJoinPredicates() {
        ADBPartialJoinResult results = costModels.get(0).getJoinCandidates(leftAttributes, rightAttributes);
        this.costModelsProcessed = this.costModels.size();
        this.returnResults(results);
    }

    private void handleRowBasedJoin() {
        ADBPartialJoinResult candidates = costModels.get(0).getJoinCandidates(leftAttributes, rightAttributes);
        this.costModelsProcessed = this.costModels.size();
        this.joinRowBased(candidates, this.costModels.subList(1, this.costModels.size()));
    }

    private void handleColumnBasedJoin() {
        this.costModelsProcessed = this.costModels.size();
        this.joinColumnBased(this.costModels);
    }

    private void joinRowBased(ADBPartialJoinResult joinCandidates, ObjectList<ADBJoinPredicateCostModel> costModels) {
        val workload = new JoinQueryRowWorkload(joinCandidates, this.leftColumns, this.rightColumns, costModels);
        val respondTo = getContext().messageAdapter(GenericWorker.Response.class, GenericWorkerResponseWrapper::new);
        ADBQueryManager.getWorkerPool().tell(new GenericWorker.WorkloadMessage(respondTo, workload));
    }

    private void joinColumnBased(ObjectList<ADBJoinPredicateCostModel> costModels) {
        val respondTo = getContext().messageAdapter(StepExecuted.class, ADBColumnJoinExecutorWrapper::new);
        val actorName = "ColumnJoinStep-" + UUID.randomUUID().toString();
        val behavior = ADBColumnJoinStepExecutorFactory.createDefault(
                leftAttributes, rightAttributes, leftOriginalSizes, rightOriginalSizes, costModels, respondTo);
        this.getContext().spawn(behavior, actorName).tell(new ADBColumnJoinStepExecutor.Execute());
    }

    private Behavior<Command> handleGenericWorkerResponse(GenericWorkerResponseWrapper wrapper) {
        if (wrapper.response instanceof JoinQueryRowWorkload.Results) {
            ADBPartialJoinResult results = ((JoinQueryRowWorkload.Results) wrapper.response).getResults();
            this.returnResults(results);
            return Behaviors.same();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleADBColumnJoinExecutorWrapper(ADBColumnJoinExecutorWrapper wrapper) {
        ADBPartialJoinResult results = wrapper.response.getResults();
        if (this.costModelsProcessed == this.costModels.size()) {
            this.returnResults(results);
            return Behaviors.same();
        }
        this.joinRowBased(results, this.costModels.subList(this.costModelsProcessed, this.costModels.size()));
        this.costModelsProcessed = this.costModels.size();
        return Behaviors.same();
    }

    private void returnResults(ADBPartialJoinResult results) {
        this.resetExecutor();
        this.respondTo.tell(new PartitionsJoined(results, this.getContext().getSelf()));
    }

    private void resetExecutor() {
        this.leftAttributes = null;
        this.rightAttributes = null;
        this.costModels = null;
        this.joinTask = null;
    }

    @Override
    protected void handleReceiverTerminated() {

    }
}
