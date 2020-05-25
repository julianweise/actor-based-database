package de.hpi.julianweise.master.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.google.common.collect.Streams;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.master.ADBMaster;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import de.hpi.julianweise.utility.query.join.JoinDistributionPlan;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.val;
import org.agrona.collections.Int2ObjectHashMap;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static java.util.stream.Collectors.groupingBy;

public class ADBMasterJoinSession extends ADBMasterQuerySession {

    private final JoinDistributionPlan distributionPlan;
    private final AtomicInteger resultCounter = new AtomicInteger(0);
    private final Int2ObjectHashMap<ADBEntity> materializedEntities = new Int2ObjectHashMap<>();
    private final IntSet requestedEntitiesForMaterialization = new IntOpenHashSet();
    private final ObjectArrayFIFOQueue<ADBKeyPair> joinResults = new ObjectArrayFIFOQueue<>();
    private final ADBJoinQuery query;
    private final ActorRef<ADBPartition.MaterializedEntities> materializedRespondTo =
            this.getContext().messageAdapter(ADBPartition.MaterializedEntities.class, MaterializedEntitiesWrapper::new);

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class RequestNextNodeToJoin implements ADBMasterQuerySession.Command, CborSerializable {
        private ActorRef<ADBSlaveQuerySession.Command> respondTo;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class TriggerShardComparison implements ADBMasterQuerySession.Command, CborSerializable {
        private ActorRef<ADBQueryManager.Command> nextJoinManager;
        private ActorRef<ADBSlaveQuerySession.Command> respondTo;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @SuperBuilder
    @Getter
    public static class JoinQueryResults extends ADBMasterQuerySession.QueryResults {
        private ObjectList<ADBKeyPair> joinResults;
    }

    @AllArgsConstructor
    public static class MaterializedEntitiesWrapper implements Command {
        private final ADBPartition.MaterializedEntities entities;
    }

    public ADBMasterJoinSession(ActorContext<ADBMasterQuerySession.Command> context,
                                ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers,
                                ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers,
                                int transactionId,
                                ActorRef<ADBPartitionInquirer.Command> parent,
                                ADBJoinQuery query) {
        super(context, queryManagers, partitionManagers, transactionId, parent);
        this.distributionPlan = new JoinDistributionPlan(this.queryManagers);
        this.query = query;

        // Send initial query
        val respondTo = this.getContext().messageAdapter(ADBLargeMessageReceiver.InitializeTransfer.class,
                InitializeTransferWrapper::new);
        this.queryManagers.forEach(manager -> manager.tell(ADBQueryManager.QueryEntities
                .builder()
                .transactionId(transactionId)
                .query(query)
                .clientLargeMessageReceiver(respondTo)
                .respondTo(this.getContext().getSelf())
                .build()));
    }

    @Override
    public Receive<ADBMasterQuerySession.Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(RequestNextNodeToJoin.class, this::handleRequestNextShardComparison)
                   .onMessage(JoinQueryResults.class, this::handleJoinQueryResults)
                   .onMessage(TriggerShardComparison.class, this::handleTriggerNextShardComparison)
                   .onMessage(MaterializedEntitiesWrapper.class, this::handleMaterializedEntities)
                   .build();
    }

    private Behavior<ADBMasterQuerySession.Command> handleRequestNextShardComparison(RequestNextNodeToJoin command) {
        val nextManager = this.distributionPlan.getNextJoinShardFor(this.handlersToManager.get(command.getRespondTo()));

        if (nextManager == null) {
            command.respondTo.tell(new ADBSlaveJoinSession.NoMoreShardsToJoinWith(this.transactionId));
            return Behaviors.same();
        }
        this.getContext().getSelf().tell(new TriggerShardComparison(nextManager, command.respondTo));
        return Behaviors.same();
    }

    private Behavior<ADBMasterQuerySession.Command> handleTriggerNextShardComparison(TriggerShardComparison command) {
        if (this.managerToHandlers.containsKey(command.nextJoinManager)) {
            int partnerJoinId = ADBMaster.getGlobalIdFor(command.nextJoinManager);
            this.getContext().getLog().info("Asking " + command.respondTo + " to join with ID " + partnerJoinId);
            command.respondTo.tell(new ADBSlaveJoinSession.JoinWithShard(
                    this.managerToHandlers.get(command.nextJoinManager), partnerJoinId));
        } else {
            this.getContext().getLog().warn("No Shard-to-Session mapping present for " + command.nextJoinManager);
            this.getContext().scheduleOnce(Duration.ofSeconds(1), this.getContext().getSelf(), command);
        }
        return Behaviors.same();
    }

    private Behavior<ADBMasterQuerySession.Command> handleJoinQueryResults(JoinQueryResults results) {
        this.resultCounter.set(this.resultCounter.get() + results.joinResults.size());
        if (!this.query.isShouldBeMaterialized()) {
            parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, results.joinResults, false));
            return Behaviors.same();
        }
        results.joinResults.forEach(this.joinResults::enqueue);
        this.requestResultMaterialization(results.getJoinResults());
        return Behaviors.same();
    }

    private void requestResultMaterialization(ObjectList<ADBKeyPair> tuples) {
        Streams.concat(tuples.stream().map(ADBKeyPair::getKey), tuples.stream().map(ADBKeyPair::getValue))
               .collect(groupingBy(ADBInternalIDHelper::getNodeId))
               .forEach(this::requestMaterializedEntitiesFromNode);
    }

    private void requestMaterializedEntitiesFromNode(int nodeId, List<Integer> intIds) {
        IntList filteredIds = intIds.stream()
                                    .mapToInt(id -> id)
                                    .filter(id -> !this.requestedEntitiesForMaterialization.contains(id))
                                    .filter(id -> !this.materializedEntities.containsKey(id))
                                    .collect(IntArrayList::new, IntArrayList::add, IntArrayList::addAll);
        filteredIds.forEach((IntConsumer) this.requestedEntitiesForMaterialization::add);
        IntArrayList ids = new IntArrayList(filteredIds);
        partitionManagers.get(nodeId).tell(new ADBPartitionManager.MaterializeToEntities(materializedRespondTo, ids));
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }

    @Override
    protected void submitResults() {
        parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, new ObjectArrayList<>(), true));
        this.getContext().getLog().info("[FINAL RESULT]: Submitting " + this.resultCounter.get() + " elements.");
    }

    private Behavior<Command> handleMaterializedEntities(MaterializedEntitiesWrapper wrapper) {
        for (ADBEntity entity : wrapper.entities.getResults()) {
            this.materializedEntities.put(entity.getInternalID(), entity);
            this.requestedEntitiesForMaterialization.remove(entity.getInternalID());
        }
        ObjectList<ADBPair<ADBEntity, ADBEntity>> materializedResults = new ObjectArrayList<>(this.joinResults.size());
        int initialQueueSize = this.joinResults.size();
        for (int i = 0; i < initialQueueSize; i++) {
            ADBKeyPair pair = this.joinResults.dequeue();
            if (!this.isJoinPairMaterializable(pair)) {
                this.joinResults.enqueue(pair);
                continue;
            }
            materializedResults.add(this.materializePair(pair));
        }

        parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, materializedResults, false));
        this.conditionallyConcludeTransaction();
        return Behaviors.same();
    }

    private boolean isJoinPairMaterializable(ADBKeyPair pair) {
        return materializedEntities.containsKey(pair.getKey()) && materializedEntities.containsKey(pair.getValue());
    }

    private ADBPair<ADBEntity, ADBEntity> materializePair(ADBKeyPair pair) {
        assert this.materializedEntities.containsKey(pair.getKey()) : "Cannot mater. pair with unmapped key ID";
        assert this.materializedEntities.containsKey(pair.getValue()) : "Cannot mater. pair with unmapped value ID";
        return new ADBPair<>(materializedEntities.get(pair.getKey()), materializedEntities.get(pair.getValue()));
    }

    @Override
    protected boolean isFinalized() {
        if (!this.query.isShouldBeMaterialized()) {
            return true;
        }
        return this.joinResults.isEmpty() && this.requestedEntitiesForMaterialization.isEmpty();
    }
}
