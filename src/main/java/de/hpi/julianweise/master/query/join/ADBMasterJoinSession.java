package de.hpi.julianweise.master.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.google.common.collect.Streams;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.master.ADBMaster;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import de.hpi.julianweise.utility.list.IntArrayListCollector;
import de.hpi.julianweise.utility.query.join.JoinDistributionPlan;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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

import static java.util.stream.Collectors.groupingBy;

public class ADBMasterJoinSession extends ADBMasterQuerySession {

    private final JoinDistributionPlan distributionPlan;
    private final AtomicInteger resultCounter = new AtomicInteger(0);
    private final Int2ObjectHashMap<ADBEntity> materializedEntities = new Int2ObjectHashMap<>();
    private final ObjectList<ADBKeyPair> joinResults = new ObjectArrayList<>();
    private final ADBJoinQuery query;

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
                                int transactionId,
                                ActorRef<ADBPartitionInquirer.Command> parent,
                                ADBJoinQuery query) {
        super(context, queryManagers, transactionId, parent);
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
        if (!this.query.isMaterialized()) {
            parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, results.joinResults, false));
            return Behaviors.same();
        }
        this.joinResults.addAll(results.getJoinResults());
        this.requestResultMaterialization(results.getJoinResults());
        return Behaviors.same();
    }

    private void requestResultMaterialization(ObjectList<ADBKeyPair> tuples) {
        Streams.concat(tuples.stream().map(ADBKeyPair::getKey), tuples.stream().map(ADBKeyPair::getValue))
               .collect(groupingBy(ADBInternalIDHelper::getNodeId))
               .forEach(this::requestMaterializedEntitiesFromNode);
    }

    private void requestMaterializedEntitiesFromNode(int nodeId, List<Integer> intIds) {
        val respTo = getContext().messageAdapter(ADBPartition.MaterializedEntities.class,
                MaterializedEntitiesWrapper::new);
        IntList filteredIds = intIds.stream().filter(id -> !materializedEntities.containsKey(id)).collect(new IntArrayListCollector());
        queryManagers.get(nodeId).tell(new ADBQueryManager.MaterializeToEntities(respTo, new IntArrayList(filteredIds)));
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
        }
        ObjectArrayList<ADBPair<ADBEntity, ADBEntity>> partialResults = new ObjectArrayList<>();
        int index = 0;
        for (ADBKeyPair joinTuple : this.joinResults) {
            if (this.materializedEntities.containsKey(joinTuple.getKey()) && this.materializedEntities.containsKey(joinTuple.getValue())) {
                partialResults.add(new ADBPair<>(
                        this.materializedEntities.get(joinTuple.getKey()),
                        this.materializedEntities.get(joinTuple.getValue()))
                );
                this.joinResults.remove(index);
            }
            index++;
        }
        parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, partialResults, false));
        return Behaviors.same();
    }
}
