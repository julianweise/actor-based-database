package de.hpi.julianweise.master.data_loading.distribution;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.routing.ConsistentHash;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;
import org.agrona.collections.Object2ObjectHashMap;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBDataDistributor extends AbstractBehavior<ADBDataDistributor.Command> {

    private static final Duration MAX_ROUND_TRIP_TIME = Duration.ofMillis(300);
    private static final int VIRTUAL_NODES_FACTOR = 50;

    private final Set<ActorRef<ADBPartitionManager.Command>> partitionManagers = new HashSet<>();
    private final AtomicInteger pendingDistributions = new AtomicInteger(0);
    private ConsistentHash<ActorRef<ADBPartitionManager.Command>> consistentHash;
    private ActorRef<Response> client;
    private boolean concluding = false;
    private final int minNumberOfPartitions;
    private final Map<ActorRef<ADBPartitionManager.Command>, ObjectList<ADBEntity>> batches = new Object2ObjectHashMap<>();
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());


    public interface Command {}

    public interface Response extends CborSerializable {}
    @AllArgsConstructor
    @Getter
    public static class WrappedListing implements Command, CborSerializable {
        private final Receptionist.Listing listing;

    }
    @AllArgsConstructor
    @Getter
    public static class Distribute implements Command {
        private final ActorRef<ADBPartitionManager.Command> targetPartitionManager;
    }

    @AllArgsConstructor
    @Getter
    public static class DistributeBatch implements Command, CborSerializable {
        private final ActorRef<Response> client;
        private final ObjectList<ADBEntity> entities;

    }
    @AllArgsConstructor
    public static class ConcludeDistribution implements Command, KryoSerializable {
    }

    @AllArgsConstructor
    public static class BatchDistributed implements Response {
    }

    @AllArgsConstructor
    public static class DataFullyDistributed implements Response {
    }

    @AllArgsConstructor
    public static class MessageSenderResponse implements Command {
        ADBLargeMessageSender.Response response;
    }

    @Getter
    @NoArgsConstructor
    public static class ConfirmEntitiesPersisted implements Command, KryoSerializable { }

    protected ADBDataDistributor(ActorContext<Command> actorContext) {
        super(actorContext);
        val akkaConfig = getContext().getSystem().settings().config();
        this.minNumberOfPartitions = akkaConfig.getInt("akka.cluster.min-nr-of-members") - 1;
    }

    @Override
    public Receive<ADBDataDistributor.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedListing.class, this::handleWrappedReceptionistListing)
                .onMessage(Distribute.class, this::handleDistributeToNodes)
                .onMessage(DistributeBatch.class, this::handleDistributeBatchToNodes)
                .onMessage(ConfirmEntitiesPersisted.class, this::handleConfirmEntityPersisted)
                .onMessage(ConcludeDistribution.class, this::handleConcludeDistribution)
                .onMessage(MessageSenderResponse.class, this::handleLargeMessageSenderResponse)
                .build();
    }

    private Behavior<Command> handleWrappedReceptionistListing(WrappedListing command) {
        this.partitionManagers.addAll(command.getListing().getServiceInstances(ADBPartitionManager.SERVICE_KEY));
        this.consistentHash = ConsistentHash.create(this.partitionManagers, VIRTUAL_NODES_FACTOR);
        this.partitionManagers.forEach(manager -> this.batches.put(manager, new ObjectArrayList<>()));
        return Behaviors.same();
    }

    private Behavior<Command> handleDistributeToNodes(Distribute command) {
        ObjectList<ADBEntity> entities = this.batches.remove(command.getTargetPartitionManager());
        this.batches.put(command.getTargetPartitionManager(), new ObjectArrayList<>());
        val respondTo = getContext().messageAdapter(ADBLargeMessageSender.Response.class, MessageSenderResponse::new);
        val message  = new ADBPartitionManager.PersistEntities(Adapter.toClassic(getContext().getSelf()), entities);
        ADBLargeMessageActor.sendMessage(getContext(), Adapter.toClassic(command.targetPartitionManager), respondTo, message);
        this.pendingDistributions.incrementAndGet();
        return Behaviors.same();
    }

    public void concludeTransfer() {
        this.concluding = true;
        this.partitionManagers.forEach(manager -> manager.tell(new ADBPartitionManager.ConcludeTransfer()));
    }

    private Behavior<Command> handleConfirmEntityPersisted(ConfirmEntitiesPersisted command) {
        this.pendingDistributions.decrementAndGet();
        this.notifyClient();
        return Behaviors.same();
    }

    private void notifyClient() {
        if (this.pendingDistributions.get() > 0 || this.concluding) {
            return;
        }
        this.client.tell(new BatchDistributed());
    }

    private Behavior<Command> handleDistributeBatchToNodes(DistributeBatch command) {
        if (this.consistentHash == null || this.consistentHash.isEmpty() || this.partitionManagers.size() < this.minNumberOfPartitions) {
            this.getContext().scheduleOnce(Duration.ofMillis(500), this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.client = command.getClient();
        for (ADBEntity entity : command.getEntities()) {
            this.batches.get(this.consistentHash.nodeFor(entity.getPrimaryKey().toString())).add(entity);
        }
        this.triggerDistribution(false);
        return Behaviors.same();
    }

    private void triggerDistribution(boolean force) {
        for (Map.Entry<ActorRef<ADBPartitionManager.Command>, ObjectList<ADBEntity>> nodeBatch : batches.entrySet()) {
            if (force || nodeBatch.getValue().size() >= this.settings.DISTRIBUTION_CHUNK_SIZE) {
                this.getContext().getSelf().tell(new Distribute(nodeBatch.getKey()));
            }
        }
        if (!force) {
            this.notifyClient();
        }
    }

    private Behavior<Command> handleConcludeDistribution(ConcludeDistribution command) {
        this.triggerDistribution(true);
        if (batches.entrySet().stream().anyMatch(set -> set.getValue().size() > 0) || pendingDistributions.get() > 0) {
            this.getContext().scheduleOnce(MAX_ROUND_TRIP_TIME, this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.client.tell(new DataFullyDistributed());
        this.concludeTransfer();
        return Behaviors.same();
    }

    private Behavior<Command> handleLargeMessageSenderResponse(MessageSenderResponse wrapper) {
        return Behaviors.same();
    }
}
