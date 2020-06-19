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
    private final Map<ActorRef<ADBPartitionManager.Command>, ObjectList<ADBEntity>> batches = new Object2ObjectHashMap<>();
    private final AtomicInteger pendingDistributions = new AtomicInteger(0);
    private final int minNumberOfNodes;
    private ConsistentHash<ActorRef<ADBPartitionManager.Command>> consistentHash;
    private ActorRef<Response> lastClient;


    public interface Command {}

    public interface Response extends CborSerializable {}

    @AllArgsConstructor
    public static class WrappedListing implements Command {
        private final Receptionist.Listing listing;
    }

    @AllArgsConstructor
    @Getter
    public static class Distribute implements Command {
    }

    @AllArgsConstructor
    @Getter
    public static class DistributeBatch implements Command, CborSerializable {
        private final ActorRef<Response> client;
        private final ObjectList<ADBEntity> entities;

    }
    @AllArgsConstructor
    public static class ConcludeDistribution implements Command, KryoSerializable {
        ActorRef<Response> respondTo;
    }

    @AllArgsConstructor
    public static class BatchDistributed implements Response {}

    @AllArgsConstructor
    public static class Finalized implements Response {}

    @AllArgsConstructor
    public static class LargeMessageSenderResponse implements Command {
        ADBLargeMessageSender.Response response;
    }

    @Getter
    @NoArgsConstructor
    public static class ConfirmEntitiesPersisted implements Command, KryoSerializable { }

    protected ADBDataDistributor(ActorContext<Command> actorContext) {
        super(actorContext);
        this.minNumberOfNodes = getContext().getSystem().settings().config().getInt("akka.cluster.min-nr-of-members") - 1;
    }

    @Override
    public Receive<ADBDataDistributor.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedListing.class, this::handleWrappedReceptionistListing)
                .onMessage(Distribute.class, this::handleDistribute)
                .onMessage(DistributeBatch.class, this::handleDistributeBatchToNodes)
                .onMessage(ConfirmEntitiesPersisted.class, this::handleConfirmEntityPersisted)
                .onMessage(ConcludeDistribution.class, this::handleConcludeDistribution)
                .onMessage(LargeMessageSenderResponse.class, this::handleLargeMessageSenderResponse)
                .build();
    }

    private Behavior<Command> handleWrappedReceptionistListing(WrappedListing command) {
        if (command.listing.getServiceInstances(ADBPartitionManager.SERVICE_KEY).size() < this.minNumberOfNodes) {
            return Behaviors.same();
        }
        this.partitionManagers.addAll(command.listing.getServiceInstances(ADBPartitionManager.SERVICE_KEY));
        this.consistentHash = ConsistentHash.create(this.partitionManagers, VIRTUAL_NODES_FACTOR);
        this.partitionManagers.forEach(manager -> this.batches.put(manager, new ObjectArrayList<>()));
        return Behaviors.same();
    }

    private Behavior<Command> handleDistributeBatchToNodes(DistributeBatch command) {
        if (this.consistentHash == null  || this.partitionManagers.size() < this.minNumberOfNodes) {
            this.getContext().getLog().warn("Unable to distribute batches to nodes. Missing partitions");
            this.getContext().scheduleOnce(Duration.ofMillis(500), this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.lastClient = command.client;
        for (ADBEntity entity : command.getEntities()) {
            this.batches.get(this.consistentHash.nodeFor(entity.getPrimaryKey().toString())).add(entity);
        }
        this.getContext().getSelf().tell(new Distribute());
        return Behaviors.same();
    }

    private Behavior<Command> handleDistribute(Distribute command) {
        val respondTo = getContext().messageAdapter(ADBLargeMessageSender.Response.class, LargeMessageSenderResponse::new);
        for (Map.Entry<ActorRef<ADBLargeMessageActor.Command>, ObjectList<ADBEntity>> entry : batches.entrySet()) {
            if (entry.getValue().size() < 1) {
                continue;
            }
            ADBPartitionManager.PersistEntities message = new ADBPartitionManager.PersistEntities(
                    Adapter.toClassic(getContext().getSelf()), entry.getValue());
            ADBLargeMessageActor.sendMessage(getContext(), Adapter.toClassic(entry.getKey()), respondTo, message);
            this.pendingDistributions.incrementAndGet();
            this.batches.replace(entry.getKey(), new ObjectArrayList<>());
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleConfirmEntityPersisted(ConfirmEntitiesPersisted command) {
        if (this.pendingDistributions.decrementAndGet() < 1) {
            this.lastClient.tell(new BatchDistributed());
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeDistribution(ConcludeDistribution command) {
        if (this.batches.entrySet().stream().anyMatch(entry -> entry.getValue().size() > 0)
                || this.consistentHash == null  || this.partitionManagers.size() < this.minNumberOfNodes) {
            this.getContext().scheduleOnce(MAX_ROUND_TRIP_TIME, this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.partitionManagers.forEach(manager -> manager.tell(new ADBPartitionManager.ConcludeTransfer()));
        command.respondTo.tell(new Finalized());
        return Behaviors.stopped();
    }

    private Behavior<Command> handleLargeMessageSenderResponse(LargeMessageSenderResponse wrapper) {
        return Behaviors.same();
    }
}
