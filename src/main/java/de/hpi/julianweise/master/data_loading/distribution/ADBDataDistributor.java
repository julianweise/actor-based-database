package de.hpi.julianweise.master.data_loading.distribution;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.routing.ConsistentHash;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBDataDistributor extends AbstractBehavior<ADBDataDistributor.Command> {

    private static final Duration MAX_ROUND_TRIP_TIME = Duration.ofMillis(300);
    private static final int VIRTUAL_NODES_FACTOR = 50;

    private final Set<ActorRef<ADBPartitionManager.Command>> partitionManagers = new HashSet<>();
    private final AtomicInteger pendingDistributions = new AtomicInteger(0);
    private ConsistentHash<ActorRef<ADBPartitionManager.Command>> consistentHash;
    private ActorRef<Response> client;
    private final int minNumberOfPartitions;


    public interface Command {}

    public interface Response extends CborSerializable {}
    @AllArgsConstructor
    @Getter
    public static class WrappedListing implements Command, CborSerializable {
        private final Receptionist.Listing listing;

    }
    @AllArgsConstructor
    @Getter
    public static class Distribute implements Command, ADBPartitionManager.Command, CborSerializable {
        private final ADBEntity entity;

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
    @Getter
    @NoArgsConstructor
    public static class ConfirmEntityPersisted implements Command, KryoSerializable { }

    protected ADBDataDistributor(ActorContext<Command> actorContext) {
        super(actorContext);
        this.minNumberOfPartitions = getContext().getSystem().settings().config().getInt("akka.cluster" +
                ".min-nr-of-members") - 1;
    }

    @Override
    public Receive<ADBDataDistributor.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedListing.class, this::handleWrappedReceptionistListing)
                .onMessage(Distribute.class, this::handleDistributeToShards)
                .onMessage(DistributeBatch.class, this::handleDistributeBatchToShards)
                .onMessage(ConfirmEntityPersisted.class, this::handleConfirmEntityPersisted)
                .onMessage(ConcludeDistribution.class, this::handleConcludeDistribution)
                .build();
    }

    private Behavior<Command> handleWrappedReceptionistListing(WrappedListing command) {
        this.getContext().getLog().info("Received new PartitionManager listing containing " + command.listing.getServiceInstances(ADBPartitionManager.SERVICE_KEY).size());
        this.partitionManagers.addAll(command.getListing().getServiceInstances(ADBPartitionManager.SERVICE_KEY));
        this.consistentHash = ConsistentHash.create(this.partitionManagers, VIRTUAL_NODES_FACTOR);
        return Behaviors.same();
    }

    private Behavior<Command> handleDistributeToShards(Distribute command) {
        this.sendToShard(new ADBPartitionManager.PersistEntity(this.getContext().getSelf(), command.getEntity()));
        return Behaviors.same();
    }

    public void sendToShard(ADBPartitionManager.PersistEntity command) {
        this.pendingDistributions.incrementAndGet();
        this.consistentHash.nodeFor(command.getEntity().getPrimaryKey().toString()).tell(command);
    }

    public void concludeTransfer() {
        this.partitionManagers.forEach(manager -> manager.tell(new ADBPartitionManager.ConcludeTransfer()));
    }

    private Behavior<Command> handleConfirmEntityPersisted(ConfirmEntityPersisted command) {
        this.pendingDistributions.decrementAndGet();
        this.notifyClient();
        return Behaviors.same();
    }

    private void notifyClient() {
        if (this.pendingDistributions.get() > 0) {
            return;
        }
        this.client.tell(new BatchDistributed());
    }

    private Behavior<Command> handleDistributeBatchToShards(DistributeBatch command) {
        if (this.consistentHash == null || this.consistentHash.isEmpty() || this.partitionManagers.size() < this.minNumberOfPartitions) {
            this.getContext().scheduleOnce(Duration.ofMillis(500), this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.client = command.getClient();
        for (ADBEntity entity : command.getEntities()) {
            this.getContext().getSelf().tell(new Distribute(entity));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeDistribution(ConcludeDistribution command) {
        if (this.pendingDistributions.get() > 0) {
            this.getContext().scheduleOnce(MAX_ROUND_TRIP_TIME, this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.client.tell(new DataFullyDistributed());
        this.concludeTransfer();
        return Behaviors.same();
    }
}
