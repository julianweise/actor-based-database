package de.hpi.julianweise.shard;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.routing.ConsistentHash;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBDoubleKey;
import de.hpi.julianweise.domain.key.ADBFloatKey;
import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.domain.key.ADBStringKey;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBShardDistributor extends AbstractBehavior<ADBShardDistributor.Command> {

    private static final Duration MAX_ROUND_TRIP_TIME = Duration.ofMillis(300);
    private static final int VIRTUAL_NODES_FACTOR = 50;

    private final Set<ActorRef<ADBShard.Command>> shards = new HashSet<>();
    private final AtomicInteger pendingDistributions = new AtomicInteger(0);
    private ConsistentHash<ActorRef<ADBShard.Command>> consistentHash;
    private ActorRef<Response> client;
    private int minNumberOfShards = this.getContext().getSystem().settings().config().getInt("akka.cluster" +
            ".min-nr-of-members");


    public interface Command extends CborSerializable {}

    public interface Response extends CborSerializable {}
    @AllArgsConstructor
    @Getter
    public static class WrappedListing implements Command {
        private Receptionist.Listing listing;

    }
    @AllArgsConstructor
    @Getter
    public static class Distribute implements Command, ADBShard.Command {
        private ADBEntityType entity;

    }
    @AllArgsConstructor
    @Getter
    public static class DistributeBatch implements Command {
        private final ActorRef<Response> client;
        private final List<ADBEntityType> entities;

    }
    @AllArgsConstructor
    public static class ConcludeDistribution implements Command {

    }
    @AllArgsConstructor
    public static class BatchDistributed implements Response {

    }
    @AllArgsConstructor
    public static class DataFullyDistributed implements Response {

    }
    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    public static class ConfirmEntityPersisted implements Command {
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
        @JsonSubTypes({
                              @JsonSubTypes.Type(value = ADBStringKey.class, name = "String"),
                              @JsonSubTypes.Type(value = ADBIntegerKey.class, name = "Integer"),
                              @JsonSubTypes.Type(value = ADBFloatKey.class, name = "Float"),
                              @JsonSubTypes.Type(value = ADBDoubleKey.class, name = "Double"),
                      })
        private ADBKey entityPrimaryKey;

    }

    protected ADBShardDistributor(ActorContext<Command> actorContext) {
        super(actorContext);
    }

    @Override
    public Receive<ADBShardDistributor.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedListing.class, this::handleWrappedReceptionistListing)
                .onMessage(Distribute.class, this::handleDistributeToShards)
                .onMessage(DistributeBatch.class, this::handleDistributeBatchToShards)
                .onMessage(ConfirmEntityPersisted.class, this::handleConfirmEntityPersisted)
                .onMessage(ConcludeDistribution.class, this::handleConcludeDistribution)
                .build();
    }

    private Behavior<Command> handleWrappedReceptionistListing(WrappedListing command) {
        this.getContext().getLog().info("Received new Shards listing containing " + command.listing.getServiceInstances(ADBShard.SERVICE_KEY).size());
        this.shards.addAll(command.getListing().getServiceInstances(ADBShard.SERVICE_KEY));
        this.consistentHash = ConsistentHash.create(this.shards, VIRTUAL_NODES_FACTOR);
        return Behaviors.same();
    }

    private Behavior<Command> handleDistributeToShards(Distribute command) {
        this.sendToShard(new ADBShard.PersistEntity(this.getContext().getSelf(), command.getEntity()));
        return Behaviors.same();
    }

    public void sendToShard(ADBShard.PersistEntity command) {
        this.pendingDistributions.incrementAndGet();
        this.consistentHash.nodeFor(command.getEntity().getPrimaryKey().toString()).tell(command);
    }

    public void concludeTransfer() {
        List<ActorRef<ADBShard.Command>> numberedShards = new ArrayList<>(this.shards);
        for(int i = 0; i < numberedShards.size(); i ++) {
            numberedShards.get(i).tell(new ADBShard.ConcludeTransfer(i));
        }
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
        if (this.consistentHash == null || this.consistentHash.isEmpty() || this.shards.size() < this.minNumberOfShards) {
            this.getContext().scheduleOnce(Duration.ofMillis(500), this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.client = command.getClient();
        for (ADBEntityType entity : command.getEntities()) {
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
