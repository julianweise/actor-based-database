package de.hpi.julianweise.shard;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.GroupRouter;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
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
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ADBShardDistributor extends AbstractBehavior<ADBShardDistributor.Command> {

    public interface Command extends CborSerializable {
    }

    public interface Response extends CborSerializable {
    }

    @AllArgsConstructor
    @Getter
    public static class Distribute implements Command, ADBShard.Command {
        ADBEntityType entity;
    }

    @AllArgsConstructor
    @Getter
    public static class CheckPendingDistributions implements Command {
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
        ADBKey entityPrimaryKey;
    }

    private static final float MIN_FACTOR_NEXT_BATCH = 0.3f;
    private static final int MAX_ROUND_TRIP_TIME = 3000;

    private final ActorRef<ADBShard.Command> clusterShardsRouter;
    private final Map<ADBKey, ADBShard.Command> pendingDistributions = new HashMap<>();
    private final BlockingQueue<Pair<Long, ADBKey>> pendingDistTimer = new LinkedBlockingQueue<>();
    private final TimerScheduler<Command> timers;
    private ActorRef<Response> client;
    private int batchSize = 0;
    private boolean wrappingUp = false;

    protected ADBShardDistributor(ActorContext<Command> actorContext, TimerScheduler<Command> timers,
                                  GroupRouter<ADBShard.Command> clusterShards) {
        super(actorContext);
        this.timers = timers;
        this.clusterShardsRouter = this.getContext().spawn(clusterShards, "cluster-shards");
    }

    @Override
    public Receive<ADBShardDistributor.Command> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, this::handlePostStop)
                .onMessage(Distribute.class, this::handleDistributeToShards)
                .onMessage(DistributeBatch.class, this::handleDistributeBatchToShards)
                .onMessage(ConfirmEntityPersisted.class, this::handleConfirmEntityPersisted)
                .onMessage(CheckPendingDistributions.class, this::handleCheckPendingDistribution)
                .onMessage(ConcludeDistribution.class, this::handleConcludeDistribution)
                .build();
    }

    private Behavior<Command> handlePostStop(PostStop signal) {
        this.timers.cancelAll();
        return this;
    }

    private Behavior<Command> handleDistributeToShards(Distribute command) {
        this.sendToShard(new ADBShard.PersistEntity(this.getContext().getSelf(), command.getEntity()));
        return this;
    }

    public void sendToShard(ADBShard.PersistEntity command) {
        this.pendingDistTimer.add(new Pair<>(System.currentTimeMillis(), command.getEntity().getPrimaryKey()));
        this.pendingDistributions.put(command.getEntity().getPrimaryKey(), command);
        this.clusterShardsRouter.tell(command);
    }

    private Behavior<Command> handleConfirmEntityPersisted(ConfirmEntityPersisted command) {
        this.pendingDistributions.remove(command.getEntityPrimaryKey());
        this.notifyClient();
        return this;
    }

    private void notifyClient() {
        if (this.batchSize == 0 || this.pendingDistributions.size() > this.batchSize * MIN_FACTOR_NEXT_BATCH) {
            return;
        }
        this.client.tell(new BatchDistributed());
        this.batchSize = 0;
    }

    private Behavior<Command> handleDistributeBatchToShards(DistributeBatch command) {
        this.client = command.getClient();
        this.batchSize = command.getEntities().size();
        for (ADBEntityType entity : command.getEntities()) {
            this.getContext().getSelf().tell(new Distribute(entity));
        }
        return this;
    }

    private Behavior<Command> handleCheckPendingDistribution(CheckPendingDistributions command) {
        for (Pair<Long, ADBKey> pair : this.pendingDistTimer) {
            if (pair.getKey() > System.currentTimeMillis() - MAX_ROUND_TRIP_TIME) {
                break;
            }
            if (this.pendingDistributions.get(pair.getValue()) != null) {
                this.sendToShard((ADBShard.PersistEntity) this.pendingDistributions.get(pair.getValue()));
                this.pendingDistributions.remove(pair.getValue());
            }
            this.pendingDistTimer.remove(pair);
        }
        if (this.wrappingUp && this.pendingDistributions.size() < 1 && this.pendingDistTimer.size() < 1) {
            this.client.tell(new DataFullyDistributed());
        }
        return this;
    }

    private Behavior<Command> handleConcludeDistribution(ConcludeDistribution command) {
        this.wrappingUp = true;
        return this;
    }
}
