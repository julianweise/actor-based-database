package de.hpi.julianweise.shard;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.GroupRouter;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.Routers;
import akka.actor.typed.javadsl.TimerScheduler;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.master.ADBMasterSupervisor;
import de.hpi.julianweise.utility.CborSerializable;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ADBShardDistributor extends AbstractBehavior<ADBShardDistributor.Command> {

    public interface Command extends CborSerializable {
    }

    @AllArgsConstructor
    @Getter
    public static class DistributeToShards implements Command, ADBShard.Command {
        ADBEntityType entity;
    }

    @AllArgsConstructor
    @Getter
    public static class CheckPendingDistributions implements Command {
    }

    @AllArgsConstructor
    @Getter
    public static class DistributeBatchToShards implements Command {
        private final ActorRef<ADBMasterSupervisor.Response> client;
        private final List<ADBEntityType> entities;
    }

    @AllArgsConstructor
    public static class ConcludeDistribution implements Command {}

    @Getter
    @AllArgsConstructor
    public static class BatchDistributed implements ADBMasterSupervisor.Response { }

    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    public static class ConfirmEntityPersisted implements Command {
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
        @JsonSubTypes({
                              @JsonSubTypes.Type(value = String.class, name = "String"),
                              @JsonSubTypes.Type(value = Integer.class, name = "Integer"),
                              @JsonSubTypes.Type(value = Float.class, name = "Float"),
                              @JsonSubTypes.Type(value = Double.class, name = "Double"),
                              @JsonSubTypes.Type(value = Character.class, name = "Character"),
                              @JsonSubTypes.Type(value = Boolean.class, name = "Boolean"),
                      })
        Comparable<?> entityPrimaryKey;
    }

    public static Behavior<ADBShardDistributor.Command> create() {
        return Behaviors.setup(actorContext ->
                Behaviors.withTimers(timers ->
                        new ADBShardDistributor(actorContext, timers)));
    }

    private static final int VIRTUAL_ROUTES_FACTOR_HASHING = 50;
    private static final int TIMER_DELAY = 3000;
    private static final float MIN_FACTOR_NEXT_BATCH = 0.3f;
    private static final Object TIMER_KEY = new Object();


    private final ActorRef<ADBShard.Command> clusterShardsRouter;
    private final Map<Comparable<?>, ADBShard.Command> pendingDistributions = new HashMap<>();
    private final BlockingQueue<Pair<Long, Comparable<?>>> pendingDistTimer = new LinkedBlockingQueue<>();
    private final TimerScheduler<Command> timers;
    private ActorRef<ADBMasterSupervisor.Response> client;
    private int batchSize = 0;
    private boolean wrappingUp = false;

    private ADBShardDistributor(ActorContext<Command> actorContext, TimerScheduler<Command> timers) {
        super(actorContext);
        this.timers = timers;
        this.timers.startTimerWithFixedDelay(TIMER_KEY, new CheckPendingDistributions(),
                Duration.of(TIMER_DELAY, ChronoUnit.MILLIS));
        GroupRouter<ADBShard.Command> clusterShards = Routers.group(ADBShard.SERVICE_KEY);
        clusterShards.withConsistentHashingRouting(VIRTUAL_ROUTES_FACTOR_HASHING, (this::getHashingKeyFromCommand));
        this.clusterShardsRouter = this.getContext().spawn(clusterShards, "cluster-shards");
    }

    private String getHashingKeyFromCommand(ADBShard.Command command) {
        return command.getEntity().getPrimaryKey().toString();
    }

    @Override
    public Receive<ADBShardDistributor.Command> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, this::handlePostStop)
                .onMessage(DistributeToShards.class, this::handleDistributeToShards)
                .onMessage(DistributeBatchToShards.class, this::handleDistributeBatchToShards)
                .onMessage(ConfirmEntityPersisted.class, this::handleConfirmEntityPersisted)
                .onMessage(CheckPendingDistributions.class, this::handleCheckPendingDistribution)
                .onMessage(ConcludeDistribution.class, this::handleConcludeDistribution)
                .build();
    }

    private Behavior<Command> handlePostStop(PostStop signal) {
        this.timers.cancelAll();
        return this;
    }

    private Behavior<Command> handleDistributeToShards(DistributeToShards command) {
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

    private Behavior<Command> handleDistributeBatchToShards(DistributeBatchToShards command) {
        this.client = command.getClient();
        this.batchSize = command.getEntities().size();
        for (ADBEntityType entity : command.getEntities()) {
            this.getContext().getSelf().tell(new DistributeToShards(entity));
        }
        return this;
    }

    private Behavior<Command> handleCheckPendingDistribution(CheckPendingDistributions command) {
        for (Pair<Long, Comparable<?>> pair : this.pendingDistTimer) {
            if (pair.getKey() > System.currentTimeMillis() - TIMER_DELAY) {
                break;
            }
            if (this.pendingDistributions.get(pair.getValue()) != null) {
                this.sendToShard((ADBShard.PersistEntity) this.pendingDistributions.get(pair.getValue()));
                this.pendingDistributions.remove(pair.getValue());
            }
            this.pendingDistTimer.remove(pair);
        }
        if (this.wrappingUp && this.pendingDistributions.size() < 1 && this.pendingDistTimer.size() < 1) {
            this.client.tell(new ADBMasterSupervisor.DataSuccessfullyDistributed());
        }
        return this;
    }

    private Behavior<Command> handleConcludeDistribution(ConcludeDistribution command) {
        this.wrappingUp = true;
        return this;
    }
}
