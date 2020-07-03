package de.hpi.julianweise.master.data_loading.distribution;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBDataDistributor extends AbstractBehavior<ADBDataDistributor.Command> {

    private static final Duration MAX_ROUND_TRIP_TIME = Duration.ofMillis(100);

    private final ActorRef<ADBPartitionManager.Command> partitionManager;
    private final ObjectArrayFIFOQueue<ADBEntity> payload = new ObjectArrayFIFOQueue<>();
    private final AtomicInteger pendingDistributions = new AtomicInteger(0);
    private final SettingsImpl settings;


    public interface Command {}

    public interface Response extends CborSerializable {}

    @AllArgsConstructor
    @Getter
    public static class Distribute implements Command {
        boolean force;
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
    public static class LargeMessageSenderResponse implements Command {
        ADBLargeMessageSender.Response response;
    }

    @Getter
    @NoArgsConstructor
    public static class ConfirmEntitiesPersisted implements Command, KryoSerializable {}

    @AllArgsConstructor
    public static class BatchDistributed implements Response {}

    @AllArgsConstructor
    public static class Finalized implements Response {}

    protected ADBDataDistributor(ActorContext<Command> actorContext, ActorRef<ADBPartitionManager.Command> endpoint) {
        super(actorContext);
        this.partitionManager = endpoint;
        this.settings = Settings.SettingsProvider.get(getContext().getSystem());
    }

    @Override
    public Receive<ADBDataDistributor.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Distribute.class, this::handleDistribute)
                .onMessage(DistributeBatch.class, this::handleDistributeBatchToNodes)
                .onMessage(ConfirmEntitiesPersisted.class, this::handleConfirmEntityPersisted)
                .onMessage(ConcludeDistribution.class, this::handleConcludeDistribution)
                .onMessage(LargeMessageSenderResponse.class, this::handleLargeMessageSenderResponse)
                .build();
    }
    private Behavior<Command> handleDistributeBatchToNodes(DistributeBatch command) {
        for (ADBEntity entity : command.getEntities()) {
            this.payload.enqueue(entity);
        }
        this.getContext().getSelf().tell(new Distribute(false));
        command.getClient().tell(new BatchDistributed());
        return Behaviors.same();
    }

    private Behavior<Command> handleDistribute(Distribute command) {
        val respondTo = getContext().messageAdapter(ADBLargeMessageSender.Response.class, LargeMessageSenderResponse::new);
        while (payload.size() >= this.settings.DISTRIBUTION_CHUNK_SIZE || command.force && payload.size() > 0) {
            this.distributeBatch(respondTo);
        }
        return Behaviors.same();
    }

    private void distributeBatch(ActorRef<ADBLargeMessageSender.Response> respondTo) {
        ObjectList<ADBEntity> payload = new ObjectArrayList<>(this.settings.DISTRIBUTION_CHUNK_SIZE);
        int upperBoundQueue = Math.min(this.settings.DISTRIBUTION_CHUNK_SIZE, this.payload.size());
        for (int i = 0; i < upperBoundQueue; i++) payload.add(this.payload.dequeue());
        val message = new ADBPartitionManager.PersistEntities(Adapter.toClassic(getContext().getSelf()), payload);
        ADBLargeMessageActor.sendMessage(getContext(), Adapter.toClassic(partitionManager), respondTo, message);
        this.pendingDistributions.incrementAndGet();
    }

    private Behavior<Command> handleConfirmEntityPersisted(ConfirmEntitiesPersisted command) {
        this.pendingDistributions.decrementAndGet();
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeDistribution(ConcludeDistribution command) {
        if (pendingDistributions.get() > 0 || this.payload.size() > 0) {
            this.getContext().getSelf().tell(new Distribute(true));
            this.getContext().scheduleOnce(MAX_ROUND_TRIP_TIME, this.getContext().getSelf(), command);
            return Behaviors.same();
        }
        this.partitionManager.tell(new ADBPartitionManager.ConcludeTransfer());
        command.respondTo.tell(new Finalized());
        return Behaviors.stopped();
    }

    private Behavior<Command> handleLargeMessageSenderResponse(LargeMessageSenderResponse wrapper) {
        return Behaviors.same();
    }
}
