package de.hpi.julianweise.slave.partition;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import de.hpi.julianweise.utility.partition.ADBEntityBuffer;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ADBPartitionManager extends AbstractBehavior<ADBPartitionManager.Command> {

    public static final ServiceKey<Command> SERVICE_KEY = ServiceKey.create(Command.class, "PartitionManager");
    private static final int MAX_PARTITIONS = 0x100;

    private static ActorRef<ADBPartitionManager.Command> INSTANCE;
    private final ObjectList<ADBPartitionHeader> partitionHeaders = new ObjectArrayList<>();
    private final ObjectList<ActorRef<ADBPartition.Command>> partitions = new ObjectArrayList<>();
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private ADBEntityBuffer entityBuffer = new ADBEntityBuffer(this.settings.MAX_SIZE_PARTITION);

    public interface Command {
    }

    public interface Response {
    }

    public static void setInstance(ActorRef<ADBPartitionManager.Command> manager) {
        assert INSTANCE == null : "Instance has already been created";
        INSTANCE = manager;
    }

    public static ActorRef<ADBPartitionManager.Command> getInstance() {
        return INSTANCE;
    }

    public static void resetSingleton() {
        INSTANCE = null;
    }

    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PersistEntity implements Command, CborSerializable {
        private ActorRef<ADBDataDistributor.Command> respondTo;
        private ADBEntity entity;
    }

    @AllArgsConstructor
    public static class Register implements Command {
        ActorRef<ADBPartition.Command> partition;
        ADBPartitionHeader header;
    }

    @AllArgsConstructor
    public static class PartitionFailed implements Command {
        ActorRef<ADBPartition.Command> partition;
    }

    @Getter
    @NoArgsConstructor
    public static class ConcludeTransfer implements Command, KryoSerializable {
    }

    @AllArgsConstructor
    public static class RequestPartitionsForSelectionQuery implements Command {
        private final ActorRef<ADBPartitionManager.RelevantPartitionsSelectionQuery> respondTo;
        private final ADBSelectionQuery query;
    }

    @AllArgsConstructor
    @Builder
    public static class RequestPartitionsForJoinQuery implements Command {
        private final ActorRef<ADBPartitionManager.RelevantPartitionsJoinQuery> respondTo;
        private final ADBPartitionHeader externalHeader;
        private final ADBJoinQuery query;
    }

    @AllArgsConstructor
    @Getter
    public static class RelevantPartitionsSelectionQuery implements Response {
        private final ObjectList<ActorRef<ADBPartition.Command>> partitions;
    }

    @AllArgsConstructor
    @Getter
    public static class RelevantPartitionsJoinQuery implements Response {
        private final Set<ActorRef<ADBPartition.Command>> partitions;
        private final int fPartitionId;
        private final int[] lPartitionIdsLeft;
        private final int[] lPartitionIdsRight;
    }

    @AllArgsConstructor
    public static class RequestAllPartitionsAndHeaders implements Command {
        private final ActorRef<AllPartitionsAndHeaders> respondTo;
    }

    @AllArgsConstructor
    @Getter
    public static class AllPartitionsAndHeaders implements Response {
        private final ObjectList<ActorRef<ADBPartition.Command>> partitions;
        private final ObjectList<ADBPartitionHeader> headers;
    }

    public ADBPartitionManager(ActorContext<Command> context) {
        super(context);
        getContext().getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, this.getContext().getSelf()));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PersistEntity.class, this::handlePersistEntity)
                .onMessage(Register.class, this::handlePartitionRegistration)
                .onMessage(ConcludeTransfer.class, this::handleConcludeTransfer)
                .onMessage(PartitionFailed.class, this::handlePartitionFails)
                .onMessage(RequestPartitionsForSelectionQuery.class, this::handleRequestForSelectionQuery)
                .onMessage(RequestPartitionsForJoinQuery.class, this::handleRequestForJoinQuery)
                .onMessage(RequestAllPartitionsAndHeaders.class, this::handleRequestAllPartitionsAndHeaders)
                .build();
    }

    private Behavior<Command> handlePersistEntity(PersistEntity command) {
        this.entityBuffer.add(command.getEntity());
        command.respondTo.tell(new ADBDataDistributor.ConfirmEntityPersisted());
        this.conditionallyCreateNewPartition(false);
        return Behaviors.same();
    }

    private void conditionallyCreateNewPartition(boolean forceCreation) {
        if (forceCreation && this.entityBuffer.getBufferSize() > 0 || this.entityBuffer.isNewPartitionReady()) {
            int partId = ADBPartitionFactory.getNewPartitionId();
            this.getContext().spawn(ADBPartitionFactory.createDefault(entityBuffer.getPayloadForPartition(), partId),
                    "Partition-" + partId);
        }
    }

    private Behavior<Command> handlePartitionFails(PartitionFailed signal) {
        this.getContext().getLog().error("Partition " + signal.partition + " failed and gets removed from manager.");
        this.partitions.remove(signal.partition);
        return Behaviors.same();
    }

    private Behavior<Command> handlePartitionRegistration(Register registration) {
        this.getContext().watchWith(registration.partition, new PartitionFailed(registration.partition));
        this.partitions.add(registration.partition);
        this.partitionHeaders.add(registration.header);
        assert this.partitions.size() < MAX_PARTITIONS : "Only " + MAX_PARTITIONS + " are supported per node. " +
                "Currently: " + this.partitions.size();
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeTransfer(ConcludeTransfer command) {
        this.conditionallyCreateNewPartition(true);
        this.entityBuffer = null;
        this.getContext().getLog().info("Distribution concluded.");
        this.getContext().getLog().info("[PARTITIONS MAINTAINED] " + this.partitions.size());
        System.gc();
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestForSelectionQuery(RequestPartitionsForSelectionQuery command) {
        ObjectList<ActorRef<ADBPartition.Command>> relevantPartitions =
                IntStream.range(0, this.partitions.size())
                         .filter(index -> this.partitionHeaders.get(index).isRelevant(command.query))
                         .mapToObj(this.partitions::get)
                         .collect(new ObjectArrayListCollector<>());
        command.respondTo.tell(new RelevantPartitionsSelectionQuery(relevantPartitions));
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestAllPartitionsAndHeaders(RequestAllPartitionsAndHeaders command) {
        command.respondTo.tell(new AllPartitionsAndHeaders(this.partitions, this.partitionHeaders));
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestForJoinQuery(RequestPartitionsForJoinQuery command) {
        int[] lPartitionIdsLeft = IntStream.range(0, this.partitions.size())
                                           .filter(id -> this.mightJoin(id, command.externalHeader, command.query))
                                           .toArray();
        int[] lPartitionIdsRight = IntStream.range(0, this.partitions.size())
                                            .filter(id -> this.mightJoin(command.externalHeader, id, command.query))
                                            .toArray();
        val relevantPartitions = IntStream.concat(Arrays.stream(lPartitionIdsLeft), Arrays.stream(lPartitionIdsRight))
                                          .mapToObj(this.partitions::get)
                                          .collect(Collectors.toSet());
        command.respondTo.tell(new RelevantPartitionsJoinQuery(relevantPartitions, command.externalHeader.getId(), lPartitionIdsLeft,
                lPartitionIdsRight));
        return Behaviors.same();
    }

    private boolean mightJoin(int index, ADBPartitionHeader b, ADBJoinQuery query) {
        return this.partitionHeaders.get(index).isOverlapping(b, query);
    }

    private boolean mightJoin(ADBPartitionHeader b, int index, ADBJoinQuery query) {
        return b.isOverlapping(this.partitionHeaders.get(index), query);
    }
}