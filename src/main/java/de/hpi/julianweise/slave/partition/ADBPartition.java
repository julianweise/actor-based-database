package de.hpi.julianweise.slave.partition;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeaderFactory;
import de.hpi.julianweise.slave.partition.meta.ADBSortedEntityAttributes;
import de.hpi.julianweise.slave.partition.meta.ADBSortedEntityAttributesFactory;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.Map;
import java.util.stream.Collectors;


public class ADBPartition extends AbstractBehavior<ADBPartition.Command> {

    private static final int MAX_ELEMENTS = 0x10000;

    private final ObjectList<ADBEntity> data;
    private final Map<String, ADBSortedEntityAttributes> sortedAttributes;
    private final int id;

    public interface Command {
    }

    public interface Response {
    }

    @AllArgsConstructor
    public static class RequestData implements Command {
        private final ActorRef<ADBPartition.Data> respondTo;
    }

    @AllArgsConstructor
    @Getter
    public static class Data implements Response {
        private final ObjectList<ADBEntity> data;
    }

    @AllArgsConstructor
    @Getter
    public static class RequestJoinAttributes implements Command {
        private final ActorRef<ADBPartition.JoinAttributes> respondTo;
        private final ADBJoinQuery query;
    }

    @AllArgsConstructor
    @Getter
    public static class JoinAttributes implements Response {
        private final Map<String, ObjectList<ADBComparable2IntPair>> attributes;
        private final int partitionId;
    }

    @AllArgsConstructor
    public static class MaterializeToEntities implements Command {
        private final IntList internalIds;
        private final ActorRef<MaterializedEntities> respondTo;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class MaterializedEntities implements Command, KryoSerializable {
        private ObjectList<ADBEntity> results;
    }

    public ADBPartition(ActorContext<Command> context, int id, ObjectList<ADBEntity> data) {
        super(context);
        assert data.size() > 0;
        assert data.stream().mapToInt(ADBEntity::getSize).sum() < Settings.SettingsProvider.get(getContext().getSystem()).MAX_SIZE_PARTITION;
        assert data.size() < MAX_ELEMENTS : "Maximum 2^16 elements allowed per partition";
        assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";

        this.getContext().getLog().info("Partition maintains " + data.size() + " entities.");

        this.id = id;
        this.data = data;
        this.sortedAttributes = ADBSortedEntityAttributesFactory.of(data);

        ADBPartitionHeader header = ADBPartitionHeaderFactory.createDefault(data, id);
        ADBPartitionManager.getInstance().tell(new ADBPartitionManager.Register(this.getContext().getSelf(), header));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RequestData.class, this::handleProvideData)
                .onMessage(RequestJoinAttributes.class, this::handleRequestJoinAttributes)
                .onMessage(MaterializeToEntities.class, this::handleMaterialize)
                .build();
    }

    private Behavior<Command> handleProvideData(RequestData command) {
        command.respondTo.tell(new Data(this.data));
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestJoinAttributes(RequestJoinAttributes command) {
        val attributes = command.query.getAllFields()
                                      .stream()
                                      .map(this.sortedAttributes::get)
                                      .collect(Collectors.toMap(ADBSortedEntityAttributes::getField, s -> s.getMaterialized(this.data)));
        command.respondTo.tell(new JoinAttributes(attributes, this.id));
        return Behaviors.same();
    }

    private Behavior<Command> handleMaterialize(MaterializeToEntities command) {
        assert command.internalIds.stream().filter(id -> this.id != ADBInternalIDHelper.getPartitionId(id)).count() < 1: "Entities belonging to different Partition";
        ObjectList<ADBEntity> materializedResults = command.internalIds.parallelStream()
                .map(internalId -> this.data.get(ADBInternalIDHelper.getEntityId(internalId)))
                .collect(new ObjectArrayListCollector<>());
        command.respondTo.tell(new MaterializedEntities(materializedResults));
        return Behaviors.same();
    }
}
