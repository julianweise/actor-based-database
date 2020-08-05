package de.hpi.julianweise.slave.partition;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.slave.partition.column.pax.ADBColumn;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeaderFactory;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.Map;


public class ADBPartition extends AbstractBehavior<ADBPartition.Command> {

    public static final int MAX_ELEMENTS = 0x10000;

    private final int id;
    private final Class<? extends ADBEntity> schema;
    private final Map<String, ADBColumn> columns;

    public interface Command {}

    public interface Response {}

    @AllArgsConstructor
    public static class RequestData implements Command {
        private final ActorRef<ADBPartition.Data> respondTo;
    }

    @AllArgsConstructor
    @Getter
    public static class Data implements Response {
        private final Map<String, ADBColumn> data;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Builder
    public static class RequestMultipleAttributesFiltered implements Command, CborSerializable {
        private ActorRef<ADBLargeMessageActor.Command> respondTo;
        private String[] attributes;
        private ADBEntityEntry[] minValues;
        private ADBEntityEntry[] maxValues;
        private boolean isLeft;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class MultipleAttributes implements Response, ADBLargeMessageSender.LargeMessage {
        private Map<String, ADBColumnSorted> attributes;
        private Object2IntMap<String> originalSize;
        private boolean isLeft;
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

    @AllArgsConstructor
    @Getter
    public static class MessageSenderResponse implements Command, CborSerializable {
        private final ADBLargeMessageSender.Response response;
    }

    public ADBPartition(ActorContext<Command> context, int id, Map<String, ADBColumn> columns, Class<? extends ADBEntity> schema) {
        super(context);
        this.id = id;
        this.schema = schema;
        this.columns = columns;

        ADBPartitionHeader header = ADBPartitionHeaderFactory.createDefault(this.columns, id);
        assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";
        this.getContext().getLog().info("Partition {} maintains {} elements", this.id, this.numberOfElements());
        ADBPartitionManager.getInstance().tell(new ADBPartitionManager.Register(this.getContext().getSelf(), header));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RequestData.class, this::handleProvideData)
                .onMessage(RequestMultipleAttributesFiltered.class, this::handleRequestMultipleAttributesFiltered)
                .onMessage(MaterializeToEntities.class, this::handleMaterialize)
                .onMessage(MessageSenderResponse.class, (cmd) -> this)
                .build();
    }

    private Behavior<Command> handleProvideData(RequestData command) {
        command.respondTo.tell(new Data(this.columns));
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestMultipleAttributesFiltered(RequestMultipleAttributesFiltered command) {
        Map<String, ADBColumnSorted> attributeValues = new Object2ObjectOpenHashMap<>();
        Object2IntMap<String> originalSize = new Object2IntLinkedOpenHashMap<>();
        for(int i = 0; i < command.attributes.length; i++) {
            String attribute = command.attributes[i];
            attributeValues.put(attribute, getFilteredValuesOf(attribute, command.minValues[i], command.maxValues[i]));
            originalSize.put(attribute, this.columns.get(attribute).size());
        }
        val message = new MultipleAttributes(attributeValues, originalSize, command.isLeft);
        val respondTo = getContext().messageAdapter(ADBLargeMessageSender.Response.class, MessageSenderResponse::new);
        ADBLargeMessageActor.sendMessage(this.getContext(), Adapter.toClassic(command.respondTo), respondTo, message);
        return Behaviors.same();
    }

    private ADBColumnSorted getFilteredValuesOf(String attribute, ADBEntityEntry min, ADBEntityEntry max) {
        return this.columns.get(attribute).getSortedColumn(min, max);
    }

    private Behavior<Command> handleMaterialize(MaterializeToEntities command) {
        assert command.internalIds.stream().anyMatch(id -> this.id != ADBInternalIDHelper.getPartitionId(id)) :
                "Entities belonging to different Partition";
        ObjectList<ADBEntity> materializedResults = command.internalIds
                .parallelStream()
                .map(internalId -> ADBEntity.fromColumns(this.columns, internalId, this.schema))
                .collect(new ObjectArrayListCollector<>());
        command.respondTo.tell(new MaterializedEntities(materializedResults));
        return Behaviors.same();
    }

    private int numberOfElements() {
        for(Map.Entry<String, ADBColumn> column : this.columns.entrySet()) {
            return column.getValue().size();
        }
        return 0;
    }
}
