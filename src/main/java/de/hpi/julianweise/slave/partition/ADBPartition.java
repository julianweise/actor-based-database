package de.hpi.julianweise.slave.partition;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeaderFactory;
import de.hpi.julianweise.slave.partition.meta.ADBSortedEntityAttributes2;
import de.hpi.julianweise.slave.partition.meta.ADBSortedEntityAttributes2Factory;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import de.hpi.julianweise.utility.largemessage.ADBSemiMaterializedPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ADBPartition extends AbstractBehavior<ADBPartition.Command> {

    private static final int MAX_ELEMENTS = 1 << 15;

    private final List<ADBEntity> data;
    private final Map<String, ADBSortedEntityAttributes2> sortedAttributes;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
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
        private final List<ADBEntity> data;
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
        private final Map<String, List<ADBPair<Comparable<Object>, Integer>>> attributes;
        private final int partitionId;
    }

    @AllArgsConstructor
    public static class SemiMaterializeTuples implements Command {
        private final ActorRef<SemiMaterializedTuples> respondTo;
        private final int foreignPartitionId;
        private final List<ADBKeyPair> targets;
        private final boolean reversed;
    }

    @AllArgsConstructor
    @Getter
    public static class SemiMaterializedTuples implements Response {
        private final int foreignPartitionId;
        private final List<ADBSemiMaterializedPair> results;
        private final boolean reversed;
    }

    @AllArgsConstructor
    public static class MaterializeTuples implements Command {
        private final ActorRef<MaterializedTuples> respondTo;
        private final List<ADBSemiMaterializedPair> targets;
        private final boolean reversed;
    }

    @AllArgsConstructor
    @Getter
    public static class MaterializedTuples implements Command {
        private final List<ADBPair<ADBEntity, ADBEntity>> results;
    }

    public ADBPartition(ActorContext<Command> context, int id, List<ADBEntity> data) {
        super(context);
        assert data.size() > 0;
        assert data.stream().mapToInt(ADBEntity::getSize).sum() < this.settings.MAX_SIZE_PARTITION;
        assert data.size() < MAX_ELEMENTS : "Maximum 2^16 elements allowed per partition";

        this.id = id;
        this.getContext().getLog().info("Partition maintains " + data.size() + " entities.");
        this.data = data;
        ADBPartitionHeader header = ADBPartitionHeaderFactory.createDefault(data, id);
        assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";
        ADBPartitionManager.getInstance().tell(new ADBPartitionManager.Register(this.getContext().getSelf(), header));
        this.sortedAttributes = ADBSortedEntityAttributes2Factory.of(data);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RequestData.class, this::handleProvideData)
                .onMessage(RequestJoinAttributes.class, this::handleRequestJoinAttributes)
                .onMessage(SemiMaterializeTuples.class, this::handleSemiMaterialize)
                .onMessage(MaterializeTuples.class, this::handleMaterialize)
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
                                      .collect(Collectors.toMap(ADBSortedEntityAttributes2::getField, s -> s.getMaterialized(this.data)));
        command.respondTo.tell(new JoinAttributes(attributes, this.id));
        return Behaviors.same();
    }

    private Behavior<Command> handleSemiMaterialize(SemiMaterializeTuples command) {
        command.respondTo.tell(new SemiMaterializedTuples(command.foreignPartitionId, command.targets
                .parallelStream()
                .map(tuple -> new ADBSemiMaterializedPair(tuple.getKey(), this.data.get(tuple.getValue())))
                .collect(Collectors.toList()), command.reversed));
        return Behaviors.same();
    }

    private Behavior<Command> handleMaterialize(MaterializeTuples command) {
        command.respondTo.tell(new MaterializedTuples(command.targets
                .stream()
                .map(tuple -> {
                    if (command.reversed) return new ADBPair<>(tuple.getValue(), this.data.get(tuple.getKey()));
                    return new ADBPair<>(this.data.get(tuple.getKey()), tuple.getValue());
                })
                .collect(Collectors.toList())));
        return Behaviors.same();
    }
}
