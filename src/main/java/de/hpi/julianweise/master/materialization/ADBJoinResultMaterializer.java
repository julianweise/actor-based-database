package de.hpi.julianweise.master.materialization;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.google.common.collect.Streams;
import de.hpi.julianweise.slave.partition.ADBPartition.MaterializedEntities;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.query.join.ADBPartialJoinResult;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import de.hpi.julianweise.utility.list.IntArrayListCollector;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static java.util.stream.Collectors.groupingBy;

public class ADBJoinResultMaterializer extends AbstractBehavior<ADBJoinResultMaterializer.Command> {

    public interface Command {}
    public interface Response {}

    private final int MATERIALIZATION_THRESHOLD = 1000;

    @AllArgsConstructor
    public static class MaterializeJoinResultChunk implements Command {
        private final ADBPartialJoinResult results;
    }

    @AllArgsConstructor
    public static class MaterializedEntitiesWrapper implements Command {
        private final MaterializedEntities entities;
    }

    @AllArgsConstructor
    public static class Conclude implements Command {}

    @AllArgsConstructor
    @Getter
    public static class MaterializedResults implements Response {
        private final ObjectList<ADBPair<ADBEntity, ADBEntity>> tuples;
    }

    @AllArgsConstructor
    @Getter
    public static class Concluded implements Response {}

    private final IntArrayList unmaterializedLeft = new IntArrayList();
    private final IntArrayList unmaterializedRight = new IntArrayList();
    private final IntSet materializationRequested = new IntOpenHashSet();
    private final Int2ObjectMap<ADBEntity> materializedEntities = new Int2ObjectOpenHashMap<>();
    private final ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers;
    private final ActorRef<Response> supervisor;
    private final ActorRef<MaterializedEntities> materializedRef;
    private boolean conclude = false;

    public ADBJoinResultMaterializer(ActorContext<Command> context,
                                     ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers,
                                     ActorRef<Response> supervisor) {
        super(context);
        this.materializedRef = getContext().messageAdapter(MaterializedEntities.class, MaterializedEntitiesWrapper::new);
        this.partitionManagers = partitionManagers;
        this.supervisor = supervisor;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(MaterializeJoinResultChunk.class, this::handleMaterializeJoinResultChunk)
                .onMessage(MaterializedEntitiesWrapper.class, this::handleMaterializedEntities)
                .onMessage(Conclude.class, this::handleConclude)
                .build();
    }

    private Behavior<Command> handleConclude(Conclude command) {
        this.conclude = true;
        return Behaviors.same();
    }

    private Behavior<Command> handleMaterializeJoinResultChunk(MaterializeJoinResultChunk command) {
        for (ADBKeyPair keyPair : command.results) {
            this.unmaterializedLeft.add(keyPair.getKey());
            this.unmaterializedRight.add(keyPair.getValue());
        }
        this.triggerMaterialization();
        return Behaviors.same();
    }

    private void triggerMaterialization() {
        if (Math.min(this.unmaterializedLeft.size(), this.unmaterializedRight.size()) < MATERIALIZATION_THRESHOLD) {
            return;
        }
        Streams.concat(this.unmaterializedLeft.stream(), this.unmaterializedRight.stream())
               .collect(groupingBy(ADBInternalIDHelper::getNodeId, new IntArrayListCollector()))
               .forEach(this::requestMaterializedEntitiesFromNode);
    }

    private void requestMaterializedEntitiesFromNode(int nodeId, IntList intIds) {
        IntList filteredIds = new IntArrayList();
        for(int id : intIds) {
            if (this.materializationRequested.contains(id)) continue;
            if (this.materializedEntities.containsKey(id)) continue;
            this.materializationRequested.add(id);
            filteredIds.add(id);
        }
        partitionManagers.get(nodeId).tell(new ADBPartitionManager.MaterializeToEntities(materializedRef, filteredIds));
    }

    private Behavior<Command> handleMaterializedEntities(MaterializedEntitiesWrapper wrapper) {
        for (ADBEntity entity : wrapper.entities.getResults()) {
            this.materializedEntities.put(entity.getInternalID(), entity);
            this.materializationRequested.remove(entity.getInternalID());
        }
        this.materialize(wrapper.entities.getResults());
        return this.conditionallyConclude();
    }

    private void materialize(ObjectList<ADBEntity> materialized) {
        ObjectList<ADBPair<ADBEntity, ADBEntity>> results = new ObjectArrayList<>(materialized.size());
        for (int i = 0; i < Math.min(this.unmaterializedLeft.size(), this.unmaterializedRight.size()); i++) {
            if (!this.isJoinPairMaterializable(this.unmaterializedLeft.getInt(i), this.unmaterializedRight.getInt(i))) {
                continue;
            }
            results.add(materializePair(unmaterializedLeft.removeInt(i), unmaterializedRight.removeInt(i)));
        }
        supervisor.tell(new MaterializedResults(results));
    }

    private boolean isJoinPairMaterializable(int leftId, int rightId) {
        return materializedEntities.containsKey(leftId) && materializedEntities.containsKey(rightId);
    }

    private ADBPair<ADBEntity, ADBEntity> materializePair(int leftId, int rightId) {
        assert this.materializedEntities.containsKey(leftId) : "Cannot mater. pair with unmapped key ID";
        assert this.materializedEntities.containsKey(rightId) : "Cannot mater. pair with unmapped value ID";
        return new ADBPair<>(materializedEntities.get(leftId), materializedEntities.get(rightId));
    }

    private Behavior<Command> conditionallyConclude() {
        if (!this.conclude && !this.unmaterializedLeft.isEmpty() && !this.unmaterializedRight.isEmpty()) {
            return Behaviors.same();
        }
        this.supervisor.tell(new Concluded());
        return Behaviors.stopped();
    }
}
