package de.hpi.julianweise.shard;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.Routers;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandler;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandlerFactory;
import de.hpi.julianweise.shard.query_operation.join.ADBSortedEntityAttributes;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.ADBJoinAttributeComparator;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.ADBJoinAttributeComparatorFactory;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ADBShard extends AbstractBehavior<ADBShard.Command> {

    public final static ServiceKey<ADBShard.Command> SERVICE_KEY = ServiceKey.create(ADBShard.Command.class, "data" +
            "-shard");
    private Set<ADBEntity> transferData = new HashSet<>();
    private ArrayList<ADBEntity> data = new ArrayList<>();
    private Map<String, ADBSortedEntityAttributes> sortedAttributes;
    private final ActorRef<ADBJoinAttributeComparator.Command> comparatorPool;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());

    private int globalId;

    public interface Command extends CborSerializable {

    }
    @Getter
    @AllArgsConstructor
    public static class PersistEntity implements Command {
        private final ActorRef<ADBShardDistributor.Command> respondTo;
        private final ADBEntity entity;

    }
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class QueryEntities implements Command {
        private int transactionId;
        private ActorRef<ADBQuerySession.Command> respondTo;
        private ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver;
        private ADBQuery query;

    }
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ConcludeTransfer implements Command {
        private int shardId;
    }


    protected ADBShard(ActorContext<Command> context) {
        super(context);
        this.comparatorPool = this.createComparatorPool();
    }

    private ActorRef<ADBJoinAttributeComparator.Command> createComparatorPool() {
        return this.getContext().spawn(Routers.pool(this.settings.NUMBER_OF_THREADS,
                Behaviors.supervise(ADBJoinAttributeComparatorFactory
                .createDefault()).onFailure(SupervisorStrategy.restart())), "join-comparator-pool");
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PersistEntity.class, this::handleEntity)
                .onMessage(QueryEntities.class, this::handleQueryEntities)
                .onMessage(ConcludeTransfer.class, this::handleConcludeTransfer)
                .build();
    }

    private Behavior<Command> handleEntity(PersistEntity command) {
        this.transferData.add(command.getEntity());
        command.respondTo.tell(new ADBShardDistributor.ConfirmEntityPersisted(command.getEntity().getPrimaryKey()));
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryEntities(QueryEntities command) {
        this.getContext().getLog().info("New Query [TX #" + command.getTransactionId() + "] to match against local entities.");
        ActorRef<ADBQuerySessionHandler.Command> sessionHandler = this.getContext().spawn(
                ADBQuerySessionHandlerFactory.create(command, this.getContext().getSelf(), this.data, this.globalId,
                        this.sortedAttributes, this.comparatorPool),
                ADBQuerySessionHandlerFactory.sessionHandlerName(command, this.globalId));
        sessionHandler.tell(new ADBQuerySessionHandler.Execute());
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeTransfer(ConcludeTransfer command) {
        this.globalId = command.shardId;
        this.data = new ArrayList<>(this.transferData);
        this.transferData = null;
        this.getContext().getLog().info("GlobalID of this shard: " + command.shardId);
        this.getContext().getLog().info("Distribution concluded. Shard owns " + this.data.size() + " elements");
        this.data.trimToSize();
        this.data.sort(Comparator.comparing(ADBEntity::getPrimaryKey));
        this.sortedAttributes = ADBSortedEntityAttributes.of(this.data);
        return Behaviors.same();
    }
}
