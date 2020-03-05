package de.hpi.julianweise.shard;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.shard.queryOperation.ADBQueryOperationHandler;
import de.hpi.julianweise.shard.queryOperation.ADBQueryOperationHandlerFactory;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ADBShard extends AbstractBehavior<ADBShard.Command> {

    public interface Command extends CborSerializable {}

    @Getter
    @AllArgsConstructor
    public static class PersistEntity implements Command {
        private ActorRef<ADBShardDistributor.Command> respondTo;
        private ADBEntityType entity;
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class QueryEntities implements Command {
        int transactionId;
        private ActorRef<ADBShardInquirer.Command> respondTo;
        private ADBSelectionQuery query;
    }

    public static ServiceKey<ADBShard.Command> SERVICE_KEY = ServiceKey.create(ADBShard.Command.class, "data-shard");

    private final Map<ADBKey, ADBEntityType> data = new HashMap<>();


    protected ADBShard(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PersistEntity.class, this::handleEntity)
                .onMessage(QueryEntities.class, this::handleQueryEntities)
                .build();
    }

    private Behavior<Command> handleEntity(PersistEntity command) {
        this.data.put(command.getEntity().getPrimaryKey(), command.getEntity());
        command.respondTo.tell(new ADBShardDistributor.ConfirmEntityPersisted(command.getEntity().getPrimaryKey()));
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryEntities(QueryEntities command) {
        int transactionId = command.getTransactionId();
        this.getContext().spawn(ADBQueryOperationHandlerFactory.create(command, Collections.unmodifiableMap(this.data)),
                "queryEntities-" + transactionId)
            .tell(new ADBQueryOperationHandler.Execute());
        return Behaviors.same();
    }
}
