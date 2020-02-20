package de.hpi.julianweise.entity;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import de.hpi.julianweise.master.DBMasterSupervisor;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;


public class DatabaseEntity extends AbstractBehavior<DatabaseEntity.Operation> {

    public interface Operation {
        ActorRef<DBMasterSupervisor.Response> getClient();
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class UpdateOperation implements Operation, CborSerializable {
        private ActorRef<DBMasterSupervisor.Response> client;
        private List<Tuple<String, String>> tuples;
        private String id;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OperationSuccessful implements DBMasterSupervisor.Response, CborSerializable {
        private String id;
    }

    public static EntityTypeKey<DatabaseEntity.Operation> ENTITY_TYPE_KEY =
            EntityTypeKey.create(DatabaseEntity.Operation.class,
                    "DatabaseEntity");

    public static Behavior<DatabaseEntity.Operation> create() {
        return Behaviors.setup(DatabaseEntity::new);
    }

    private List<Tuple<String, String>> tuples = new ArrayList<>();
    private DatabaseEntity(ActorContext<Operation> actorContext ) {
        super(actorContext);
    }

    @Override
    public Receive<Operation> createReceive() {
        return newReceiveBuilder()
                .onMessage(UpdateOperation.class, this::handleUpdate)
                .build();
    }

    private Behavior<Operation> handleUpdate(UpdateOperation operation) {
        this.tuples = operation.getTuples();
        operation.getClient().tell(new OperationSuccessful(operation.getId()));
        return this;
    }
}
