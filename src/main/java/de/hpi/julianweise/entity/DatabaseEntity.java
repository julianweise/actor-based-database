package de.hpi.julianweise.entity;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import de.hpi.julianweise.utility.CborSerializable;


public class DatabaseEntity extends AbstractBehavior<DatabaseEntity.Command> {

    public static final EntityTypeKey<Command> TypeKey =
            EntityTypeKey.create(DatabaseEntity.Command.class, "DatabaseEntity");

    public static void initSharding(ActorSystem<?> system) {
        ClusterSharding.get(system).init(Entity.of(TypeKey, entityContext ->
                DatabaseEntity.create(entityContext.getEntityId())
        ));
    }

    public static Behavior<Command> create(String id) {
        return Behaviors.setup(context ->
                new DatabaseEntity(context, id)
        );
    }

    private final String id;

    public DatabaseEntity(ActorContext<Command> context, String id) {
        super(context);
        this.id = id;
    }

    @Override public Receive<Command> createReceive() {
        return null;
    }

    interface Command extends CborSerializable {}

}
