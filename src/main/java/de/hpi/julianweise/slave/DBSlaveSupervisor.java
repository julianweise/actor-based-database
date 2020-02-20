package de.hpi.julianweise.slave;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import de.hpi.julianweise.entity.DatabaseEntity;


public class DBSlaveSupervisor extends AbstractBehavior<Void> {

    public static Behavior<Void> create() {
        return Behaviors.setup(DBSlaveSupervisor::new);
    }

    private final ClusterSharding sharding;
    private final ActorRef<ShardingEnvelope<DatabaseEntity.Operation>> entityRef;

    private DBSlaveSupervisor(ActorContext<Void> context) {
        super(context);
        context.getLog().info("DBSlave started");

        this.sharding = ClusterSharding.get(context.getSystem());
        this.entityRef = sharding.init(Entity.of(DatabaseEntity.ENTITY_TYPE_KEY,
                ctx -> DatabaseEntity.create()));
    }

    @Override
    public Receive<Void> createReceive() {
        return newReceiveBuilder().onSignal(PostStop.class, signal -> onPostStop()).build();
    }

    private DBSlaveSupervisor onPostStop() {
        this.getContext().getLog().info("DBSlave stopped");
        return this;
    }
}
