package de.hpi.julianweise.slave;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.shard.ADBShard;

public class ADBSlaveSupervisor extends AbstractBehavior<Void> {

    public static Behavior<Void> create() {
        return Behaviors.setup(ADBSlaveSupervisor::new);
    }

    private final ActorRef<ADBShard.Command> localShard;

    private ADBSlaveSupervisor(ActorContext<Void> context) {
        super(context);
        context.getLog().info("DBSlave started");
        this.localShard = this.getContext().spawn(ADBShard.create(), "ADBShard");
        context.getSystem().receptionist().tell(Receptionist.register(ADBShard.SERVICE_KEY, this.localShard));
    }

    @Override
    public Receive<Void> createReceive() {
        return newReceiveBuilder().onSignal(PostStop.class, signal -> onPostStop()).build();
    }

    private ADBSlaveSupervisor onPostStop() {
        this.getContext().getLog().info("DBSlave stopped");
        return this;
    }
}
