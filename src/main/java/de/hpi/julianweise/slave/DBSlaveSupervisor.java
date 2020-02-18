package de.hpi.julianweise.slave;

import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

public class DBSlaveSupervisor extends AbstractBehavior<Void> {

    public static Behavior<Void> create() {
        return Behaviors.setup(DBSlaveSupervisor::new);
    }

    private DBSlaveSupervisor(ActorContext<Void> context) {
        super(context);
        context.getLog().info("DBSlave started");
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
