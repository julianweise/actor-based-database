package de.hpi.julianweise.master;

import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.ActorContext;

public class DBMasterSupervisor extends AbstractBehavior<Void> {

    public static Behavior<Void> create() {
        return Behaviors.setup(DBMasterSupervisor::new);
    }

    private DBMasterSupervisor(ActorContext<Void> context) {
        super(context);
        context.getLog().info("DBMaster started");
    }

    @Override
    public Receive<Void> createReceive() {
        return newReceiveBuilder().onSignal(PostStop.class, signal -> onPostStop()).build();
    }

    private DBMasterSupervisor onPostStop() {
        this.getContext().getLog().info("DBMaster stopped");
        return this;
    }
}
