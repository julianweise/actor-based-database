package de.hpi.julianweise.slave;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.slave.partition.ADBPartitionManagerFactory;
import de.hpi.julianweise.slave.query.ADBQueryManagerFactory;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

public class ADBSlave extends AbstractBehavior<ADBSlave.Command> {

    public static final ServiceKey<ADBSlave.Command> SERVICE_KEY = ServiceKey.create(Command.class, "Slave");
    public static int ID;

    public interface Command {
    }

    @AllArgsConstructor
    @NoArgsConstructor
    public static class Joined implements Command, KryoSerializable {
        int globalId;
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(ADBSlave::new);
    }

    private ADBSlave(ActorContext<Command> context) {
        super(context);
        context.getLog().info("DBSlave started");
        ADBPartitionManagerFactory.createSingleton(context);
        ADBQueryManagerFactory.createSingleton(context);
        this.getContext().getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, getContext().getSelf()));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Joined.class, this::handleJoined)
                .build();
    }

    private Behavior<Command> handleJoined(Joined command) {
        ADBSlave.ID = command.globalId;
        this.getContext().getLog().info("GLOBAL ID: {}", command.globalId);
        return Behaviors.same();
    }
}
