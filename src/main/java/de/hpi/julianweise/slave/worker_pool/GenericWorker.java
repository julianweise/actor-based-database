package de.hpi.julianweise.slave.worker_pool;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.slave.worker_pool.workload.Workload;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class GenericWorker extends AbstractBehavior<GenericWorker.Command> {

    public static Behavior<Command> create() {
        return Behaviors.setup(GenericWorker::new);
    }

    public interface Command {}

    public interface Response {}

    @AllArgsConstructor
    @Getter
    public static class WorkloadMessage implements Command {
        ActorRef<Response> respondTo;
        Workload workload;
    }

    public GenericWorker(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WorkloadMessage.class, this::handleWorkload)
                .build();
    }

    private Behavior<Command> handleWorkload(WorkloadMessage command) {
        command.workload.execute(command);
        return Behaviors.same();
    }


}
