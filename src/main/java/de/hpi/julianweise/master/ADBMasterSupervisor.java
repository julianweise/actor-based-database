package de.hpi.julianweise.master;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.ADBShardInquirerFactory;
import de.hpi.julianweise.queryEndpoint.ADBQueryEndpoint;
import de.hpi.julianweise.queryEndpoint.ADBQueryEndpointFactory;

public class ADBMasterSupervisor extends AbstractBehavior<ADBMasterSupervisor.Command> {

    public interface Command {
    }

    public static class StartOperationalService implements Command {
    }

    private final MasterConfiguration configuration;
    private final ActorRef<ADBLoadAndDistributeDataProcess.Command> loadAndDistributeProcessActor;
    private ActorRef<ADBQueryEndpoint.Command> queryEndpoint;
    private ActorRef<ADBShardInquirer.Command> shardInquirer;


    protected ADBMasterSupervisor(ActorContext<Command> context, MasterConfiguration configuration,
                                  Behavior<ADBLoadAndDistributeDataProcess.Command> loadAndDistributeProcess) {
        super(context);
        context.getLog().info("DBMaster started");
        this.configuration = configuration;
        this.loadAndDistributeProcessActor = this.getContext().spawn(loadAndDistributeProcess, "LoadAndDistribute");
        this.loadAndDistributeProcessActor.tell(new ADBLoadAndDistributeDataProcess.Start(this.getContext().getSelf()));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, this::handlePostStop)
                .onMessage(StartOperationalService.class, this::handleStartOperationalService)
                .build();
    }

    private Behavior<Command> handleStartOperationalService(StartOperationalService command) {
        this.shardInquirer = this.getContext().spawn(ADBShardInquirerFactory.createDefault(), "shardInquirer");
        this.queryEndpoint = this.getContext().spawn(ADBQueryEndpointFactory.createDefault(this.shardInquirer),
                "endpoint");
        return Behaviors.same();
    }

    private Behavior<Command> handlePostStop(PostStop signal) {
        return Behaviors.same();
    }
}
