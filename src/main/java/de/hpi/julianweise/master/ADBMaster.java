package de.hpi.julianweise.master;

import akka.actor.RootActorPath;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcess;
import de.hpi.julianweise.master.query_endpoint.ADBQueryEndpointFactory;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirerFactory;
import de.hpi.julianweise.slave.ADBSlave;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

import java.util.HashMap;
import java.util.Map;

public class ADBMaster extends AbstractBehavior<ADBMaster.Command> {

    private static final Map<RootActorPath, Integer> GLOBAL_IDS = new Object2IntLinkedOpenHashMap<>();
    private final Map<ActorRef<ADBSlave.Command>, Boolean> activeSlaveNodes = new HashMap<>();

    public interface Command {}

    public static class StartOperationalService implements Command {}

    @AllArgsConstructor
    @Getter
    public static class WrappedListing implements Command {
        private final Receptionist.Listing listing;
    }

    public static int getGlobalIdFor(ActorRef<?> target) {
        if (!GLOBAL_IDS.containsKey(target.path().root())) {
            System.out.println("[WARN] Unable to find global node ID for " + target);
            return -1;
        }
        return GLOBAL_IDS.get(target.path().root());
    }

    protected ADBMaster(ActorContext<Command> context,
                        Behavior<ADBLoadAndDistributeDataProcess.Command> loadAndDistributeProcess) {
        super(context);
        context.getLog().info("DBMaster started");
        this.getContext().spawn(loadAndDistributeProcess, "LoadAndDistribute")
            .tell(new ADBLoadAndDistributeDataProcess.Start(this.getContext().getSelf()));
        val subscriber = this.getContext().messageAdapter(Receptionist.Listing.class, WrappedListing::new);
        this.getContext().getSystem().receptionist().tell(Receptionist.subscribe(ADBSlave.SERVICE_KEY, subscriber));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartOperationalService.class, this::handleStartOperationalService)
                .onMessage(WrappedListing.class, this::handleReceptionistListing)
                .build();
    }

    private Behavior<Command> handleStartOperationalService(StartOperationalService command) {
        ActorRef<ADBPartitionInquirer.Command> shardInquirer =
                this.getContext().spawn(ADBPartitionInquirerFactory.createDefault(), "shardInquirer");
        this.getContext().spawn(ADBQueryEndpointFactory.createDefault(shardInquirer), "endpoint");
        return Behaviors.same();
    }

    private Behavior<Command> handleReceptionistListing(WrappedListing wrapper) {
        activeSlaveNodes.replaceAll((s, v) -> false);
        for(ActorRef<ADBSlave.Command> slaveNode : wrapper.listing.getAllServiceInstances(ADBSlave.SERVICE_KEY)) {
            if (!activeSlaveNodes.containsKey(slaveNode)) {
                this.getContext().getLog().info(slaveNode + " joins master");
                slaveNode.tell(new ADBSlave.Joined(activeSlaveNodes.size()));
                GLOBAL_IDS.putIfAbsent(slaveNode.path().root(), activeSlaveNodes.size());
                activeSlaveNodes.put(slaveNode, true);
            }
            activeSlaveNodes.replace(slaveNode, true);
        }
        return Behaviors.same();
    }
}
