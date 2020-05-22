package de.hpi.julianweise.slave.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

public class ADBQueryManager extends AbstractBehavior<ADBQueryManager.Command> {

    public static final ServiceKey<Command> SERVICE_KEY = ServiceKey.create(ADBQueryManager.Command.class, "QueryManager");
    private static ActorRef<GenericWorker.Command> WORKER_POOL;
    private static ActorRef<ADBQueryManager.Command> INSTANCE;

    public interface Command {}

    public static ActorRef<ADBQueryManager.Command> getInstance() {
        return INSTANCE;
    }

    public static ActorRef<GenericWorker.Command> getWorkerPool() {
        return WORKER_POOL;
    }

    public static void setInstance(ActorRef<ADBQueryManager.Command> queryManager) {
        assert INSTANCE == null : "Instance has already been created";
        INSTANCE = queryManager;
    }

    public static void setWorkerPool(ActorRef<GenericWorker.Command> pool) {
        assert WORKER_POOL == null : "Pool has already been created";
        WORKER_POOL = pool;
    }

    public static void resetSingleton() {
        INSTANCE = null;
    }
    public static void resetPool() {
        WORKER_POOL = null;
    }

    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class QueryEntities implements Command, CborSerializable {
        private int transactionId;
        private ActorRef<ADBMasterQuerySession.Command> respondTo;
        private ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver;
        private ADBQuery query;
    }

    @AllArgsConstructor
    public static class SessionHandlerTerminated implements Command {
        ActorRef<ADBSlaveQuerySession.Command> handler;
    }

    public ADBQueryManager(ActorContext<Command> context) {
        super(context);
        getContext().getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, this.getContext().getSelf()));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(QueryEntities.class, this::handleQueryEntities)
                .onMessage(SessionHandlerTerminated.class, this::handleQuerySessionHandlerTerminated)
                .build();
    }

    private Behavior<Command> handleQueryEntities(QueryEntities cm) {
        this.getContext().getLog().info("New Query [TX #" + cm.getTransactionId() + "] received");
        String handlerName = ADBSlaveQuerySessionFactory.getName(cm);
        val sessionHandler = this.getContext().spawn(ADBSlaveQuerySessionFactory.create(cm), handlerName);
        this.getContext().watchWith(sessionHandler, new SessionHandlerTerminated(sessionHandler));
        sessionHandler.tell(new ADBSlaveQuerySession.Execute());
        return Behaviors.same();
    }

    private Behavior<Command> handleQuerySessionHandlerTerminated(SessionHandlerTerminated message) {
        return Behaviors.same();
    }
}
