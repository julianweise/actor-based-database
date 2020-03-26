package de.hpi.julianweise.shard;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.join.ADBJoinQuerySession;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandler;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandlerFactory;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Comparator;

public class ADBShard extends AbstractBehavior<ADBShard.Command> {

    public final static ServiceKey<ADBShard.Command> SERVICE_KEY = ServiceKey.create(ADBShard.Command.class, "data" +
            "-shard");
    private final ArrayList<ADBEntityType> data = new ArrayList<>();
    private int globalId;

    public interface Command extends CborSerializable {

    }
    @Getter
    @AllArgsConstructor
    public static class PersistEntity implements Command {
        private ActorRef<ADBShardDistributor.Command> respondTo;
        private ADBEntityType entity;

    }
    @Getter
    @AllArgsConstructor
    @Builder
    public static class QueryEntities implements Command {
        private int transactionId;
        private ActorRef<ADBQuerySession.Command> respondTo;
        private ADBQuery query;

    }
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ConcludeTransfer implements Command {
        private int shardId;

    }


    protected ADBShard(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PersistEntity.class, this::handleEntity)
                .onMessage(QueryEntities.class, this::handleQueryEntities)
                .onMessage(ConcludeTransfer.class, this::handleConcludeTransfer)
                .build();
    }

    private Behavior<Command> handleEntity(PersistEntity command) {
        this.data.add(command.getEntity());
        command.respondTo.tell(new ADBShardDistributor.ConfirmEntityPersisted(command.getEntity().getPrimaryKey()));
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryEntities(QueryEntities command) {
        this.getContext().getLog().info("New Query [TX #" + command.getTransactionId() + "] to match against local entities.");
        ActorRef<ADBQuerySessionHandler.Command> sessionHandler = this.getContext().spawn(
                ADBQuerySessionHandlerFactory.create(command, this.getContext().getSelf(), this.data, this.globalId),
                ADBQuerySessionHandlerFactory.sessionHandlerName(command, this.globalId));
        command.getRespondTo().tell(new ADBJoinQuerySession.UpdateShardToHandlerMapping(this.getContext().getSelf(),
                sessionHandler));
        sessionHandler.tell(new ADBQuerySessionHandler.Execute());
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeTransfer(ConcludeTransfer command) {
        this.globalId = command.shardId;
        this.getContext().getLog().info("GlobalID of this shard: " + command.shardId);
        this.getContext().getLog().info("Distribution concluded. Shard owns " + this.data.size() + " elements");
        this.data.trimToSize();
        System.gc();
        this.data.sort(Comparator.comparing(ADBEntityType::getPrimaryKey));
        return Behaviors.same();
    }
}
