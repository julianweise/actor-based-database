package de.hpi.julianweise.query.session;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class ADBQuerySession extends AbstractBehavior<ADBQuerySession.Command> {

    public interface Command extends CborSerializable {
    }

    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    public static class ConcludeTransaction implements Command {
        private ActorRef<ADBShard.Command> shard;
        private int transactionId;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class QueryResults implements Command {
        private int transactionId;
        private List<ADBEntityType> results;
    }

    protected final int transactionId;
    protected final List<ADBEntityType> queryResults = new ArrayList<>();
    protected final ActorRef<ADBShardInquirer.Command> parent;
    protected final Set<ActorRef<ADBShard.Command>> shards;

    public ADBQuerySession(ActorContext<Command> context, Set<ActorRef<ADBShard.Command>> shards,
                           int transactionId, ActorRef<ADBShardInquirer.Command> parent) {
        super(context);
        this.shards = shards;
        this.transactionId = transactionId;
        this.parent = parent;
        this.getContext().getLog().info(String.format("Started new QuerySession %d for %s",
                this.transactionId, this.getQuerySessionName()));
    }

    protected Behavior<ADBQuerySession.Command> concludeTransaction() {
        this.parent.tell(new ADBShardInquirer.TransactionResults(this.transactionId, this.queryResults));
        this.getContext().getLog().info(String.format("Concluding QuerySessionHandler for transaction %d handling %s",
                this.transactionId, this.getQuerySessionName()));
        return Behaviors.stopped();
    }

    protected abstract String getQuerySessionName();
}
