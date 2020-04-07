package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQueryTerm;

public class ADBJoinTermComparatorFactory {

    public static Behavior<ADBJoinTermComparator.Command> createDefault(ADBJoinQueryTerm term,
                                                                        ActorRef<ADBJoinQueryComparator.Command> supervisor,
                                                                        ActorRef<ADBJoinAttributeComparator.Command> comparatorPool) {
        return Behaviors.setup(context -> new ADBJoinTermComparator(context, term, supervisor, comparatorPool));
    }
}
