package de.hpi.julianweise.shard.query_operation.join.attribute_comparison;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.strategies.ADBAttributeComparisonStrategy;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.strategies.ADBOffsetAttributeComparisonStrategy;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.strategies.ADBPrimitiveAttributeComparisonStrategy;

public class ADBJoinAttributeComparatorFactory {

    public static Behavior<ADBJoinAttributeComparator.Command> createDefault() {
        return ADBJoinAttributeComparatorFactory.createOffsetComparator();
    }

    public static Behavior<ADBJoinAttributeComparator.Command> createOffsetComparator() {
        ADBAttributeComparisonStrategy strategy = new ADBOffsetAttributeComparisonStrategy();
        return Behaviors.setup(context -> new ADBJoinAttributeComparator(context, strategy));
    }

    public static Behavior<ADBJoinAttributeComparator.Command> createPrimitiveComparator() {
        ADBAttributeComparisonStrategy strategy = new ADBPrimitiveAttributeComparisonStrategy();
        return Behaviors.setup(context -> new ADBJoinAttributeComparator(context, strategy));
    }
}
