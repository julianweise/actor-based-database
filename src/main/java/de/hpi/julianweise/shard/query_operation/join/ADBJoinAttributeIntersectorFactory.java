package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import javafx.util.Pair;

import java.util.List;

public class ADBJoinAttributeIntersectorFactory {

    public static Behavior<ADBJoinAttributeIntersector.Command> createDefault(List<Pair<Integer, Integer>> candidates) {
        return Behaviors.setup(context -> new ADBJoinAttributeIntersector(context, candidates));
    }

    public static String getName(String attributeName) {
        return "ADBJoinAttributeIntersector-for-attribute:" + attributeName;
    }

}
