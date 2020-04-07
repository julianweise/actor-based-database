package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import lombok.SneakyThrows;

import java.net.URLEncoder;
import java.util.List;

public class ADBJoinAttributeIntersectorFactory {

    public static Behavior<ADBJoinAttributeIntersector.Command> createDefault(List<ADBKeyPair> candidates) {
        return Behaviors.setup(context -> new ADBJoinAttributeIntersector(context, candidates));
    }

    @SneakyThrows
    public static String getName(ADBJoinQuery query) {
        return "ADBJoinAttributeIntersector-for-query:" + URLEncoder.encode(query.toString(), "utf-8");
    }

}
