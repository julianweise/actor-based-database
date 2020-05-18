package de.hpi.julianweise.slave.query.join.steps;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModel;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;

import java.util.List;
import java.util.Map;

public class ADBColumnJoinStepExecutorFactory {

    public static Behavior<ADBColumnJoinStepExecutor.Command> createDefault(
            Map<String, List<ADBComparable2IntPair>> left,
            Map<String, List<ADBComparable2IntPair>> right,
            List<ADBJoinTermCostModel> costModels,
            ActorRef<ADBColumnJoinStepExecutor.StepExecuted> respondTo
    ) {
        return Behaviors.setup(context -> new ADBColumnJoinStepExecutor(context, left ,right, costModels, respondTo));
    }
}
