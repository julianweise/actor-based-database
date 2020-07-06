package de.hpi.julianweise.query.session.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.utility.query.join.JoinExecutionPlan;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.var;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class JoinExecutionPlanTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();
    
    private final int transactionId = 1;

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
        ADBQueryManager.resetSingleton();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @AfterClass
    public static void after() {
        testKit.after();
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationABC() {
        TestProbe<ADBPartitionManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeC = testKit.createTestProbe();

        TestProbe<JoinExecutionPlan.NextJoinNodePair> responseProbe = testKit.createTestProbe();

        ObjectList<ActorRef<ADBPartitionManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        ActorRef<JoinExecutionPlan.Command> plan = testKit.spawn(JoinExecutionPlan.createDefault(nodes, transactionId));

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        var response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeB.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeC.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeC.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationACB() {
        TestProbe<ADBPartitionManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeC = testKit.createTestProbe();

        TestProbe<JoinExecutionPlan.NextJoinNodePair> responseProbe = testKit.createTestProbe();

        ObjectList<ActorRef<ADBPartitionManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        ActorRef<JoinExecutionPlan.Command> plan = testKit.spawn(JoinExecutionPlan.createDefault(nodes, transactionId));

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        var response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeB.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeC.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeC.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeC.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationBCA() {
        TestProbe<ADBPartitionManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeC = testKit.createTestProbe();

        TestProbe<JoinExecutionPlan.NextJoinNodePair> responseProbe = testKit.createTestProbe();

        ObjectList<ActorRef<ADBPartitionManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        ActorRef<JoinExecutionPlan.Command> plan = testKit.spawn(JoinExecutionPlan.createDefault(nodes, transactionId));

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        var response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeC.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeC.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeB.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeC.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationBAC() {
        TestProbe<ADBPartitionManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeC = testKit.createTestProbe();

        TestProbe<JoinExecutionPlan.NextJoinNodePair> responseProbe = testKit.createTestProbe();

        ObjectList<ActorRef<ADBPartitionManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        ActorRef<JoinExecutionPlan.Command> plan = testKit.spawn(JoinExecutionPlan.createDefault(nodes, transactionId));

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        var response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeC.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeC.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeB.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationCAB() {
        TestProbe<ADBPartitionManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeC = testKit.createTestProbe();

        TestProbe<JoinExecutionPlan.NextJoinNodePair> responseProbe = testKit.createTestProbe();

        ObjectList<ActorRef<ADBPartitionManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        ActorRef<JoinExecutionPlan.Command> plan = testKit.spawn(JoinExecutionPlan.createDefault(nodes, transactionId));

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeC.ref(), responseProbe.ref()));
        var response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeC.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeC.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeB.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationCBA() {
        TestProbe<ADBPartitionManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeC = testKit.createTestProbe();

        TestProbe<JoinExecutionPlan.NextJoinNodePair> responseProbe = testKit.createTestProbe();

        ObjectList<ActorRef<ADBPartitionManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        ActorRef<JoinExecutionPlan.Command> plan = testKit.spawn(JoinExecutionPlan.createDefault(nodes, transactionId));

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeC.ref(), responseProbe.ref()));
        var response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeC.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeC.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeB.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsLargerVolume() {
        TestProbe<ADBPartitionManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeC = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeD = testKit.createTestProbe();

        TestProbe<JoinExecutionPlan.NextJoinNodePair> responseProbe = testKit.createTestProbe();

        ObjectList<ActorRef<ADBPartitionManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());
        nodes.add(nodeD.ref());

        ActorRef<JoinExecutionPlan.Command> plan = testKit.spawn(JoinExecutionPlan.createDefault(nodes, transactionId));

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        var response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeB.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeC.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeC.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeD.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeC.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeD.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeD.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeD.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeD.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());
    }

    @Test
    public void distributionPlanIsFulfilledAfterAllPermutationsHaveBeenTested() {
        TestProbe<ADBPartitionManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeC = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> nodeD = testKit.createTestProbe();

        TestProbe<JoinExecutionPlan.NextJoinNodePair> responseProbe = testKit.createTestProbe();

        ObjectList<ActorRef<ADBPartitionManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());
        nodes.add(nodeD.ref());

        ActorRef<JoinExecutionPlan.Command> plan = testKit.spawn(JoinExecutionPlan.createDefault(nodes, transactionId));

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeA.ref(), responseProbe.ref()));
        var response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeA.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeB.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeA.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeB.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeB.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeD.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeB.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeD.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeD.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeD.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeC.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeC.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeA.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeC.ref());

        plan.tell(new JoinExecutionPlan.GetNextJoinNodePair(nodeD.ref(), responseProbe.ref()));
        response = responseProbe.expectMessageClass(JoinExecutionPlan.NextJoinNodePair.class);
        assertThat(response.isHasNode()).isTrue();
        assertThat(response.getRequestingPartitionManager()).isEqualTo(nodeD.ref());
        assertThat(response.getRightQueryManager()).isEqualTo(nodeC.ref());
        assertThat(response.getLeftQueryManager()).isEqualTo(nodeD.ref());
    }
}