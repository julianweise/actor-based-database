package de.hpi.julianweise.query.session.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.utility.query.join.JoinDistributionPlan;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class JoinDistributionPlanTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @AfterClass
    public static void after() {
        testKit.after();
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationABC() {
        TestProbe<ADBQueryManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(nodes);

        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeB.ref());
        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeC.ref());
        assertThat(plan.getNextJoinNodeFor(nodeC.ref())).isEqualTo(nodeA.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationACB() {
        TestProbe<ADBQueryManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(nodes);

        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeB.ref());
        assertThat(plan.getNextJoinNodeFor(nodeC.ref())).isEqualTo(nodeA.ref());
        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationBCA() {
        TestProbe<ADBQueryManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(nodes);

        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeA.ref());
        assertThat(plan.getNextJoinNodeFor(nodeC.ref())).isEqualTo(nodeB.ref());
        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationBAC() {
        TestProbe<ADBQueryManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(nodes);

        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeA.ref());
        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeC.ref());
        assertThat(plan.getNextJoinNodeFor(nodeC.ref())).isEqualTo(nodeB.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationCAB() {
        TestProbe<ADBQueryManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(nodes);

        assertThat(plan.getNextJoinNodeFor(nodeC.ref())).isEqualTo(nodeA.ref());
        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeB.ref());
        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationCBA() {
        TestProbe<ADBQueryManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(nodes);

        assertThat(plan.getNextJoinNodeFor(nodeC.ref())).isEqualTo(nodeA.ref());
        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeC.ref());
        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeB.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsLargerVolume() {
        TestProbe<ADBQueryManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeC = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeD = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());
        nodes.add(nodeD.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(nodes);

        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeB.ref()); // A:1 B:1 C:0 D:0
        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeC.ref()); // A:1 B:2 C:1 D:0
        assertThat(plan.getNextJoinNodeFor(nodeC.ref())).isEqualTo(nodeD.ref()); // A:1 B:2 C:2 D:1
        assertThat(plan.getNextJoinNodeFor(nodeD.ref())).isEqualTo(nodeA.ref()); // A:2 B:2 C:2 D:2
        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeC.ref()); // A:3 B:2 C:3 D:2
        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeD.ref()); // A:3 B:3 C:3 D:3
    }

    @Test
    public void distributionPlanIsFulfilledAfterAllPermutationsHaveBeenTested() {
        TestProbe<ADBQueryManager.Command> nodeA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeC = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> nodeD = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> nodes = new ObjectArrayList<>();
        nodes.add(nodeA.ref());
        nodes.add(nodeB.ref());
        nodes.add(nodeC.ref());
        nodes.add(nodeD.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(nodes);

        assertThat(plan.getNextJoinNodeFor(nodeA.ref())).isEqualTo(nodeB.ref());
        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeC.ref());
        assertThat(plan.getNextJoinNodeFor(nodeB.ref())).isEqualTo(nodeD.ref());
        assertThat(plan.getNextJoinNodeFor(nodeD.ref())).isEqualTo(nodeA.ref());
        assertThat(plan.getNextJoinNodeFor(nodeC.ref())).isEqualTo(nodeA.ref());
        assertThat(plan.getNextJoinNodeFor(nodeD.ref())).isEqualTo(nodeC.ref());
    }
}