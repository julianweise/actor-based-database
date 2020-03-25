package de.hpi.julianweise.shard;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.typed.ActorRef;
import akka.actor.typed.receptionist.Receptionist;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class ADBShardDistributorTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @ClassRule
    public final static TemporaryFolder folder = new TemporaryFolder();

    @AfterClass
    public static void after() {
        folder.delete();
    }

    @Before
    public void before() {
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShardFactory.createDefault());
        testKit.system().receptionist().tell(Receptionist.register(ADBShard.SERVICE_KEY, shard));
    }
}
