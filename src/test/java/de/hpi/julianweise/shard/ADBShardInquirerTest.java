package de.hpi.julianweise.shard;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.ADBShardInquirerFactory;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.csv.TestEntity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBShardInquirerTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
    }

    @AfterClass
    public static void after() {
        testKit.after();
    }

    @Test
    public void testDistributeQuerySuccessfully() {
        int requestId = 1;
        TestProbe<ADBShardInquirer.Response> resultProbe = testKit.createTestProbe();

        TestProbe<Receptionist.Listing> receptionistProbe = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.subscribe(ADBShard.SERVICE_KEY, receptionistProbe.ref()));

        TestProbe<ADBShard.Command> testProbe = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.register(ADBShard.SERVICE_KEY, testProbe.ref()));
        ActorRef<ADBShardInquirer.Command> inquirer = testKit.spawn(ADBShardInquirerFactory.createDefault());

        // necessary to ensure receptionist registration propagates successfully before querying shards
        receptionistProbe.receiveSeveralMessages(2);

        ADBSelectionQuery query = new ADBSelectionQuery();

        inquirer.tell(ADBShardInquirer.QueryShards.builder()
                                                  .requestId(requestId)
                                                  .query(query)
                                                  .respondTo(resultProbe.ref())
                                                  .build());

        ADBShard.Command queryCommand = testProbe.receiveMessage();
        ADBShard.QueryEntities queryForShards = (ADBShard.QueryEntities) queryCommand;

        assertThat(queryForShards.getQuery()).isEqualTo(query);
    }

    @Test
    public void testDistributeQueryToManyShardsSuccessfully() {
        int requestId = 1;

        TestProbe<ADBShardInquirer.Response> resultProbe = testKit.createTestProbe();

        TestProbe<Receptionist.Listing> receptionistProbe = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.subscribe(ADBShard.SERVICE_KEY, receptionistProbe.ref()));

        TestProbe<ADBShard.Command> testProbe1 = testKit.createTestProbe();
        TestProbe<ADBShard.Command> testProbe2 = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.register(ADBShard.SERVICE_KEY, testProbe1.ref()));
        testKit.system().receptionist().tell(Receptionist.register(ADBShard.SERVICE_KEY, testProbe2.ref()));
        ActorRef<ADBShardInquirer.Command> inquirer = testKit.spawn(ADBShardInquirerFactory.createDefault());

        // necessary to ensure receptionist registration propagates successfully before querying shards
        receptionistProbe.receiveSeveralMessages(2);

        ADBSelectionQuery query = new ADBSelectionQuery();

        inquirer.tell(ADBShardInquirer.QueryShards.builder()
                                                  .requestId(requestId)
                                                  .query(query)
                                                  .respondTo(resultProbe.ref())
                                                  .build());

        ADBShard.Command queryCommand1 = testProbe1.receiveMessage();
        ADBShard.QueryEntities queryForShards1 = (ADBShard.QueryEntities) queryCommand1;

        ADBShard.Command queryCommand2 = testProbe2.receiveMessage();
        ADBShard.QueryEntities queryForShards2 = (ADBShard.QueryEntities) queryCommand2;

        assertThat(queryForShards1.getQuery()).isEqualTo(query);
        assertThat(queryForShards2.getQuery()).isEqualTo(query);
    }

    @Test
    public void testEmptyResultsAreReturnedSuccessfully() {
        int requestId = 1;

        // Implicitly ensure that test waits for receptionist registrations to propagate
        TestProbe<Receptionist.Listing> receptionistProbe = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.subscribe(ADBShard.SERVICE_KEY, receptionistProbe.ref()));

        // Ensure shard is present
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShardFactory.createDefault());
        testKit.system().receptionist().tell(Receptionist.register(ADBShard.SERVICE_KEY, shard));


        TestProbe<ADBShardInquirer.Response> resultProbe = testKit.createTestProbe();
        ActorRef<ADBShardInquirer.Command> inquirer = testKit.spawn(ADBShardInquirerFactory.createDefault());

        // necessary to ensure receptionist registration propagates successfully before querying shards
        receptionistProbe.receiveSeveralMessages(2);

        ADBSelectionQuery query = new ADBSelectionQuery();
        inquirer.tell(ADBShardInquirer.QueryShards.builder()
                                                  .requestId(requestId)
                                                  .query(query)
                                                  .respondTo(resultProbe.ref())
                                                  .build());

        ADBShardInquirer.Response results = resultProbe.receiveMessage();
        ADBShardInquirer.AllQueryResults typedResults = (ADBShardInquirer.AllQueryResults) results;

        assertThat(typedResults.getResults().length).isZero();
    }

    @Test
    public void testValidResultsAreReturnedSuccessfully() {
        int requestId = 1;

        TestProbe<ADBShardDistributor.Command> persistProbe = testKit.createTestProbe();
        TestProbe<ADBShardInquirer.Response> resultProbe = testKit.createTestProbe();

        // Implicitly ensure that test waits for receptionist registrations to propagate
        TestProbe<Receptionist.Listing> receptionistProbe = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.subscribe(ADBShard.SERVICE_KEY, receptionistProbe.ref()));

        // Ensure shard is present
        ActorRef<ADBShard.Command> shard = testKit.spawn(ADBShardFactory.createDefault());
        testKit.system().receptionist().tell(Receptionist.register(ADBShard.SERVICE_KEY, shard));
        ADBEntityType testEntity = new TestEntity(1, "Test", 2f, true, 12.02132, 'w');
        shard.tell(new ADBShard.PersistEntity(persistProbe.ref(), testEntity));

        ActorRef<ADBShardInquirer.Command> inquirer = testKit.spawn(ADBShardInquirerFactory.createDefault());

        // necessary to ensure receptionist registration propagates successfully before querying shards
        receptionistProbe.receiveSeveralMessages(2);

        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();
        query.addTerm(term);
        inquirer.tell(ADBShardInquirer.QueryShards.builder()
                                                  .requestId(requestId)
                                                  .query(query)
                                                  .respondTo(resultProbe.ref())
                                                  .build());

        ADBShardInquirer.AllQueryResults results =
                resultProbe.expectMessageClass(ADBShardInquirer.AllQueryResults.class);

        assertThat(results.getResults().length).isOne();
        assertThat(((TestEntity) results.getResults()[0]).getPrimaryKey()).isEqualTo(testEntity.getPrimaryKey());
        assertThat(results.getRequestId()).isEqualTo(requestId);
    }

    @Test
    public void ensureNoArgsConstructorForConcludeTransactionForDeserializationIsPresent() {
        ADBQuerySession.ConcludeTransaction concludeTransaction = new ADBQuerySession.ConcludeTransaction();

        assertThat(concludeTransaction.getTransactionId()).isZero();
    }


}