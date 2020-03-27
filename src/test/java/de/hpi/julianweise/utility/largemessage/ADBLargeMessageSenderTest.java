package de.hpi.julianweise.utility.largemessage;


import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.Adapter;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import lombok.AllArgsConstructor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.NotSerializableException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBLargeMessageSenderTest {

    private int KRYO_SIZE_OVERHEAD = 89;

    @AllArgsConstructor
    private static class LargeTestMessage implements ADBLargeMessageSender.LargeMessage {
        byte[] payload;
    }

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
    public void expectToInitializeTransferOnStart() {

        TestProbe<ADBLargeMessageSender.Response> sender = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.Command> receiver = testKit.createTestProbe();

        byte[] randomPayload = new byte[0];
        new Random().nextBytes(randomPayload);

        LargeTestMessage payload = new LargeTestMessage(randomPayload);

        ActorRef<ADBLargeMessageSender.Command> largeMessageSender = testKit.spawn(ADBLargeMessageSenderFactory
                .createDefault(payload, sender.ref()));

        ADBLargeMessageSender.StartTransfer startMessage =
                new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(receiver.ref()), LargeTestMessage.class);

        largeMessageSender.tell(startMessage);

        ADBLargeMessageReceiver.InitializeTransfer initializationMessage =
                receiver.expectMessageClass(ADBLargeMessageReceiver.InitializeTransfer.class);

        assertThat(initializationMessage.getType()).isEqualTo(LargeTestMessage.class);
        assertThat(initializationMessage.getTotalSize()).isEqualTo(KRYO_SIZE_OVERHEAD + payload.payload.length);
        assertThat(initializationMessage.getOriginalSender()).isEqualTo(largeMessageSender.path().parent().name());
        assertThat(initializationMessage.getRespondTo()).isEqualTo(largeMessageSender);
    }

    @Test
    public void expectNextChunkAfterRequesting() throws NotSerializableException {
        TestProbe<ADBLargeMessageSender.Response> sender = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.Command> receiver = testKit.createTestProbe();
        TestProbe<ADBLargeMessageSender.Command> largeMessageSenderProbe = testKit.createTestProbe();

        byte[] randomPayload = new byte[65536];
        new Random().nextBytes(randomPayload);

        LargeTestMessage payload = new LargeTestMessage(randomPayload);

        Serialization serialization = SerializationExtension.get(testKit.system());
        Serializer serializer = serialization.serializerFor(randomPayload.getClass());
        System.out.println(serializer.toBinary(payload.payload).length);


        ActorRef<ADBLargeMessageSender.Command> largeMessageSender = testKit.spawn(ADBLargeMessageSenderFactory
                .createDefault(payload, sender.ref()));

        ADBLargeMessageSender.StartTransfer startMessage =
                new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(receiver.ref()), LargeTestMessage.class);

        largeMessageSender.tell(startMessage);

        largeMessageSender.tell(new ADBLargeMessageSender.SendNextChunk(receiver.ref()));

        receiver.expectMessageClass(ADBLargeMessageReceiver.InitializeTransfer.class);

        ADBLargeMessageReceiver.ReceiveChunk chunk =
                receiver.expectMessageClass(ADBLargeMessageReceiver.ReceiveChunk.class);

        assertThat(chunk.getChunk().length).isEqualTo(ADBLargeMessageSender.getChunkSize(testKit.system().settings()));
        assertThat(chunk.isLastChunk()).isFalse();

        largeMessageSender.tell(new ADBLargeMessageSender.SendNextChunk(receiver.ref()));

        chunk = receiver.expectMessageClass(ADBLargeMessageReceiver.ReceiveChunk.class);

        assertThat(chunk.getChunk().length).isEqualTo(ADBLargeMessageSender.getChunkSize(testKit.system().settings()));
        assertThat(chunk.isLastChunk()).isFalse();

        largeMessageSender.tell(new ADBLargeMessageSender.SendNextChunk(receiver.ref()));

        chunk = receiver.expectMessageClass(ADBLargeMessageReceiver.ReceiveChunk.class);

        assertThat(chunk.getChunk().length)
                .isLessThan(3 * ADBLargeMessageSender.getChunkSize(testKit.system().settings()));
        assertThat(chunk.isLastChunk()).isTrue();

        largeMessageSenderProbe.expectTerminated(largeMessageSender);
    }

    @Test
    public void expectNextFinalChunkAfterRequesting() {
        TestProbe<ADBLargeMessageSender.Command> largeMessageSenderProbe = testKit.createTestProbe();
        TestProbe<ADBLargeMessageSender.Response> sender = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.Command> receiver = testKit.createTestProbe();

        byte[] randomPayload = new byte[32];
        new Random().nextBytes(randomPayload);

        LargeTestMessage payload = new LargeTestMessage(randomPayload);

        ActorRef<ADBLargeMessageSender.Command> largeMessageSender = testKit.spawn(ADBLargeMessageSenderFactory
                .createDefault(payload, sender.ref()));

        ADBLargeMessageSender.StartTransfer startMessage =
                new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(receiver.ref()), LargeTestMessage.class);

        largeMessageSender.tell(startMessage);

        largeMessageSender.tell(new ADBLargeMessageSender.SendNextChunk(receiver.ref()));

        receiver.expectMessageClass(ADBLargeMessageReceiver.InitializeTransfer.class);

        ADBLargeMessageReceiver.ReceiveChunk chunk =
                receiver.expectMessageClass(ADBLargeMessageReceiver.ReceiveChunk.class);

        assertThat(chunk.getChunk().length).isEqualTo(KRYO_SIZE_OVERHEAD + randomPayload.length);
        assertThat(chunk.isLastChunk()).isTrue();

        largeMessageSenderProbe.expectTerminated(largeMessageSender);
    }
}