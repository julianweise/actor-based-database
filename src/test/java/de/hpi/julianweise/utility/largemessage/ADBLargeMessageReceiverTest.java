package de.hpi.julianweise.utility.largemessage;


import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.NotSerializableException;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBLargeMessageReceiverTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @NoArgsConstructor
    @AllArgsConstructor
    private static class LargeTestMessage implements ADBLargeMessageSender.LargeMessage {
        private byte[] payload;

    }

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
    public void expectReceiverToRequestNextChunkAfterInitialization() {

        TestProbe<ADBLargeMessageActor.Command> originalReceiver = testKit.createTestProbe();
        TestProbe<ADBLargeMessageSender.Command> sender = testKit.createTestProbe();

        ActorRef<ADBLargeMessageReceiver.Command> receiver =
                testKit.spawn(ADBLargeMessageReceiverFactory.createDefault(originalReceiver.ref(),
                        LargeTestMessage.class, sender.ref()));

        receiver.tell(new ADBLargeMessageReceiver.InitializeTransfer(sender.ref(), 500, LargeTestMessage.class));

        ADBLargeMessageSender.SendNextChunk request =
                sender.expectMessageClass(ADBLargeMessageSender.SendNextChunk.class);

        assertThat(request.getRespondTo()).isEqualTo(receiver);
    }

    @Test
    public void expectReceiverToRequestNextChunkAfterIntermediateChunk() throws NotSerializableException {
        Serialization serialization = SerializationExtension.get(testKit.system());

        TestProbe<ADBLargeMessageActor.Command> originalReceiver = testKit.createTestProbe();
        TestProbe<ADBLargeMessageSender.Command> sender = testKit.createTestProbe();

        byte[] randomPayload = new byte[500];
        new Random().nextBytes(randomPayload);
        LargeTestMessage typedPayload = new LargeTestMessage(randomPayload);
        Serializer serializer = serialization.serializerFor(typedPayload.getClass());
        byte[] payload = serializer.toBinary(typedPayload);

        ActorRef<ADBLargeMessageReceiver.Command> receiver =
                testKit.spawn(ADBLargeMessageReceiverFactory.createDefault(originalReceiver.ref(),
                        LargeTestMessage.class, sender.ref()));

        receiver.tell(new ADBLargeMessageReceiver.InitializeTransfer(sender.ref(), payload.length,
                LargeTestMessage.class));

        sender.expectMessageClass(ADBLargeMessageSender.SendNextChunk.class);

        receiver.tell(new ADBLargeMessageReceiver.ReceiveChunk(payload, false));

        ADBLargeMessageSender.SendNextChunk request =
                sender.expectMessageClass(ADBLargeMessageSender.SendNextChunk.class);

        assertThat(request.getRespondTo()).isEqualTo(receiver);

    }

    @Test
    public void expectReceiverToConcludeTransferAfterLastChunk() throws NotSerializableException {
        Serialization serialization = SerializationExtension.get(testKit.system());

        TestProbe<ADBLargeMessageReceiver.Command> receiverProbe = testKit.createTestProbe();
        TestProbe<ADBLargeMessageActor.Command> originalReceiver = testKit.createTestProbe();
        TestProbe<ADBLargeMessageSender.Command> sender = testKit.createTestProbe();

        byte[] randomPayload = new byte[500];
        new Random().nextBytes(randomPayload);
        LargeTestMessage typedPayload = new LargeTestMessage(randomPayload);
        Serializer serializer = serialization.serializerFor(typedPayload.getClass());
        byte[] payload = serializer.toBinary(typedPayload);

        ActorRef<ADBLargeMessageReceiver.Command> receiver =
                testKit.spawn(ADBLargeMessageReceiverFactory.createDefault(originalReceiver.ref(),
                        LargeTestMessage.class, sender.ref()));

        receiver.tell(new ADBLargeMessageReceiver.InitializeTransfer(sender.ref(), payload.length,
                LargeTestMessage.class));

        sender.expectMessageClass(ADBLargeMessageSender.SendNextChunk.class);

        receiver.tell(new ADBLargeMessageReceiver.ReceiveChunk(payload, true));

        LargeTestMessage result = originalReceiver.expectMessageClass(LargeTestMessage.class);

        assertThat(result.payload).isEqualTo(typedPayload.payload);
        receiverProbe.expectTerminated(receiver);
    }

    @Test
    public void expectReceiverToHandleMultipleChunks() throws NotSerializableException {
        Serialization serialization = SerializationExtension.get(testKit.system());

        TestProbe<ADBLargeMessageReceiver.Command> receiverProbe = testKit.createTestProbe();
        TestProbe<ADBLargeMessageActor.Command> originalReceiver = testKit.createTestProbe();
        TestProbe<ADBLargeMessageSender.Command> sender = testKit.createTestProbe();

        byte[] randomPayload = new byte[500];
        new Random().nextBytes(randomPayload);
        LargeTestMessage typedPayload = new LargeTestMessage(randomPayload);
        Serializer serializer = serialization.serializerFor(typedPayload.getClass());
        byte[] payload = serializer.toBinary(typedPayload);

        ActorRef<ADBLargeMessageReceiver.Command> receiver =
                testKit.spawn(ADBLargeMessageReceiverFactory.createDefault(originalReceiver.ref(),
                        LargeTestMessage.class, sender.ref()));

        receiver.tell(new ADBLargeMessageReceiver.InitializeTransfer(sender.ref(), payload.length,
                LargeTestMessage.class));

        sender.expectMessageClass(ADBLargeMessageSender.SendNextChunk.class);

        receiver.tell(new ADBLargeMessageReceiver.ReceiveChunk(Arrays.copyOfRange(payload, 0, 250), false));

        sender.expectMessageClass(ADBLargeMessageSender.SendNextChunk.class);

        receiver.tell(new ADBLargeMessageReceiver.ReceiveChunk(Arrays.copyOfRange(payload, 250, payload.length), true));

        LargeTestMessage result = originalReceiver.expectMessageClass(LargeTestMessage.class);

        assertThat(result.payload).isEqualTo(typedPayload.payload);
        receiverProbe.expectTerminated(receiver);
    }

    @Test
    public void expectReceiverIgnoresSecondInitializeTransfer() throws NotSerializableException {
        Serialization serialization = SerializationExtension.get(testKit.system());

        TestProbe<ADBLargeMessageReceiver.Command> receiverProbe = testKit.createTestProbe();
        TestProbe<ADBLargeMessageActor.Command> originalReceiver = testKit.createTestProbe();
        TestProbe<ADBLargeMessageSender.Command> sender = testKit.createTestProbe();

        byte[] randomPayload = new byte[500];
        new Random().nextBytes(randomPayload);
        LargeTestMessage typedPayload = new LargeTestMessage(randomPayload);
        Serializer serializer = serialization.serializerFor(typedPayload.getClass());
        byte[] payload = serializer.toBinary(typedPayload);

        ActorRef<ADBLargeMessageReceiver.Command> receiver =
                testKit.spawn(ADBLargeMessageReceiverFactory.createDefault(originalReceiver.ref(),
                        LargeTestMessage.class, sender.ref()));

        receiver.tell(new ADBLargeMessageReceiver.InitializeTransfer(sender.ref(), payload.length,
                LargeTestMessage.class));

        sender.expectMessageClass(ADBLargeMessageSender.SendNextChunk.class);

        receiver.tell(new ADBLargeMessageReceiver.ReceiveChunk(Arrays.copyOfRange(payload, 0, 250), false));

        sender.expectMessageClass(ADBLargeMessageSender.SendNextChunk.class);

        receiver.tell(new ADBLargeMessageReceiver.InitializeTransfer(sender.ref(), payload.length - 200,
                LargeTestMessage.class));

        sender.expectNoMessage();

        receiver.tell(new ADBLargeMessageReceiver.ReceiveChunk(Arrays.copyOfRange(payload, 250, payload.length), true));

        LargeTestMessage result = originalReceiver.expectMessageClass(LargeTestMessage.class);

        assertThat(result.payload).isEqualTo(typedPayload.payload);
        receiverProbe.expectTerminated(receiver);
    }

}