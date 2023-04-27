package io.hstream.impl

import io.hstream.Record
import io.hstream.buildBlackBoxSinkClient_
import org.junit.jupiter.api.Test

class BufferedProducerKtImplTest {
    @Test
    fun testBufferedProducerCanWork() {
        val streamName = "some-stream"
        val xs = buildBlackBoxSinkClient_()
        val client = xs.first
        val controller = xs.second
        val chan = controller.getStreamNameFlushChannel(streamName)
        val producer = client.newBufferedProducer()
            .stream(streamName)
            .build()
        val writeResult = producer.write(
            Record.newBuilder()
                .rawRecord("some-byte-data".toByteArray())
                .build()
        )
        producer.flush()
        writeResult.get()
        assert(chan.tryReceive().isSuccess)
    }
}
