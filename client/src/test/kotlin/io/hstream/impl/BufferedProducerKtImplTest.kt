package io.hstream.impl

import io.hstream.buildMockedClient
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class BufferedProducerKtImplTest {
    @Test
    @Disabled
    fun testBufferedProducerCanWork() {
        val client = buildMockedClient()
        client.newBufferedProducer()
//            .batchSetting()
//            .stream()
//            .compressionType()
//            .flowControlSetting()
//            .requestTimeoutMs()
            .build()
    }
}
