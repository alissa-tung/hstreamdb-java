package io.hstream

import io.grpc.stub.StreamObserver
import io.hstream.internal.AppendRequest
import io.hstream.internal.AppendResponse
import io.hstream.internal.HStreamApiGrpc
import io.hstream.internal.LookupShardRequest
import io.hstream.internal.LookupShardResponse
import io.hstream.internal.RecordId
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlin.random.Random

class BlackBoxSinkHServerMock(
    hMetaMockCluster: HMetaMock,
    private val serverName: String
) : HServerMock(
    hMetaMockCluster,
    serverName
) {
    val streamNameFlushChannelMap: MutableMap<String, Channel<List<RecordId>>> = mutableMapOf()
    override fun append(request: AppendRequest?, responseObserver: StreamObserver<AppendResponse>?) {
        val streamName = request!!.streamName
        val channel = this.streamNameFlushChannelMap.getOrPut(streamName) {
            Channel(1000)
        }

        val recordIds = (1..request.records.batchSize).map {
            RecordId.newBuilder()
                .setShardId(Random.nextLong())
                .setBatchId(Random.nextLong())
                .setBatchIndex(it).build()
        }

        val sendResult = channel.trySendBlocking(recordIds)
        if (sendResult.isClosed || sendResult.isFailure) {
            io.hstream.impl.logger.error(sendResult.toString())
            responseObserver!!.onError(
                io.grpc.Status.INTERNAL.asException()
            )
        }

        val shardId = request.shardId
        responseObserver!!.onNext(
            AppendResponse.newBuilder()
                .setStreamName(streamName)
                .setShardId(shardId)
                .addAllRecordIds(recordIds)
                .build()
        )
        responseObserver.onCompleted()
    }

    override fun lookupShard(request: LookupShardRequest?, responseObserver: StreamObserver<LookupShardResponse>?) {
        val shardId = request!!.shardId

        responseObserver!!.onNext(
            LookupShardResponse.newBuilder()
                .setShardId(shardId)
                .setServerNode(serverNameToServerNode(serverName))
                .build()
        )

        responseObserver.onCompleted()
    }
}

class BlackBoxSinkHServerMockController(
    private val streamNameFlushChannelMap: MutableMap<String, Channel<List<RecordId>>>
) {
    fun getStreamNameFlushChannel(streamName: String): Channel<List<RecordId>> {
        return this.streamNameFlushChannelMap.getOrPut(streamName) {
            Channel(1000)
        }
    }
}

fun buildBlackBoxSinkClient_(): Pair<HStreamClient, BlackBoxSinkHServerMockController> {
    val xs = buildMockedClient_(
        BlackBoxSinkHServerMock::class.java as Class<HStreamApiGrpc.HStreamApiImplBase>
    )
    val client = xs.first
    val server = xs.second
    val controller = BlackBoxSinkHServerMockController((server as BlackBoxSinkHServerMock).streamNameFlushChannelMap)
    return Pair(client, controller)
}

fun buildBlackBoxSinkClient(): HStreamClient {
    return buildBlackBoxSourceClient_().first
}
