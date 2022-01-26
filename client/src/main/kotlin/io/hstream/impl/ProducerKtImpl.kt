package io.hstream.impl

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.hstream.HRecord
import io.hstream.HStreamDBClientException
import io.hstream.Producer
import io.hstream.RecordId
import io.hstream.internal.AppendRequest
import io.hstream.internal.HStreamApiGrpcKt
import io.hstream.internal.HStreamRecord
import io.hstream.internal.LookupStreamRequest
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference

open class ProducerKtImpl(private val stream: String) : Producer {
    private val serverUrlRef: AtomicReference<String> = AtomicReference(null)

    private suspend fun lookupServerUrl(): String {
        return HStreamClientKtImpl.unaryCallCoroutine {
            val serverNode = it.lookupStream(LookupStreamRequest.newBuilder().setStreamName(stream).build()).serverNode
            return@unaryCallCoroutine "${serverNode.host}:${serverNode.port}"
        }
    }

    private suspend fun refreshServerUrl() {
        logger.info("producer will refreshServerUrl, current url is [{}]", serverUrlRef.get())
        serverUrlRef.set(lookupServerUrl())
        logger.info("producer refreshed serverUrl, now url is [{}]", serverUrlRef.get())
    }

    override fun write(rawRecord: ByteArray): CompletableFuture<RecordId> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromRawRecord(rawRecord)
        return writeInternal(hStreamRecord)
    }

    override fun write(hRecord: HRecord): CompletableFuture<RecordId> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromHRecord(hRecord)
        return writeInternal(hStreamRecord)
    }

    protected open fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        val future = CompletableFuture<RecordId>()
        writeRecordScope.launch {
            try {
                val ids = writeHStreamRecords(listOf(hStreamRecord))
                future.complete(ids[0])
            } catch (e: Throwable) {
                future.completeExceptionally(e)
            }
        }
        return future
    }

    private suspend fun appendWithRetry(appendRequest: AppendRequest, tryTimes: Int): List<RecordId> {
        // Note: A failed grpc call can throw both 'StatusException' and 'StatusRuntimeException'.
        //       This function is for handling them.
        suspend fun handleGRPCException(serverUrl: String, e: Throwable): List<RecordId> {
            logger.error("append with serverUrl [{}] error", serverUrl, e)
            val status = Status.fromThrowable(e)
            if (status.code == Status.UNAVAILABLE.code && tryTimes > 1) {
                delay(DefaultSettings.REQUEST_RETRY_INTERVAL_SECONDS * 1000)
                refreshServerUrl()
                return appendWithRetry(appendRequest, tryTimes - 1)
            } else {
                throw HStreamDBClientException(e)
            }
        }

        check(tryTimes > 0)
        var serverUrl = serverUrlRef.get()
        if (serverUrl == null) {
            refreshServerUrl()
            serverUrl = serverUrlRef.get()
        }
        checkNotNull(serverUrl)
        logger.info("try append with serverUrl [{}], current left tryTimes is [{}]", serverUrl, tryTimes)
        try {
            return HStreamApiGrpcKt.HStreamApiCoroutineStub(HStreamClientKtImpl.channelProvider.get(serverUrl))
                .append(appendRequest).recordIdsList.map(GrpcUtils::recordIdFromGrpc)
        } catch (e: StatusException) {
            return handleGRPCException(serverUrl, e)
        } catch (e: StatusRuntimeException) {
            return handleGRPCException(serverUrl, e)
        }
    }

    protected suspend fun writeHStreamRecords(
        hStreamRecords: List<HStreamRecord>?
    ): List<RecordId> {
        val appendRequest = AppendRequest.newBuilder().setStreamName(stream).addAllRecords(hStreamRecords).build()
        return appendWithRetry(appendRequest, DefaultSettings.APPEND_RETRY_MAX_TIMES)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProducerKtImpl::class.java)
        private val writeRecordScope = CoroutineScope(Dispatchers.Default)
    }
}
