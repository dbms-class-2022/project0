package net.barashev.dbi2022.txn

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import net.barashev.dbi2022.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class TransactionManager(private val scheduler: Scheduler, val cache: PageCache): Scheduler by scheduler {
    private val txnWorkerScope = CoroutineScope(Executors.newCachedThreadPool().asCoroutineDispatcher())
    private val lastTxnId = AtomicInteger(0)
    private val txnCompletionScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val txnCompletionChannel = Channel<TransactionDescriptor>()

    override fun commit(txn: TransactionDescriptor): List<TransactionDescriptor> =
        scheduler.commit(txn).also {
            txnCompletionScope.launch { txnCompletionChannel.send(txn) }
        }

    override fun abort(txn: TransactionDescriptor): List<TransactionDescriptor> =
        scheduler.abort(txn).also {
            txnCompletionScope.launch { txnCompletionChannel.send(txn) }
        }

    fun txn(txnCode: suspend (PageCache, TransactionDescriptor) -> Unit) {
        val txnId = lastTxnId.incrementAndGet()
        val txnPageCache = TxnPageCacheImpl(cache, txnId, this, txnCompletionChannel)
        txnWorkerScope.launch {
            txnCode(txnPageCache, txnId)
        }
    }

}

internal class TxnPageCacheImpl(
    private val proxiedCache: PageCache,
    private val txn: TransactionDescriptor,
    private val scheduler: Scheduler,
    private val txnCompletionChannel: Channel<TransactionDescriptor>
) : PageCache by proxiedCache {
    override fun get(pageId: PageId): CachedPage = tryRead(pageId) { proxiedCache.get(pageId) }

    override fun getAndPin(pageId: PageId): CachedPage = tryRead(pageId) { proxiedCache.getAndPin(pageId) }

    private fun tryRead(pageId: PageId, code: () -> CachedPage): CachedPage {
        while (true) {
            when (val read = scheduler.read(txn, pageId)) {
                is ReadOk -> {
                    return TxnPage(code(), this)
                }

                is ReadAbort -> {
                    scheduler.abort(txn)
                    throw TransactionException("Transaction $txn can't read page $pageId and has been aborted")
                }

                is ReadWait -> {
                    runBlocking {
                        while (true) {
                            val completedTxn = txnCompletionChannel.receive()
                            if (completedTxn == read.blockingTxn) {
                                break
                            }
                        }
                    }
                }
            }
        }
    }

    internal fun <T> tryWrite(pageId: PageId, code: () -> T): T {
        while (true) {
            when (val write = scheduler.write(txn, pageId)) {
                is WriteOk -> {
                    return code()
                }

                is WriteAbort -> {
                    scheduler.abort(txn)
                    throw TransactionException("Transaction $txn can't write page $pageId and has been aborted")
                }

                is WriteWait -> {
                    runBlocking {
                        while (true) {
                            val completedTxn = txnCompletionChannel.receive()
                            if (completedTxn == write.blockingTxn) {
                                break
                            }
                        }
                    }
                }
            }
        }
    }

    override fun createSubCache(size: Int): PageCache =
        TxnPageCacheImpl(proxiedCache.createSubCache(size), txn, scheduler, txnCompletionChannel)
}

internal class TxnPage(private val proxiedPage: CachedPage, private val txnCache: TxnPageCacheImpl): CachedPage by proxiedPage {
    override fun putRecord(recordData: ByteArray, recordId: RecordId): PutRecordResult =
        txnCache.tryWrite(proxiedPage.id) {
            proxiedPage.putRecord(recordData, recordId)
        }


    override fun deleteRecord(recordId: RecordId) =
        txnCache.tryWrite(proxiedPage.id) {
            proxiedPage.deleteRecord(recordId)
        }


    override fun clear() =
        txnCache.tryWrite(proxiedPage.id) {
            proxiedPage.clear()
        }
}