package net.barashev.dbi2022

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.selects.select
import net.barashev.dbi2022.txn.TransactionManager
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine
import kotlin.test.assertEquals

class TransactionManagerTest {
    @Test
    fun `single transaction with fake scheduler`() {
        val storage = createHardDriveEmulatorStorage()
        val realCache = SimplePageCacheImpl(storage, 20)
        val scheduler = FakeScheduler()
        val txnManager = TransactionManager(scheduler, realCache)

        val countDown = CountDownLatch(2)
        txnManager.txn {cache, txnId ->
            cache.get(10).use {
                it.putRecord(Record1(intField(42)).asBytes())
            }
            txnManager.commit(txnId)
            countDown.countDown()
        }
        txnManager.txn {cache, txnId ->
            cache.get(10).use {
                it.allRecords()
            }
            txnManager.abort(txnId)
            countDown.countDown()
        }

        countDown.await()
    }

    @Test
    fun `wait for blocking txn`() {
        val storage = createHardDriveEmulatorStorage()
        val realCache = SimplePageCacheImpl(storage, 20)
        val scheduler = BlockAllOnWriteScheduler()
        val txnManager = TransactionManager(scheduler, realCache)
        val sync = TxnActionSync()

        val countDown = CountDownLatch(2)
        txnManager.txn { cache, txn ->
            sync.await(0)

            println("T$txn-R1")
            assertEquals(0, cache.get(10).allRecords().size)
            sync.send(1)

            sync.await(2)
            println("T$txn-W2")
            cache.get(11).putRecord(Record1(intField(43)).asBytes())

            assertEquals(2, cache.get(11).allRecords().size)
            txnManager.commit(txn)
            countDown.countDown()
        }
        txnManager.txn { cache, txn ->
            sync.await(1)
            println("T$txn-W1")
            cache.get(10).putRecord(Record1(intField(42)).asBytes())
            sync.send(2)
            // We wait a little bit just to give T1 a chance to do its W2 action. It is not supposed to
            // be allowed because T2-W1 blocked all writes.
            runBlocking { delay(100) }

            println("T$txn-R2")
            assertEquals(0, cache.get(11).allRecords().size)
            println("T$txn-W3")
            cache.get(11).putRecord(Record1(intField(44)).asBytes())
            txnManager.commit(txn)
            // When T2 is committed T1 is supposed to continue execution.
            countDown.countDown()
        }
        sync.send(0)
        countDown.await()
    }

}


internal class BlockAllOnWriteScheduler : Scheduler {
    val blockingTxn = AtomicInteger(-1)
    val waitQueue = mutableListOf<TransactionDescriptor>()
    override fun read(txn: TransactionDescriptor, page: PageId): ReadResult = ReadOk(page)

    override fun write(txn: TransactionDescriptor, page: PageId): WriteResult {
        if (blockingTxn.compareAndSet(-1, txn) || blockingTxn.compareAndSet(txn, txn)) {
            return WriteOk {_,_ -> }
        } else {
            waitQueue.add(txn)
            return WriteWait(blockingTxn.get())
        }
    }

    override fun commit(txn: TransactionDescriptor): List<TransactionDescriptor> {
        if (blockingTxn.compareAndSet(txn, -1)) {
            return waitQueue.toList()
        } else {
            return emptyList()
        }
    }

    override fun abort(txn: TransactionDescriptor): List<TransactionDescriptor> {
        if (blockingTxn.compareAndSet(txn, -1)) {
            return waitQueue.toList()
        } else {
            return emptyList()
        }
    }
}

internal class TxnActionSync {
    private var lastValue: Int = -1
    private val events = Channel<Int>()
    private val continuations = mutableMapOf<Int, Continuation<Unit>?>()
    init {
        GlobalScope.launch {
            for (value in events) {
                println("received value $value")
                lastValue = value
                continuations[value]?.resume(Unit)
                continuations.remove(value)
            }
        }
    }
    fun send(action: Int) {
        runBlocking {
            events.send(action)
        }
    }

    suspend fun await(action: Int) {
        if (action <= lastValue) {
            return
        }
        println("awaiting for value $action")
        suspendCoroutine { continuations[action] = it }
    }
}