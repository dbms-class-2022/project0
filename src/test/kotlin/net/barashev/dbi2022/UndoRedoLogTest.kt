package net.barashev.dbi2022

import net.barashev.dbi2022.txn.LogManager
import net.barashev.dbi2022.txn.TransactionManager
import net.barashev.dbi2022.txn.recoveryFactory
import net.barashev.dbi2022.txn.walFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
//import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UndoRedoLogTest {
    fun `putRecord redo`() {
        val masterStorage = createHardDriveEmulatorStorage()
        val scheduler = FakeScheduler()
        val walStorage = createHardDriveEmulatorStorage()
        val replicaStorage = createHardDriveEmulatorStorage()
        val wal = walFactory(walStorage)

        val txnManager = TransactionManager(scheduler, LogManager(masterStorage, 20, wal))
        val countDown = CountDownLatch(2)
        txnManager.txn {cache, txnId ->
            cache.getAndPin(10).use {
                it.putRecord(Record1(intField(42)).asBytes())
            }
            assertEquals(0, masterStorage.read(10).allRecords().size)
            txnManager.commit(txnId)
            countDown.countDown()
        }
        txnManager.txn {cache, txnId ->
            cache.getAndPin(9).use {
                it.putRecord(Record1(intField(146)).asBytes())
            }
            assertEquals(0, masterStorage.read(10).allRecords().size)
            txnManager.abort(txnId)
            countDown.countDown()
        }

        countDown.await()
        txnManager.logManager.pageCache.flush()
        masterStorage.read(10).let {committedPage ->
            assertEquals(1, committedPage.allRecords().size) {
                "log:\n${wal.toString()}"
            }
            assertEquals(42, Record1(intField()).fromBytes(committedPage.getRecord(0).bytes).first)
        }

        println(wal)
        // -- The test is supposed to pass until this point with the "default" WAL and Recovery implementations

        val recovery = recoveryFactory()
        recovery.run(walStorage, replicaStorage)

        val replicatedPage = replicaStorage.read(10)
        val getRecordResult = replicatedPage.getRecord(0)
        assertTrue(getRecordResult.isOk)
        assertEquals(42, Record1(intField()).fromBytes(getRecordResult.bytes).first)
    }

    fun `putRecord undo`() {
        val tableStorage = createHardDriveEmulatorStorage()
        val scheduler = FakeScheduler()
        val walStorage = createHardDriveEmulatorStorage()

        val txnManager = TransactionManager(scheduler, LogManager(tableStorage, 20, walFactory(walStorage), useRevertableStorage = false))
        val countDown = CountDownLatch(2)
        txnManager.txn {cache, txnId ->
            cache.get(10).use {
                it.putRecord(Record1(intField(42)).asBytes())
            }
            cache.flush()

            assertEquals(1, tableStorage.read(10).allRecords().size)
            txnManager.abort(txnId)
            countDown.countDown()
        }

        txnManager.txn {cache, txnId ->
            cache.get(9).use {
                it.putRecord(Record1(intField(146)).asBytes())
            }
            txnManager.commit(txnId)
            assertEquals(1, tableStorage.read(9).allRecords().size)
            countDown.countDown()
        }

        countDown.await()

        // -- The test is supposed to pass until this point with the "default" WAL and Recovery implementations

        val recovery = recoveryFactory()
        recovery.run(walStorage, tableStorage)

        assertEquals(0, tableStorage.read(10).allRecords().size)
        tableStorage.read(9).let {committedPage ->
            assertEquals(1, committedPage.allRecords().size)
            assertEquals(146, Record1(intField()).fromBytes(committedPage.getRecord(0).bytes).first)
        }
    }


}
