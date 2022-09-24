package net.barashev.dbi2022

import org.junit.jupiter.api.Test
import java.util.function.Function
import kotlin.test.assertEquals

class FakeMergeSort(private val accessMethodManager: AccessMethodManager, private val cache: PageCache): MultiwayMergeSort {
    override fun <T : Comparable<T>> sort(tableName: String, comparableValue: Function<ByteArray, T>): String {
        val raw = accessMethodManager.createFullScan(tableName) {
            it
        }.toList()
        val unsorted = raw.map { comparableValue.apply(it) }
        val sortedValues = unsorted.mapIndexed { index, t -> t to index }.sortedBy { it.first }
        val outOid = accessMethodManager.createTable("output")
        cache.getAndPin(accessMethodManager.addPage(outOid)).use {outPage ->
            sortedValues.forEach { pair ->
                outPage.putRecord(raw[pair.second])
            }
        }
        return "output"
    }

}

class OperationsTest  {
    @Test
    fun `fake sort`() {
        Operations.sortFactory = { accessMethodManager, pageCache -> FakeMergeSort(accessMethodManager, pageCache) }
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleAccessMethodManager(cache)
        val fooOid = accessMethodManager.createTable("foo")
        cache.getAndPin(accessMethodManager.addPage(fooOid)).use {inPage ->
            (1..10).forEach { inPage.putRecord(intField().first.asBytes(it)) }
        }
        val outTable = Operations.sortFactory(accessMethodManager, cache).sort("foo") {
            intField().first.fromBytes(it).first
        }
        assertEquals((1..10).toList(), accessMethodManager.createFullScan(outTable) {
            intField().first.fromBytes(it).first
        }.toList())
    }

}