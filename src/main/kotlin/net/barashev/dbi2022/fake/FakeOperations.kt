package net.barashev.dbi2022.fake

import net.barashev.dbi2022.*
import java.util.function.Function

class FakeMergeSort(private val accessMethodManager: AccessMethodManager, private val cache: PageCache):
    MultiwayMergeSort {
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

class FakeNestedLoops(private val accessMethodManager: AccessMethodManager, private val pageCache: PageCache) :
    InnerJoin {
    override fun <T : Comparable<T>> join(leftTable: JoinOperand<T>, rightTable: JoinOperand<T>): JoinOutput {
        val output = mutableListOf<Pair<ByteArray, ByteArray>>()
        accessMethodManager.createFullScan(leftTable.tableName) { leftBytes ->
            leftTable.joinAttribute.apply(leftBytes) to leftBytes
        }.forEach {leftTuple ->
            accessMethodManager.createFullScan(rightTable.tableName) { rightBytes ->
                rightTable.joinAttribute.apply(rightBytes) to rightBytes
            }.forEach { rightTuple ->
                if (leftTuple.first == rightTuple.first) {
                    output.add(leftTuple.second to rightTuple.second)
                }
            }
        }
        return object : JoinOutput {
            private val iterator = output.iterator()
            override fun hasNext(): Boolean = iterator.hasNext()

            override fun next(): Pair<ByteArray, ByteArray> = iterator.next()

            override fun close() {
            }

        }

    }

}