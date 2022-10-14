package net.barashev.dbi2022

import java.util.function.Function

data class HistogramBucket<T>(val rangeStart: T, val rangeEnd: T, val valueCount: Int)
data class AttributeStatistics<T: Comparable<T>>(
    val attributeName: String,
    val cardinality: Int,
    val histogramBuckets: List<HistogramBucket<T>>
)

interface StatisticsManager {
    fun <T: Comparable<T>> buildStatistics(
        tableName: String, attributeName: String, attributeType: AttributeType<T>, attributeValue: Function<ByteArray, T>): AttributeStatistics<T>

    fun <T: Comparable<T>> getStatistics(tableName: String, attributeName: String, attributeType: AttributeType<T>): AttributeStatistics<T>?
}

object Statistics {
    var managerFactory: (AccessMethodManager, PageCache) -> StatisticsManager = { _, _ -> FakeStatisticsManager() }
}


private class FakeStatisticsManager: StatisticsManager {
    override fun <T : Comparable<T>> buildStatistics(
        tableName: String,
        attributeName: String,
        attributeType: AttributeType<T>,
        attributeValue: Function<ByteArray, T>
    ): AttributeStatistics<T> = AttributeStatistics(attributeName, 0, emptyList())

    override fun <T : Comparable<T>> getStatistics(
        tableName: String,
        attributeName: String,
        attributeType: AttributeType<T>
    ): AttributeStatistics<T>? = AttributeStatistics(attributeName, 0, emptyList())
}

