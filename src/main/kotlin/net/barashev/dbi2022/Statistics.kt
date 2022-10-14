package net.barashev.dbi2022

import java.util.function.Function

/**
 * A single bucket of an attribute value histogram.
 * A bucket indicates how many values are in a semi-open range [rangeStart, rangeEnd)
 */
data class HistogramBucket<T>(val rangeBegin: T, val rangeEnd: T, val valueCount: Int)

/**
 * A single attribute statistics which includes:
 * - attribute cardinality (total number of distinct attribute values)
 * - an equi-height histogram of the attribute values distribution
 */
data class AttributeStatistics<T: Comparable<T>>(
    val attributeName: String,
    val cardinality: Int,
    val histogramBuckets: List<HistogramBucket<T>>
)

interface StatisticsManager {
    /**
     * Creates statistics for the given attribute of a table, using specified bucketCount number of histogram buckets.
     * Statistic data shall be persistent, and shall be returned from getStatistics calls without re-calculating
     */
    fun <T: Comparable<T>> buildStatistics(
        tableName: String, attributeName: String, attributeType: AttributeType<T>, bucketCount: Int, attributeValue: Function<ByteArray, T>): AttributeStatistics<T>

    /**
     * @return Returns statistics previously build for the given attribute of a table, or null if no statistics records
     * were found for that attribute.
     */
    fun <T: Comparable<T>> getStatistics(tableName: String, attributeName: String, attributeType: AttributeType<T>): AttributeStatistics<T>?
}

object Statistics {
    /**
     * Factory method for creating StatisticsManager instances.
     * Please set your own factory instance from your own code.
     */
    var managerFactory: (AccessMethodManager, PageCache) -> StatisticsManager = { _, _ -> FakeStatisticsManager() }
}


private class FakeStatisticsManager: StatisticsManager {
    override fun <T : Comparable<T>> buildStatistics(
        tableName: String,
        attributeName: String,
        attributeType: AttributeType<T>,
        bucketCount: Int,
        attributeValue: Function<ByteArray, T>
    ): AttributeStatistics<T> = AttributeStatistics(attributeName, 0, emptyList())

    override fun <T : Comparable<T>> getStatistics(
        tableName: String,
        attributeName: String,
        attributeType: AttributeType<T>
    ): AttributeStatistics<T>? = AttributeStatistics(attributeName, 0, emptyList())
}

