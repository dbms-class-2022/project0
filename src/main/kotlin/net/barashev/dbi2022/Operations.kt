/*
 * Copyright 2022 Dmitry Barashev, JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.barashev.dbi2022

import java.util.function.Function

/**
 * This operation sorts an table using order defined by Comparable objects parsed from the table records.
 * Sorting returns a copy of the input table contents as a new [temporary] table.
 *
 *
 */
interface MultiwayMergeSort : AutoCloseable {
    /**
     * Sorts a table with the name tableName by values extracted from the table records with comparableValue function.
     *
     * @returns the name of the output table. The output table is owned by the client, and it is client's responsibility
     *          to delete the output table when it is not needed anymore.
     */
    fun <T: Comparable<T>> sort(tableName: String, comparableValue: Function<ByteArray, T>): String

    /**
     * Releases all the temporary resources, such as intermediate tables, except for the output table.
     */
    override fun close() {}
}

/**
 * Hash table bucket with 0-based bucket number, name of a temporary table where bucket records are stored and
 * the total count of pages in the bucket.
 */
data class Bucket(val num: Int, val tableName: String, val pageCount: Int)

interface Hashtable : AutoCloseable {
    /**
     * Hashes the input table records using bucketCount buckets and keys returned by hashKey function.
     * @return a list of created buckets metadata, in the ascending order of bucket numbers.
     */
    fun <T> hash(tableName: String, bucketCount: Int, hashKey: Function<ByteArray, T>): List<Bucket>

    /**
     * Releases all the temporary resources, such as intermediate tables.
     */
    override fun close() {}
}

enum class JoinAlgorithm {
    NESTED_LOOPS, HASH, MERGE
}

/**
 * Join operation operand. The algorithm will read records from the specified tables and will parse them using
 * joinAttribute function which is supposed to return a value of a join attribute for each record.
 */
data class JoinOperand<T>(val tableName: String, val joinAttribute: Function<ByteArray, T>)

interface JoinOutput: Iterator<Pair<ByteArray, ByteArray>>, AutoCloseable
/**
 * Objects implementing this interface can join tables.
 * This interface it closeable, and it is the responsibility of this object client to call close() on it or
 * on the returned iterator when the work is done.
 */
interface InnerJoin : AutoCloseable {

    /**
     * Performs an inner join of the given tables and writes the matching records to the output consumer.
     * In pairs passed to the output consumer
     * @return iterator over the join output. The first component of returned pairs comes from the "left" operand and second component
     * comes from the "right" operand. Calling close() on the iterator closes the join operation, and vice versa
     */
    fun <T: Comparable<T>> join(leftTable: JoinOperand<T>, rightTable: JoinOperand<T>): JoinOutput

    override fun close() {}
}

object Operations {
    /**
     * Creates an instance of MultiwayMergeSort using the passed access method manager and page cache.
     */
    var sortFactory: (AccessMethodManager, PageCache) -> MultiwayMergeSort = { _, _  ->
        object : MultiwayMergeSort {
            override fun <T: Comparable<T>> sort(tableName: String, comparableValue: Function<ByteArray, T>): String {
                TODO("Not yet implemented")
            }
        }
    }

    /**
     * Creates an instance of a hashtable using the passed access method manager and page cache.
     */
    var hashFactory: (AccessMethodManager, PageCache) -> Hashtable = { _, _ ->
        object : Hashtable {
            override fun <T> hash(tableName: String, bucketCount: Int, hashKey: Function<ByteArray, T>): List<Bucket> {
                TODO("Not yet implemented")
            }
        }
    }

    /**
     * Creates an instance of an inner join
     */
    var innerJoinFactory: (AccessMethodManager, PageCache, JoinAlgorithm) -> InnerJoin = { _, _, _ ->
        object : InnerJoin {
            override fun <T : Comparable<T>> join(leftTable: JoinOperand<T>, rightTable: JoinOperand<T>): JoinOutput {
                TODO("Not yet implemented")
            }
        }
    }
}

