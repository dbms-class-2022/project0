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

import java.lang.Comparable
import java.util.function.Function

/**
 * This operation sorts an table using order defined by Comparable objects parsed from the table records.
 * Sorting returns a copy of the input table contents as a new [temporary] table.
 *
 *
 */
interface MultiwayMergeSort {
    /**
     * Sorts a table with the name tableName by values extracted from the table records with comparableValue function.
     *
     * @returns the name of the output table. The output table is owned by the client, and it is client's responsibility
     *          to delete the output table when it is not needed anymore.
     */
    fun <T> sort(tableName: String, comparableValue: Function<ByteArray, Comparable<in T>>): String
}

/**
 * Hash table bucket with 0-based bucket number, name of a temporary table where bucket records are stored and
 * the total count of pages in the bucket.
 */
data class Bucket(val num: Int, val tableName: String, val pageCount: Int)

interface Hashtable {
    /**
     * Hashes the input table records using bucketCount buckets and keys returned by hashKey function.
     * @return a list of created buckets metadata, in the ascending order of bucket numbers.
     */
    fun <T> hash(tableName: String, bucketCount: Int, hashKey: Function<ByteArray, T>): List<Bucket>
}


object Operations {
    var sortFactory: (AccessMethodManager, PageCache) -> MultiwayMergeSort = { _, _  ->
        object : MultiwayMergeSort {
            override fun <T> sort(tableName: String, comparableValue: Function<ByteArray, Comparable<in T>>): String {
                TODO("Not yet implemented")
            }
        }
    }

    var hashFactory: (AccessMethodManager, PageCache) -> Hashtable = { _, _ ->
        object : Hashtable {
            override fun <T> hash(tableName: String, bucketCount: Int, hashKey: Function<ByteArray, T>): List<Bucket> {
                TODO("Not yet implemented")
            }
        }
    }
}

