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

import java.lang.IllegalArgumentException
import java.util.function.Function
import kotlin.jvm.Throws

internal const val MAX_ROOT_PAGE_COUNT = 4096
typealias Oid = Int
typealias OidPageidRecord = Record2<Oid, PageId>
typealias OidNameRecord = Record3<Oid, String, Boolean>
typealias TableAttributeRecord = Record3<Oid, String, Int>


interface ColumnConstraint {}

class AccessMethodException: Exception {
    constructor(message: String): super(message)
    constructor(message: String, cause: Throwable): super(message, cause)
}

/**
 * Full scan object can iterate over table pages and records.
 * As Iterable, it creates an iterator over table records.
 * One can iterate over pages using pages() function.
 */
interface FullScan<T> : Iterable<T> {
    fun pages(): Iterable<CachedPage>
}

interface IndexScan<T, K: Comparable<K>> {
    fun byEquality(key: K, keyParser: Function<ByteArray, K>): Iterable<T>
}

/**
 * This interface provides an abstraction over basic physical operations with tables.
 * This is pretty low-level abstraction, e.g. it leaves the process of (de)serialization records from and to bytes
 * to the client. However, it hides the details of storing table metadata and implementations of table page iterators.
 */
interface AccessMethodManager {
    /**
     * Creates an iterator over the records of the given table, if such table exists.
     *
     * @throws AccessMethodException if the requested table can't be found in the catalog.
     */
    @Throws(AccessMethodException::class)
    fun <T> createFullScan(tableName: String, recordBytesParser: Function<ByteArray, T>): FullScan<T>

    /**
     * Creates an empty table with the given name and writes appropriate records into the catalog.
     *
     * @throws IllegalArgumentException if a table with the given name already exists.
     */
    @Throws(IllegalArgumentException::class)
    fun createTable(tableName: String, vararg columns: Triple<String, AttributeType<Any>, ColumnConstraint?>): Oid

    /**
     * Adds new pages to the given table. If more than 1 page is requested, their ids are sequential.
     *
     * @return id of the first added page
     * @throws AccessMethodException if a page can't be added (e.g. because of catalog page overflow)
     */
    @Throws(AccessMethodException::class)
    fun addPage(tableOid: Oid, pageCount: Int = 1): PageId

    /**
     * @return the number of pages in the given table, if it exists
     * @throws AccessMethodException if table with the given name does not exist
     */
    fun pageCount(tableName: String): Int

    /**
     * @return true if a table with the given name exists, false otherwise
     */
    fun tableExists(tableName: String): Boolean

    /**
     * Deletes table with the given name.
     */
    fun deleteTable(tableName: String)
    fun <T, K : Comparable<K>, S : AttributeType<K>> createIndexScan(
        tableName: String,
        attributeName: String,
        attributeType: S,
        keyParser: Function<ByteArray, K>,
        recordBytesParser: Function<ByteArray, T>
    ): IndexScan<T, K>?

    fun <K : Comparable<K>, S : AttributeType<K>> createIndex(
        tableName: String,
        attributeName: String,
        attributeType: S,
        keyParser: Function<ByteArray, K>
    )

    fun indexExists(tableName: String, attributeName: String): Boolean
}

