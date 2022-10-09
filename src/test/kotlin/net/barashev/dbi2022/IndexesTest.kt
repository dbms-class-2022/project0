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

import net.datafaker.Faker
import org.junit.jupiter.api.Test
import java.io.Closeable
import java.util.function.Function
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class FakeIndex<S: AttributeType<T>, T: Comparable<T>>(
    private val accessMethodManager: AccessMethodManager,
    private val pageCache: PageCache,
    private val tableName: String,
    private val indexTableName: String,
    private val keyType: S,
    private val indexKey: Function<ByteArray, T>
): Index<T> {

    private val key2page = mutableMapOf<T, PageId>()

    init {
        try  {
            accessMethodManager.pageCount(tableName)
            openIndex()
        }
        catch (ex: AccessMethodException) {
            buildIndex()
        }
    }

    private fun buildIndex() {
        val indexTableOid = accessMethodManager.createTable(indexTableName)
        val createIndexPage = {
            pageCache.getAndPin(accessMethodManager.addPage(indexTableOid))
        }
        var outPage = createIndexPage()
        accessMethodManager.createFullScan(tableName) {
            indexKey.apply(it)
        }.pages().forEach { page ->
            page.allRecords().forEach { rid, result ->
                if (result.isOk) {
                    val indexKey = indexKey.apply(result.bytes)
                    val indexRecord = Record2(keyType to indexKey, intField(page.id))
                    if (outPage.putRecord(indexRecord.asBytes()).isOutOfSpace) {
                        outPage.close()
                        outPage = createIndexPage()
                        outPage.putRecord(indexRecord.asBytes())
                    }
                    key2page[indexKey] = page.id
                }
            }
        }
        outPage.close()
    }

    private fun openIndex() {
        val parser = { bytes: ByteArray -> Record2(keyType to keyType.defaultValue(), intField()).fromBytes(bytes)}
        accessMethodManager.createFullScan(indexTableName, parser).pages().forEach {
            it.allRecords().forEach { rid, res ->
                if (res.isOk) {
                    parser(res.bytes).let {
                        key2page[it.value1] = it.value2
                    }
                }
            }
        }
    }

    override fun lookup(indexKey: T): PageId? = key2page[indexKey]
}

class FakeIndexFactory(private val accessMethodManager: AccessMethodManager, private val pageCache: PageCache): IndexFactory {
    override fun <T : Comparable<T>, S : AttributeType<T>> build(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> = FakeIndex(accessMethodManager, pageCache, tableName, indexTableName, keyType, indexKey)

    override fun <T : Comparable<T>, S : AttributeType<T>> open(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> = FakeIndex(accessMethodManager, pageCache, tableName, indexTableName, keyType, indexKey)

}

class IndexesTest {
    val faker = Faker()
    @Test
    fun `fake index test`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleAccessMethodManager(cache)
        IndexManager.indexFactory = FakeIndexFactory(accessMethodManager, cache)


        val allNames = mutableListOf<String>()
        val fooOid = accessMethodManager.createTable("foo")
        cache.getAndPin(accessMethodManager.addPage(fooOid)).use {inPage ->
            (1..10).forEach { inPage.putRecord(Record2(intField(it), stringField(faker.name().fullName().also {allNames.add(it)})).asBytes()) }
        }
        val index1 = IndexManager.indexFactory.build("foo", "foo_id_idx", keyType = IntAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value1
        }
        assertNotNull(index1.lookup(10))
        val index1a = IndexManager.indexFactory.open("foo", "foo_id_idx", keyType = IntAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value1
        }
        assertEquals(index1.lookup(10), index1a.lookup(10))


        val index2 = IndexManager.indexFactory.build("foo", "foo_name_idx", keyType = StringAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value2
        }
        val index2a = IndexManager.indexFactory.open("foo", "foo_name_idx", keyType = StringAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value2
        }
        assertNotNull(index2.lookup(allNames[0]))
        assertEquals(index2.lookup(allNames[0]), index2a.lookup(allNames[0]))
    }
}