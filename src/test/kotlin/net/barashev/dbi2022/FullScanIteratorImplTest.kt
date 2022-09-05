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

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FullScanIteratorImplTest {
    private fun createRootRecords(cache: PageCache) {
        cache.getAndPin(0).use {buf ->
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1000)).asBytes())
        }

    }

    @Test
    fun `test table is missing`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val rootRecords = RootRecords(cache, 0, 1)
        assertTrue(FullScanAccessImpl(cache, NAME_SYSTABLE_OID, rootRecords.iterator()) {
            error("Not expected to be here")
        }.toList().isEmpty())
    }

    @Test
    fun `test table is empty`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        createRootRecords(cache)
        val rootRecords = RootRecords(cache, 0, 1)
        assertTrue(FullScanAccessImpl(cache, NAME_SYSTABLE_OID, rootRecords.iterator()) {
            error("Not expected to be here")
        }.toList().isEmpty())
    }

    @Test
    fun `test single page table`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        createRootRecords(cache)
        val rootRecords = RootRecords(cache, 0, 1)
        cache.getAndPin(1000).let {
            it.putRecord(OidNameRecord(intField(2), stringField("table2")).asBytes())
            it.putRecord(OidNameRecord(intField(3), stringField("table3")).asBytes())
        }
        assertEquals(
            listOf("table2", "table3"),
            FullScanAccessImpl(cache, NAME_SYSTABLE_OID, rootRecords.iterator()) {
                OidNameRecord(intField(), stringField()).fromBytes(it)
            }.map { it.value2 }.toList()
        )
    }

    @Test
    fun `test multiple page table`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        cache.getAndPin(0).use {buf ->
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1000)).asBytes())
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1001)).asBytes())
        }
        val rootRecords = RootRecords(cache, 0, 1)
        cache.getAndPin(1000).let {
            it.putRecord(OidNameRecord(intField(2), stringField("table2")).asBytes())
            it.putRecord(OidNameRecord(intField(3), stringField("table3")).asBytes())
        }
        cache.getAndPin(1001).let {
            it.putRecord(OidNameRecord(intField(4), stringField("table4")).asBytes())
            it.putRecord(OidNameRecord(intField(5), stringField("table5")).asBytes())
        }
        assertEquals(
            listOf("table2", "table3", "table4", "table5"),
            FullScanAccessImpl(cache, NAME_SYSTABLE_OID, rootRecords.iterator()) {
                OidNameRecord(intField(), stringField()).fromBytes(it)
            }.map { it.value2 }.toList()
        )

    }

}