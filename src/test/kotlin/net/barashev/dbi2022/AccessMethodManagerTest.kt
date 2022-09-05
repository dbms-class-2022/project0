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
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class AccessMethodManagerTest {
    @Test
    fun `test table is missing`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val catalog = SimpleAccessMethodManager(cache)
        assertThrows<AccessMethodException> {
            catalog.createFullScan("qwerty") { error("Not expected to be here") }
        }
    }

    @Test
    fun `create table`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val catalog = SimpleAccessMethodManager(cache)
        catalog.createTable("table1")
        val fullScan = catalog.createFullScan("table1") { error("Not expected to be here ")}
        assertEquals(listOf(), fullScan.toList())
    }

    @Test
    fun `add table page`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val catalog = SimpleAccessMethodManager(cache)
        val tableOid = catalog.createTable("table1")
        cache.getAndPin(catalog.addPage(tableOid)).use { dataPage ->
            dataPage.putRecord(Record2(intField(42), stringField("Hello world")).asBytes())
        }

        val fullScan = catalog.createFullScan("table1") {
            Record2(intField(), stringField()).fromBytes(it)
        }
        assertEquals(listOf(Record2(intField(42), stringField("Hello world"))), fullScan.toList())

    }


}