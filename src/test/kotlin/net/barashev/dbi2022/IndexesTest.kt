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

class IndexesTest {
    val faker = Faker()
    @Test
    fun `fake index test`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleAccessMethodManager(cache)
        val fooOid = accessMethodManager.createTable("foo")
        cache.getAndPin(accessMethodManager.addPage(fooOid)).use {inPage ->
            (1..10).forEach { inPage.putRecord(Record2(intField(it), stringField(faker.name().fullName())).asBytes()) }
        }
        val index1 = IndexManager.indexFactory.build("foo", "foo_id_idx", keyType = IntAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value1
        }
        index1.lookup(10)


        val index2 = IndexManager.indexFactory.build("foo", "foo_name_idx", keyType = StringAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value2
        }
        index2.lookup("John Smith")
    }

}