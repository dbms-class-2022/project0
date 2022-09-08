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

package net.barashev.dbi2022.task0;

import kotlin.random.Random;
import net.barashev.dbi2022.*;
import net.datafaker.Faker;

import java.util.concurrent.TimeUnit;

import static net.barashev.dbi2022.StorageImplKt.createHardDriveEmulatorStorage;

/**
 * This code creates two tables table1(id, address, weather) and table2(id, date, buzzwords) and
 * populates them with some data.
 */
public class TaskSetup {
    Storage storage = createHardDriveEmulatorStorage();
    PageCache cache = new SimplePageCacheImpl(storage, 20);
    AccessMethodManager accessManager = new SimpleAccessMethodManager(cache);

    int tableOid1 = accessManager.createTable("table1");
    int tableOid2 = accessManager.createTable("table2");

    private int table1Id = 0;
    private Faker faker = new Faker();

    void populateTables() throws Exception {

        var table1Page1 = accessManager.addPage(tableOid1, 15);
        for (int idx = 0; idx < 15; idx++) {
            try (var cachedPage = cache.getAndPin(table1Page1 + idx)) {
                populatePage1(cachedPage);
            }
        }
        var table2Page1 = accessManager.addPage(tableOid2, 25);
        for (int idx = 0; idx < 25; idx++) {
            try (var cachedPage = cache.getAndPin(table2Page1 + idx)) {
                populatePage2(cachedPage);
            }
        }

        cache.flush();
    }

    void populatePage1(CachedPage table1Page) {
        do {
            table1Id++;
            var putResult = table1Page.putRecord(new Record3(
                    RecordsKt.intField(table1Id),
                    RecordsKt.stringField(faker.address().fullAddress()),
                    RecordsKt.stringField(faker.weather().description())).asBytes(), -1);
            if (putResult.isOutOfSpace()) {
                return;
            }
        } while(true);
    }

    void populatePage2(CachedPage table2page) {
        do {
            var record = new Record3(
                    RecordsKt.intField(Random.Default.nextInt(1, table1Id)),
                    RecordsKt.dateField(faker.date().future(3650, TimeUnit.DAYS)),
                    RecordsKt.stringField(faker.marketing().buzzwords()));
            var putResult = table2page.putRecord(record.asBytes(), -1);
            table1Id++;
            if (putResult.isOutOfSpace()) {
                return;
            }
        } while(true);
    }
}
