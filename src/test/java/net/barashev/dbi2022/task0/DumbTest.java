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

import kotlin.sequences.Sequence;
import net.barashev.dbi2022.Record3;
import org.junit.jupiter.api.Test;

import java.util.Date;

public class DumbTest {
    @Test
    public void testCreateTables() throws Exception {
        var taskSetup = new TaskSetup();
        taskSetup.populateTables();

        System.out.println("Table initialization cost: " + taskSetup.storage.getTotalAccessCost());
    }

    @Test
    public void testJoinCost() throws Exception {
        var taskSetup = new TaskSetup();
        taskSetup.populateTables();

        Sequence<Record3<Integer, Date, String>> resultSequence = DumbJoinKt.makeDumbJoin(taskSetup);
        var iter = resultSequence.iterator();

        while (iter.hasNext()) {
            Record3<Integer, Date, String> record = iter.next();
            System.out.println(record.getValue1() + " " + record.getValue2() + " " + record.getValue3());
        }
    }
}
