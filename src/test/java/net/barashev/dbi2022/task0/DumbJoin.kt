package net.barashev.dbi2022.task0

import net.barashev.dbi2022.Record3
import net.barashev.dbi2022.dateField
import net.barashev.dbi2022.intField
import net.barashev.dbi2022.stringField
import java.util.*

fun makeDumbJoin(taskSetup: TaskSetup): Sequence<Record3<Int, Date, String>> {
    val table2 = taskSetup.accessManager.createFullScan("table2") {
        Record3(intField(), dateField(), stringField()).fromBytes(it)
    }

    return table2.asSequence()
        .filter { record2 ->
            val date = record2.value2
            val calendar = Calendar.getInstance()
            calendar.time = date
            calendar.get(Calendar.YEAR) == 2024
        }
        .flatMap { record2 ->
            val id2 = record2.value1
            val date = record2.value2
            val table1 = taskSetup.accessManager.createFullScan("table1") {
                Record3(intField(), stringField(), stringField()).fromBytes(it)
            }
            table1.asSequence()
                .filter { record1 -> record1.value1 == id2 }
                .map { record1 -> Record3(intField(id2), dateField(date), stringField(record1.value3)) }
        }
}