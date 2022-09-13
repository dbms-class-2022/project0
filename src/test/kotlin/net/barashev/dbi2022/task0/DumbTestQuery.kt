package net.barashev.dbi2022.task0

import net.barashev.dbi2022.Record3
import net.barashev.dbi2022.dateField
import net.barashev.dbi2022.intField
import net.barashev.dbi2022.stringField
import java.util.Calendar
import java.util.Date

private fun parseT1Record(bytes: ByteArray): Record3<Int, String, String> {
    return Record3(intField(), stringField(), stringField()).fromBytes(bytes)

}

private fun parseT2Record(bytes: ByteArray): Record3<Int, Date, String> {
    return Record3(intField(), dateField(), stringField()).fromBytes(bytes)

}

private fun joinRecords(
    t1Record: Record3<Int, String, String>,
    t2Record: Record3<Int, Date, String>
): Record3<Int, Date, String> {
    assert(t1Record.value1 == t2Record.value1)

    return Record3(intField(t1Record.value1), dateField(t2Record.value2), stringField(t1Record.value3))
}

fun executeDumbTestQuery(taskSetup: TaskSetup) {
    val accessManager = taskSetup.accessManager

    for (record1 in accessManager.createFullScan("table1") { bytes -> parseT1Record(bytes) }) {
        for (record2 in accessManager.createFullScan("table2") { bytes -> parseT2Record(bytes) }) {
            val calendar = Calendar.getInstance()
            calendar.time = record2.value2
            if (record1.value1 == record2.value1 && calendar.get(Calendar.YEAR) == 2024) {
                println(joinRecords(record1, record2))
            }
        }
    }
}