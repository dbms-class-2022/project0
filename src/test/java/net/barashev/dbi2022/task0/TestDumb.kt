package net.barashev.dbi2022.task0

import net.barashev.dbi2022.*
import org.junit.jupiter.api.Test
import java.util.*


class TestDumb {
    @Test
    fun test_dumb() {
        val taskSetup = TaskSetup()
        taskSetup.populateTables()
        for (i in taskSetup.accessManager.createFullScan("table1") { byteArray: ByteArray ->
            Record3(
                intField(),
                stringField(),
                stringField()
            ).fromBytes(byteArray)
        }) {
            for (j in taskSetup.accessManager.createFullScan("table2") { byteArray: ByteArray ->
                Record3(
                    intField(),
                    dateField(),
                    stringField()
                ).fromBytes(byteArray)
            }) {
                if (i.value1 == j.value1) {
                    val calendar: Calendar = Calendar.getInstance()
                    calendar.time = j.value2;
                    if (calendar.get(Calendar.YEAR) == 2024) {
                        println(j.value1.toString() + "  " + j.value2.toString() + " " + i.value3)
                    }
                }
            }
        }
        println("Table initialization cost: " + taskSetup.storage.totalAccessCost)
    }
}