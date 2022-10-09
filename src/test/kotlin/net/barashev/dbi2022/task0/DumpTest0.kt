package net.barashev.dbi2022.task0

import net.barashev.dbi2022.*

import org.junit.jupiter.api.Test
import java.util.Calendar

class DumpTest0 {
    @Test
    fun testJoinTables() {
        val taskSetup = TaskSetup()
        taskSetup.populateTables()

        val accessManager = taskSetup.accessManager

        val table1 = accessManager.createFullScan("table1") { bytes ->
            Record3(intField(), stringField(), stringField()).fromBytes(bytes)
        }
        val table2 = accessManager.createFullScan("table2") { bytes ->
            Record3(intField(), dateField(), stringField()).fromBytes(bytes)
        }

        for (record2 in table2) {
            val id2 = record2.value1
            val date = record2.value2

            val calendar = Calendar.getInstance()
            calendar.setTime(record2.value2)
            val year = calendar.get(Calendar.YEAR)

            if (year == 2024) {
                for (record1 in table1) {
                    val id1 = record1.value1
                    val weather = record1.value3

                    if (id1 == id2) {
                        val result = Record3(intField(id1), dateField(date), stringField(weather))
                        println(result)
                    }
                }
            }
        }
        println(taskSetup.storage.totalAccessCost)
    }
}