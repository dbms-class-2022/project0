package net.barashev.dbi2022.task0

import net.barashev.dbi2022.Record3
import net.barashev.dbi2022.dateField
import net.barashev.dbi2022.intField
import net.barashev.dbi2022.stringField
import org.junit.jupiter.api.Test
import java.util.*

class Test0Test {
    @Test
    fun `Task0 Test`() {
        val taskSetup = TaskSetup()
        taskSetup.populateTables()

        val calendar = Calendar.getInstance()

        val it1 = taskSetup.accessManager.createFullScan("table1") {
            Record3(intField(), stringField(), stringField()).fromBytes(it)
        }

        for (record1 in it1) {
            val it2 = taskSetup.accessManager.createFullScan("table2") {
                Record3(intField(), dateField(), stringField()).fromBytes(it)
            }
            for (record2 in it2) {
                calendar.time = record2.value2
                val year = calendar.get(Calendar.YEAR)
                if (record1.value1 == record2.value1 && year == 2024) {
                    val res = Record3(intField(record1.value1), dateField(record2.value2), stringField(record1.value3))
                    println(res)
                }
            }
        }

        println("Table initialization cost: ${taskSetup.storage.totalAccessCost}")
    }
}