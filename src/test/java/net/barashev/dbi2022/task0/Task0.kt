package net.barashev.dbi2022.task0

import net.barashev.dbi2022.Record3
import net.barashev.dbi2022.dateField
import net.barashev.dbi2022.intField
import net.barashev.dbi2022.stringField
import org.junit.jupiter.api.Test
import java.util.*

class Task0 {

    @Test
    fun `test getFullScan`() {
        val calendar = Calendar.getInstance()
        val taskSetup = TaskSetup().apply {
            populateTables()

            accessManager.createFullScan("table2") {
                Record3(intField(), dateField(), stringField()).fromBytes(it)
            }.filter {
                calendar.time = it.value2
                calendar.get(Calendar.YEAR) == 2024
            }.forEach { itemFromTable2 ->
                accessManager.createFullScan("table1") {
                    Record3(intField(), stringField(), stringField()).fromBytes(it)
                }.filter { it.value1 == itemFromTable2.value1 }.forEach { itemFromTable1 ->
                    println(
                        Record3(
                            intField(itemFromTable1.value1),
                            dateField(itemFromTable2.value2),
                            stringField(itemFromTable1.value3)
                        )
                    )
                }
            }
        }
        println("Table initialization cost: ${taskSetup.storage.totalAccessCost}")
    }
}