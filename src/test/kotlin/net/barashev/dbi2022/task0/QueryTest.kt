package net.barashev.dbi2022.task0

import net.barashev.dbi2022.*
import org.junit.jupiter.api.Test
import java.util.*

class QueryTest {

    @Test
    fun `test query`() {
        TaskSetup().apply {
            val calendar: Calendar = Calendar.getInstance()
            populateTables()

            accessManager.createFullScan("table2") {
                Record3(intField(), dateField(), stringField()).fromBytes(it)
            }.filter {
                calendar.time = it.value2
                calendar.get(Calendar.YEAR) == 2024
            }.forEach { secondTableValues ->
                accessManager.createFullScan("table1") {
                    Record3(intField(), stringField(), stringField()).fromBytes(it)
                }.filter {
                    it.value1 == secondTableValues.value1
                }.forEach { firstTableValues ->
                    println("${secondTableValues.value1} ${secondTableValues.value2} ${firstTableValues.value3}")
                }
            }

            println("\nTable initialization cost: ${storage.totalAccessCost}")
        }
    }
}
