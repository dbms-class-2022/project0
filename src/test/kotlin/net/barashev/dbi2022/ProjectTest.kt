package net.barashev.dbi2022.task0

import net.barashev.dbi2022.Record3
import net.barashev.dbi2022.dateField
import net.barashev.dbi2022.intField
import net.barashev.dbi2022.stringField
import org.junit.jupiter.api.Test
import java.util.*


class ProjectTest {

    @Test
    fun `table test`() {
        val taskSetup = TaskSetup()
        taskSetup.populateTables()

        // id, address, weather
        val table1 = taskSetup.accessManager.createFullScan("table1") {
            Record3(intField(), stringField(), stringField()).fromBytes(it)
        }

        for (record1 in table1) {

            // id, date, buzzwords
            val table2 = taskSetup.accessManager.createFullScan("table2") {
                Record3(intField(), dateField(), stringField()).fromBytes(it)
            }

            table2.filter {
                val calendar = Calendar.getInstance()
                calendar.time = it.value2
                it.value1 == record1.value1 && calendar.get(Calendar.YEAR) == 2024
            }
                .forEach{ println("${record1.value1} ${it.value2} ${record1.value3}") }
        }

        println(taskSetup.storage.totalAccessCost)
    }

}
