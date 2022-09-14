package net.barashev.dbi2022.task0

import net.barashev.dbi2022.*
import org.junit.jupiter.api.Test
import java.util.*

class DummyQueryTests {
    @Test
    fun `select joined tables with 2024 year`() {
        val taskSetup = TaskSetup()
        taskSetup.populateTables()

        val calendar: Calendar = Calendar.getInstance()

        val yearSelect = taskSetup.accessManager.iterate("table2", intField(), dateField(), stringField()) {
            calendar.time = it.value2
            calendar.get(Calendar.YEAR) == 2024
        }

        yearSelect.map { secondTableRow ->
            taskSetup.accessManager.iterate("table1", intField(), stringField(), stringField()) {
                it.value1 == secondTableRow.value1
            }.forEach { first ->
                println("${secondTableRow.value1} ${secondTableRow.value2} ${first.value3}")
            }
        }

        println("Table initialization cost: ${taskSetup.storage.totalAccessCost}")
    }

    private fun <T1 : Any, T2 : Any, T3 : Any> AccessMethodManager.iterate(
        tableName: String,
        column1: Pair<AttributeType<T1>, T1>,
        column2: Pair<AttributeType<T2>, T2>,
        column3: Pair<AttributeType<T3>, T3>,
        predicate: (Record3<T1, T2, T3>) -> Boolean
    ): Iterable<Record3<T1, T2, T3>> {
        return createFullScan(tableName) { Record3(column1, column2, column3).fromBytes(it) }.filter(predicate)
    }
}