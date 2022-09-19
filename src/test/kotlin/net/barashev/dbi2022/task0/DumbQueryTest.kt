package net.barashev.dbi2022.task0

import net.barashev.dbi2022.*
import org.junit.jupiter.api.Test
import java.time.ZoneId

class DumbQueryTest {

    @Test
    fun `test query`() {
        TaskSetup().apply {
            populateTables()

            accessManager.iterate("table2", intField(), dateField(), stringField()) {
                it.value2.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().year == 2024
            }.forEach { second ->
                accessManager.iterate("table1", intField(), stringField(), stringField()) {
                    it.value1 == second.value1
                }.forEach { first ->
                    println("${second.value1} ${second.value2} ${first.value3}")
                }
            }

            println()
            println("Table initialization cost: ${storage.totalAccessCost}")
        }
    }

    private fun <T1: Any, T2: Any, T3: Any> AccessMethodManager.iterate(
        tableName: String,
        f1: Pair<AttributeType<T1>, T1>,
        f2: Pair<AttributeType<T2>, T2>,
        f3: Pair<AttributeType<T3>, T3>,
        predicate: (Record3<T1, T2, T3>) -> Boolean = { true }
    ): Iterable<Record3<T1, T2, T3>> {
        return createFullScan(tableName) { Record3(f1, f2, f3).fromBytes(it) }.filter(predicate)
    }
}
