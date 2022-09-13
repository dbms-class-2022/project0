package net.barashev.dbi2022.task0

import net.barashev.dbi2022.*
import org.junit.jupiter.api.Test
import java.util.*

class MakeQuery {
    @Test
    fun makeQueryFun() {
        val taskSetup = TaskSetup()
        taskSetup.populateTables()

        val iter1 = taskSetup.accessManager.createFullScan("table1") { byteArray ->
            Record3(intField(), stringField(), stringField()).fromBytes(byteArray)
        }

        for (rec1 in iter1) {
            val iter2 = taskSetup.accessManager.createFullScan("table2") { byteArray ->
                Record3(intField(), dateField(), stringField()).fromBytes(byteArray)
            }

            for (rec2 in iter2) {
                if (rec1.value1 == rec2.value1 && rec2.value2.year + 1900 == 2024) {
                    println(Record3(intField(rec1.value1), dateField(rec2.value2), stringField(rec1.value3)))
                }
            }
        }

        println(taskSetup.storage.totalAccessCost)
    }
}