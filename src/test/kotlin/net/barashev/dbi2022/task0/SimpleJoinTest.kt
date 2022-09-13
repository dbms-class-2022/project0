package net.barashev.dbi2022.task0

import net.barashev.dbi2022.Record3
import net.barashev.dbi2022.dateField
import net.barashev.dbi2022.intField
import net.barashev.dbi2022.stringField
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.math.max

class SimpleJoinTest {

    @Test
    fun testJoin() {
        val taskSetup = TaskSetup()
        taskSetup.populateTables()

        val catalog = taskSetup.accessManager

        val table1Scan = catalog.createFullScan("table1") { bytes ->
            Record3(intField(), stringField(), stringField()).fromBytes(bytes).toT1Record()
        }

        val joinResult = table1Scan.flatMap { t1Record ->
            val table2Scan = catalog.createFullScan("table2") { bytes ->
                Record3(intField(), dateField(), stringField()).fromBytes(bytes).toT2Record()
            }
            table2Scan.filter { t2Record ->
                t2Record.id == t1Record.id && t2Record.date.myYear == 2024
            }.map { t2Record ->
                Record3(intField(t1Record.id), dateField(t2Record.date), stringField(t1Record.weather))
            }
        }
        println(resultsAsPostrgesqlOutput(joinResult))
        println("Total test cost: " + taskSetup.storage.totalAccessCost)
    }

    private fun resultsAsPostrgesqlOutput(joinResult: List<Record3<Int, Date, String>>): String {
        data class MaxColumnsSizes(
            var idLen: Int = 0,
            var dateLen: Int = 0,
            var weatherLen: Int = 0
        )

        val maxColumnsSizes = MaxColumnsSizes()
        joinResult.forEach { record ->
            val (id, date, weather) = record
            maxColumnsSizes.idLen = max(maxColumnsSizes.idLen, id.toString().length)
            maxColumnsSizes.dateLen = max(maxColumnsSizes.dateLen, date.toString().length)
            maxColumnsSizes.weatherLen = max(maxColumnsSizes.weatherLen, weather.length)
        }

        fun toColumnString(value: Any, maxColumnsSize: Int): String {
            val valueString = value.toString()
            val spaces = maxColumnsSize - valueString.length
            val spacesRight = spaces / 2
            val spacesLeft = spaces - spacesRight
            return "${" ".repeat(spacesLeft)}$valueString${" ".repeat(spacesRight)}"
        }

        fun toIdColumnString(value: Any): String =
            toColumnString(value, maxColumnsSizes.idLen)

        fun toDateColumnString(value: Any): String =
            toColumnString(value, maxColumnsSizes.dateLen)

        fun toWeatherColumnString(value: Any): String =
            toColumnString(value, maxColumnsSizes.weatherLen)

        val titleRowString =
            " ${toIdColumnString("id")} | ${toDateColumnString("date")} | ${toWeatherColumnString("weather")} "
        val separatorsRowString =
            "${
                "-".repeat(maxColumnsSizes.idLen + 2)
            }+${
                "-".repeat(maxColumnsSizes.dateLen + 2)
            }+${
                "-".repeat(maxColumnsSizes.weatherLen + 2)
            }"
        return joinResult.joinToString(
            separator = "\n",
            prefix = "$titleRowString\n$separatorsRowString\n",
            postfix = "\n(${joinResult.size} rows)\n"
        ) { record ->
            val (id, date, weather) = record
            " ${toIdColumnString(id)} | ${toDateColumnString(date)} | ${toWeatherColumnString(weather)} "
        }
    }

    private data class T1Record(val id: Int, val address: String, val weather: String)
    private data class T2Record(val id: Int, val date: Date, val buzzwords: String)

    private fun Record3<Int, String, String>.toT1Record(): T1Record =
        T1Record(this.value1, this.value2, this.value3)

    private fun Record3<Int, Date, String>.toT2Record(): T2Record =
        T2Record(this.value1, this.value2, this.value3)

    private operator fun <T1 : Any, T2 : Any, T3 : Any> Record3<T1, T2, T3>.component1() = this.value1
    private operator fun <T1 : Any, T2 : Any, T3 : Any> Record3<T1, T2, T3>.component2() = this.value2
    private operator fun <T1 : Any, T2 : Any, T3 : Any> Record3<T1, T2, T3>.component3() = this.value3

    private val Date.myYear: Int
        get() {
            val calendar = Calendar.getInstance()
            calendar.time = this
            return calendar.get(Calendar.YEAR)
        }
}