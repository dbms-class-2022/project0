package net.barashev.dbi2022.app

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import net.barashev.dbi2022.AccessMethodManager
import net.barashev.dbi2022.createHardDriveEmulatorStorage

fun main(args: Array<String>) = DBI2022().main(args)

class DBI2022: CliktCommand() {
    val cacheSize: Int by option(help="Page cache size [default=100]").int().default(100)
    val dataScale: Int by option(help="Test data scale [default=1]").int().default(1)
    val fixedRowCount by option(help="Shall the generated data amount be random").flag(default = false)
    val joinClause: String by option(help="JOIN clause, e.g. 'planet.id:flight.planet_id'").default("")
    val filterClause: String by option(help="Filter clause, e.g. 'planet.id = 1'").default("")
    val indexClause: String by option(help="Index clause to create indexes before running a query, e.g. flight.num").default("")
    val disableStatistics by option(help="Disable collection of attribute statistics [default=false]").flag(default = false)

    override fun run() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessMethodManager) = initializeFactories(storage, cacheSize)
        DataGenerator(accessMethodManager, cache, dataScale, fixedRowCount, disableStatistics)

        accessMethodManager.buildIndexes(parseIndexClause(indexClause))

        val costAfterIndexes = storage.totalAccessCost
        println("Access cost after creation of tables and indexes: $costAfterIndexes")
        val innerJoins = parseJoinClause(joinClause)
        val filters = parseFilterClause(filterClause)
        val plan = Optimizer.factory(accessMethodManager, cache).buildPlan(QueryPlan(innerJoins, filters))

        var rowCount = 0
        val joinResult = QueryExecutor(accessMethodManager, cache, tableRecordParsers, attributeValueParsers).run {
            execute(plan)
        }
        val joinedTables = joinResult.realTables
        accessMethodManager.createFullScan(joinResult.tableName) { bytes ->
            parseJoinedRecord(bytes, joinedTables, tableRecordParsers)
        }.forEach {
            rowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                println("$tableName: ${tableRecordParsers[tableName]!!.invoke(recordBytes)}")
            }
            println("----")
        }
        println("Actual plan cost: ${storage.totalAccessCost - costAfterIndexes}")
    }

}


fun AccessMethodManager.buildIndexes(specs: List<IndexSpec>) {
    specs.forEach { spec ->
        val attributeType = attributeTypes["${spec.tableName}.${spec.attributeName}"] ?: throw IllegalStateException("Can't find attribute type for $spec")
        val attributeValueParser = attributeValueParsers["${spec.tableName}.${spec.attributeName}"] ?: throw IllegalStateException("Can't find attribute value parser for $spec")
        createIndex(spec.tableName, spec.attributeName, attributeType, attributeValueParser)
    }
}