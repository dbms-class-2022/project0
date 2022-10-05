package net.barashev.dbi2022

import java.util.function.Function

enum class IndexMethod {
    BTREE, HASH
}
interface Index<T> {
    fun <T> lookup(indexKey: T): PageId?
}
private class StubIndex<T>: Index<T> {
    override fun <T> lookup(indexKey: T): PageId? {
        return null
    }
}

interface IndexFactory {
    fun <T, S: AttributeType<T>> build(tableName: String, indexTableName: String, method: IndexMethod = IndexMethod.BTREE, keyType: S, indexKey: Function<ByteArray, T>): Index<T>
    fun <T, S: AttributeType<T>> open(tableName: String, indexTableName: String, method: IndexMethod = IndexMethod.BTREE, keyType: S, indexKey: Function<ByteArray, T>): Index<T>
}

private class StubIndexFactory: IndexFactory {
    override fun <T, S : AttributeType<T>> build(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> = StubIndex()

    override fun <T, S : AttributeType<T>> open(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> = StubIndex()
}

object IndexManager {
    var indexFactory: IndexFactory = StubIndexFactory()
}
