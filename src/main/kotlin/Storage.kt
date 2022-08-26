import java.util.function.Consumer
import java.util.function.Function

typealias PageId = Int
typealias RecordId = Int

data class GetRecordResult(val bytes: ByteArray, val isDeleted: Boolean = false, val isOutOfRange: Boolean = false) {
    val isOk = !isDeleted && !isOutOfRange
}

data class PutRecordResult(val recordId: RecordId, val isOutOfSpace: Boolean = false, val isOutOfRange: Boolean = false) {
    val isOk = !isOutOfSpace && !isOutOfRange
}

/**
 * A toy disk page which allows for storing indexed records.
 *
 * A record is just a byte array, and its interpretation is left on the clients of this API. Record identifiers
 * are zero-based index values. It is assumed that all records in a single page occupy thw whole range of
 * index values from 0 until ... without gaps.
 *
 * Record identifiers are not guaranteed to be permanent, and implementation may or may not change then as
 * records are added or deleted. In particular, the implementation is free to choose how to handle record removals,
 * and may either remove it completely and thus change the ids of some records, or to put a tombstone instead.
 */
interface DiskPage {
    // Identifier of this page
    val id: PageId

    // Raw bytes
    val rawBytes: ByteArray

    // The number of bytes on this page, which are not occupied by records or auxiliary data. The reported
    // free space may or may not be sufficient for adding new records, depending on how records and auxiliary data
    // structures grow internally.
    val freeSpace: Int

    // Puts the passed record into the page. Record id is supposed to be in the range [0..recordCount]. Special value
    // -1 is accepted too, and is equivalent to recordCount.
    // If record id is equal to recordCount, a new record is added, otherwise the existing record is updated.
    // Boolean flags in the result indicate if put completed successfully or if there were any issues.
    fun putRecord(recordData: ByteArray, recordId: RecordId = -1): PutRecordResult

    // Returns a record by its id. In the result object a boolean flag isOk indicates whether the operation was successful.
    // If isOk is false, other boolean flags indicate what is the issue. In particular, a record may exist but may be
    // logically deleted, and in this case isDeleted flag is set to true.
    fun getRecord(recordId: RecordId): GetRecordResult

    // Deleted a record with the given id if it exists
    fun deleteRecord(recordId: RecordId)

    // Returns all records. Records which are logically deleted are returned as well.
    fun allRecords(): Map<RecordId, GetRecordResult>
}

interface Storage {
    fun readPage(pageId: PageId): DiskPage
    fun readPageSequence(startPageId: PageId = -1, numPages: Int = 1, reader: Consumer<DiskPage>)
    fun writePage(page: DiskPage)
    fun writePageSequence(startPageId: PageId = -1): Function<in DiskPage?, out DiskPage?>

    fun createPage(): DiskPage


    val totalAccessCost: Double
}