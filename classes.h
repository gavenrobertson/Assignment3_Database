#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <bitset>
#include <fstream>
#include <cmath>
#include <cstdio>
#include <cstdint>
using namespace std;

class Record
{
public:
    int id, manager_id;
    std::string bio, name;

    Record(vector<std::string> fields)
    {
        id = stoi(fields[0]);
        name = fields[1];
        bio = fields[2];
        manager_id = stoi(fields[3]);
    }

    void print()
    {
        cout << "\tID: " << id << "\n";
        cout << "\tNAME: " << name << "\n";
        cout << "\tBIO: " << bio << "\n";
        cout << "\tMANAGER_ID: " << manager_id << "\n";
    }

    // Calculate size of record to determine if it can fit in block
    int getSize() {
        // Include the size for all fields, delimiters, and a newline
        return to_string(id).length() + name.length() + bio.length() + to_string(manager_id).length() + 3 /* commas */ + 1 /* newline */;
    }

    void writeRecord(fstream &indexFile)
    {
        indexFile << id << ',' << name << ',' << bio << ',' << manager_id << '\n';
    }
};

class Block
{
private:
    const int PAGE_SIZE = 4096;

public:
    vector<Record> records;

    int blockSize;
    int overflowPtrIdx;
    int numRecords;
    int blockIdx;

    Block()
    {
        blockSize = 0;
        blockIdx = 0;
    }

    Block(int physIdx)
    {
        blockSize = 0;
        blockIdx = physIdx;
    }

    void readBlock(fstream &inputFile)
    {

        inputFile.seekg(blockIdx * PAGE_SIZE);

        inputFile.read(reinterpret_cast<char *>(&overflowPtrIdx), sizeof(overflowPtrIdx));
        inputFile.read(reinterpret_cast<char *>(&numRecords), sizeof(numRecords));

        blockSize += 8;

        for (int i = 0; i < numRecords; i++)
        {
            readRecord(inputFile);
            blockSize += records[i].getSize();
        }
    }

    void readRecord(fstream &inputFile) {
        string recordLine;
        if (!getline(inputFile, recordLine)) return; // Read a whole line for a record

        stringstream ss(recordLine);
        string field;
        vector<string> fields;

        while (getline(ss, field, ',')) {
            fields.push_back(field);
        }

        if (fields.size() >= 4) { // Ensure there are enough fields
            records.push_back(Record(fields));
        }
    }
};

class LinearHashIndex
{

private:
    const int PAGE_SIZE = 4096;

    vector<int> pageDirectory;
    int numBlocks;

    int numBuckets;
    int i;
    int numRecords;   // Records in index
    int nextFreePage; // Next page to write to
    string fName;     // Name of output index file

    int numOverflowBlocks;

    int currentTotalSize;

    Record getRecord(fstream &recordIn)
    {
        string line, word;
        vector<std::string> fields;

        if (!getline(recordIn, line, '\n'))
        {
            fields.push_back("-1");
            fields.push_back("-1");
            fields.push_back("-1");
            fields.push_back("-1");
            return Record(fields);
        }
        else
        {
            stringstream s(line);

            getline(s, word, ',');
            fields.push_back(word);
            getline(s, word, ',');
            fields.push_back(word);
            getline(s, word, ',');
            fields.push_back(word);
            getline(s, word, ',');
            fields.push_back(word);

            return Record(fields);
        }
    }

    int hash(int id)
    {
        return (id % (int)pow(2, 16));
    }

    int getLastIthBits(int hashVal, int i)
    {
        return hashVal & ((1 << i) - 1);
    }

    int initBucket(fstream &indexFile)
    {

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        indexFile.seekp(nextFreePage * PAGE_SIZE);

        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

        pageDirectory.push_back(nextFreePage++);
        numBlocks++;
        numBuckets++;

        // Update current total size
        currentTotalSize += sizeof(overflowIdx) + sizeof(defaultNumRecords);

        return pageDirectory.size() - 1;
    }

    int initOverflowBlock(int parentBlockIdx, fstream &indexFile)
    {

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        // Get index of current overflow block
        int currIdx = nextFreePage++;

        indexFile.seekp(currIdx * PAGE_SIZE);

        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

        indexFile.seekp(parentBlockIdx * PAGE_SIZE);
        indexFile.write(reinterpret_cast<const char *>(&currIdx), sizeof(currIdx));

        currentTotalSize += sizeof(overflowIdx) + sizeof(defaultNumRecords);

        // Update number of overflow blocks and blocks
        numBlocks++;
        numOverflowBlocks++;

        return currIdx;
    }

    int initEmptyBlock(fstream &indexFile)
    {

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        // Get index for current block
        int currIdx = nextFreePage++;

        indexFile.seekp(currIdx * PAGE_SIZE);

        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

        // Update current total size
        currentTotalSize += sizeof(overflowIdx) + sizeof(defaultNumRecords);

        numBlocks++;

        return currIdx;
    }

    // Write a record to a given position in the index file and update record count
    void writeRecordAndUpdateCountInPosition(Record &record, int pos, double totalBlocksize, int newNumRecords, fstream &indexFile)
    {
        indexFile.seekp(totalBlocksize);
        record.writeRecord(indexFile);
        indexFile.seekp(pos + sizeof(int));
        indexFile.write(reinterpret_cast<const char *>(&newNumRecords), sizeof(newNumRecords));

        // Update current total size
        currentTotalSize += record.getSize();
    }

    // Get overflow index and write record to overflow block
    void writeRecordToOverflowBlock(Record &record, int baseBlockPgIdx, fstream &indexFile)
    {
        int overflowIdx = initOverflowBlock(baseBlockPgIdx, indexFile);
        int pos = (overflowIdx * PAGE_SIZE);
        writeRecordAndUpdateCountInPosition(record, pos, pos + sizeof(int) + sizeof(int), 1, indexFile);
    }

    void writeRecordToIndexFile(Record record, int baseBlockPgIdx, fstream &indexFile)
    {
        bool hasWrittenRecord = false;
        while (!hasWrittenRecord)
        {
            Block currBlock(baseBlockPgIdx);
            currBlock.readBlock(indexFile);
            if (currBlock.blockSize + record.getSize() <= PAGE_SIZE)
            {
                int pos = (baseBlockPgIdx * PAGE_SIZE);
                double totalBlocksize = pos + currBlock.blockSize;
                writeRecordAndUpdateCountInPosition(record, pos, totalBlocksize, currBlock.numRecords + 1, indexFile);
                hasWrittenRecord = true;
            }
            else if (currBlock.overflowPtrIdx != -1)
            {
                baseBlockPgIdx = currBlock.overflowPtrIdx;
            }
            else
            {
                writeRecordToOverflowBlock(record, baseBlockPgIdx, indexFile);
                hasWrittenRecord = true;
            }
        }
    }

    // Initialize buckets if no records are present
    void initBucketsIfNecessary(fstream &indexFile)
    {
        if (numRecords == 0)
        {
            for (int i = 0; i < 2; i++)
            {
                initBucket(indexFile);
            }
            i = 1;
        }
    }

    // Write a record to the index file and update record count
    void writeRecordAndUpdateCount(Record &record, int pgIdx, fstream &indexFile)
    {
        writeRecordToIndexFile(record, pgIdx, indexFile);
        numRecords++;
    }

    // Handle situation if bucket overflows
    void handleBucketOverflow(fstream &indexFile)
    {
        double avgCapacityPerBucket = (double)currentTotalSize / numBuckets;
        double pageSizeMul = 0.7 * PAGE_SIZE;

        if (avgCapacityPerBucket > pageSizeMul)
        {
            int newBucketIdx = initBucket(indexFile);

            int digitsToAddressNewBucket = (int)ceil(log2(numBuckets));

            int bucketToTransferFromIdx = newBucketIdx;
            bucketToTransferFromIdx &= ~(1 << (digitsToAddressNewBucket - 1));

            int bucketToTransferFromPageIdx = pageDirectory[bucketToTransferFromIdx];

            int newOldBucketPageIdx = initEmptyBlock(indexFile);

            while (bucketToTransferFromPageIdx != -1)
            {

                Block oldBlock(bucketToTransferFromPageIdx);
                oldBlock.readBlock(indexFile);

                indexFile.seekp(oldBlock.blockIdx * PAGE_SIZE);
                indexFile << string(PAGE_SIZE, '*');

                numBlocks--;
                numOverflowBlocks--;

                currentTotalSize -= oldBlock.blockSize;
                numRecords -= oldBlock.numRecords;

                for (int i = 0; i < oldBlock.numRecords; i++)
                {

                    if (getLastIthBits(hash(oldBlock.records[i].id), digitsToAddressNewBucket) != newBucketIdx)
                    {
                        int tempNewOldBlockPgIdx = newOldBucketPageIdx;
                        writeRecordToIndexFile(oldBlock.records[i], tempNewOldBlockPgIdx, indexFile);
                    }
                    else
                    {
                        int newBucketBlockPgIdx = pageDirectory[newBucketIdx];
                        writeRecordToIndexFile(oldBlock.records[i], newBucketBlockPgIdx, indexFile);
                    }
                    numRecords++;
                }
                bucketToTransferFromPageIdx = oldBlock.overflowPtrIdx;
            }

            numOverflowBlocks++;

            pageDirectory[bucketToTransferFromIdx] = newOldBucketPageIdx;

            i = digitsToAddressNewBucket;
        }
    }

    void insertRecord(Record record, fstream &indexFile)
    {
        initBucketsIfNecessary(indexFile);
        int bucketIdx = getLastIthBits(hash(record.id), i);
        if (bucketIdx >= numBuckets)
        {
            bucketIdx &= ~(1 << (i - 1));
        }
        int pgIdx = pageDirectory[bucketIdx];
        writeRecordAndUpdateCount(record, pgIdx, indexFile);
        handleBucketOverflow(indexFile);
    }

public:
    LinearHashIndex(string indexFileName)
    {
        numBlocks = 0;
        i = 0;
        numRecords = 0;
        numBuckets = 0;
        fName = indexFileName;
        numOverflowBlocks = 0;
        currentTotalSize = 0;
        nextFreePage = 0;
    }

    void createFromFile(string csvFName)
    {
        fstream indexFile(fName, ios::in | ios::out | ios::trunc | ios::binary);
        fstream inputFile(csvFName, ios::in);

        if (inputFile.is_open())
            cout << "Employee.csv opened" << endl;

        bool recordsRemaining = true;

        while (recordsRemaining)
        {
            Record singleRec = getRecord(inputFile);

            if (singleRec.id == -1)
            {
                recordsRemaining = false;
            }
            else
            {
                insertRecord(singleRec, indexFile);
            }
        }
        indexFile.close();
        inputFile.close();
    }

    Record findRecordById(int id)
    {
        fstream indexFile(fName, ios::in);

        int bucketIdx = getLastIthBits(hash(id), i);

        if (bucketIdx >= numBuckets)
        {
            bucketIdx &= ~(1 << (i - 1));
        }

        int pgIdx = pageDirectory[bucketIdx];

        while (pgIdx != -1)
        {
            Block currBlock(pgIdx);
            currBlock.readBlock(indexFile);

            for (int i = 0; i < currBlock.numRecords; i++)
            {
                if (currBlock.records[i].id == id)
                    return currBlock.records[i];
            }
            cout << "Record not found";

            pgIdx = currBlock.overflowPtrIdx;
        }
        indexFile.close();
    }
};