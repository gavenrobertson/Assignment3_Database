#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
using namespace std;

class Record {
public:
    int id, manager_id;
    std::string bio, name;

    Record(vector<std::string> fields) {
        id = stoi(fields[0]);
        name = fields[1];
        bio = fields[2];
        manager_id = stoi(fields[3]);
    }

    bool operator==(const Record &other) const {
        return id == other.id; // or any other logic that defines equality
    }

    void print() {
        cout << "\tID: " << id << "\n";
        cout << "\tNAME: " << name << "\n";
        cout << "\tBIO: " << bio << "\n";
        cout << "\tMANAGER_ID: " << manager_id << "\n";
    }

    // Convert Record to a delimited string
    std::string toDelimitedString() const {
        std::ostringstream ss;
        ss << id << "," << name << "," << bio << "," << manager_id; // Use comma as delimiter
        return ss.str();
    }

    // Initialize a Record from a delimited string
    static Record fromDelimitedString(const std::string &data) {
        std::istringstream ss(data);
        std::vector<std::string> fields;
        std::string field;

        while (std::getline(ss, field, ',')) { // Use comma as delimiter
            fields.push_back(field);
        }

        if (fields.size() >= 4) { // Assuming at least 4 fields are present
            return Record(fields);
        } else {
            throw std::runtime_error("Invalid data format");
        }
    };

};

    class Bucket {
    public:
        std::vector<Record> records;
        int overflowOffset = -1;
        const double PAGE_USAGE_THRESHOLD = 0.7;


        // Method to serialize the bucket, including the overflow pointer
        std::string serialize() const {
            std::string bucketData;
            for (const auto& record : records) {
                bucketData += record.toDelimitedString();
            }
            bucketData += std::to_string(overflowOffset) + "|"; // Append overflow pointer
            return bucketData;
        }

        // Deserialize and add records from a delimited string to the bucket
        void deserialize(const std::string& bucketData) {
            std::istringstream ss(bucketData);
            std::string recordData;

            while (std::getline(ss, recordData, '|')) {
                try {
                    records.push_back(Record::fromDelimitedString(recordData));
                } catch (const std::runtime_error& e) {
                    std::cerr << "Error parsing record: " << e.what() << std::endl;
                    // Handle error or continue parsing next record
                }
            }
        }

        // Method to check if the bucket is full
        bool isFull(int blockSize) const {
            // Calculate 70% of the blockSize
            int threshold = static_cast<int>(blockSize * PAGE_USAGE_THRESHOLD);
            // Check if the serialized data exceeds this threshold
            return serialize().length() > threshold;
        }

    };

class LinearHashIndex {
private:
    const int BLOCK_SIZE = 4096;


    vector<int> blockDirectory; // Map the least-significant-bits of h(id) to a bucket location in EmployeeIndex (e.g., the jth bucket)
    // can scan to correct bucket using j*BLOCK_SIZE as offset (using seek function)
    // can initialize to a size of 256 (assume that we will never have more than 256 regular (i.e., non-overflow) buckets)
    int n;  // The number of indexes in blockDirectory currently being used
    int i;    // The number of least-significant-bits of h(id) to check. Will need to increase i once n > 2^i
    int numRecords;    // Records currently in index. Used to test whether to increase n
    int nextFreeBlock; // Next place to write a bucket. Should increment it by BLOCK_SIZE whenever a bucket is written to EmployeeIndex
    string fName;      // Name of index file


    int writeBucketToFile(const Bucket &bucket) {
        fstream indexFile(fName,
                          ios::in | ios::out | ios::binary | ios::ate); // Open file for reading, writing, and append
        int position = indexFile.tellp(); // Get the current end of file position
        std::string bucketData = bucket.serialize(); // Serialize the bucket
        indexFile.write(bucketData.c_str(), bucketData.length()); // Write the bucket data
        indexFile.close();
        return position; // Return the position where the bucket was written
    }


    // Insert new record into index
    void insertRecord(const Record &record) {
        // Calculate if any bucket exceeds the 70% threshold before inserting the new record
        bool needsResizing = false;
        for (const auto& position : blockDirectory) {
            if (position != -1) { // Assuming -1 indicates an unused entry
                Bucket bucket = readBucketAtPosition(position * BLOCK_SIZE);
                if (bucket.isFull(BLOCK_SIZE)) {
                    needsResizing = true;
                    break;
                }
            }
        }

        // Resize index if needed
        if (needsResizing) {
            resizeIndex();
        }

        // Proceed with the original insertion logic
        int bucketIndex = record.id % (1 << i);
        if (bucketIndex < n) {
            bucketIndex = record.id % (1 << (i + 1)); // Adjust for linear hashing if needed
        }

        // Calculate the position in the file based on the bucket index
        int position = blockDirectory[bucketIndex] * BLOCK_SIZE;

        Bucket bucket = readBucketAtPosition(position);

        if (!bucket.isFull(BLOCK_SIZE)) {
            bucket.records.push_back(record);
            numRecords++; // Increment total record count after successful insertion
        } else {
            handleOverflow(bucket, record, position);
        }

        // Write the (potentially updated) primary bucket back to the file
        fstream indexFile(fName, ios::in | ios::out | ios::binary);
        indexFile.seekp(position); // Go back to the primary bucket's position
        std::string updatedBucketData = bucket.serialize();
        indexFile.write(updatedBucketData.c_str(), updatedBucketData.length());
        indexFile.close();
    }

    void resizeIndex() {
        // Increment 'n' and adjust 'i' if necessary
        n++;
        if (n > (1 << i)) {
            i++;
        }

        // Identify the split bucket that needs rehashing
        int splitBucketIndex = n - 1;

        // Load the split bucket
        int position = blockDirectory[splitBucketIndex] * BLOCK_SIZE;
        Bucket splitBucket = readBucketAtPosition(position);

        // Vector to store records that need to be moved to a new bucket
        vector<Record> recordsToMove;

        // Iterate over records in the split bucket to determine which need rehashing
        for (const auto& record : splitBucket.records) {
            int newBucketIndex = record.id % (1 << i);
            if (newBucketIndex != splitBucketIndex) {
                recordsToMove.push_back(record);
            }
        }

        // Remove records that are moving to a new bucket
        splitBucket.records.erase(
                remove_if(splitBucket.records.begin(), splitBucket.records.end(), [&](const Record& record) {
                    return find(recordsToMove.begin(), recordsToMove.end(), record) != recordsToMove.end();
                }), splitBucket.records.end());

        // Update the split bucket in the file
        updateBucketInFile(splitBucket, position);

        // Initialize and write the new bucket
        Bucket newBucket;
        for (const auto& record : recordsToMove) {
            // Handle the new bucket insertion, considering possible overflow
            newBucket.records.push_back(record);
        }

        // Write the new bucket to the file and update the directory
        int newPosition = writeBucketToFile(newBucket);
        blockDirectory.push_back(newPosition / BLOCK_SIZE);
    }



    void handleOverflow(Bucket& currentBucket, const Record& record, int& currentBucketPosition) {
        // Open the file for binary read and write
        fstream indexFile(fName, ios::in | ios::out | ios::binary);

        if (currentBucket.overflowOffset == -1) {  // No existing overflow
            Bucket newOverflowBucket;
            newOverflowBucket.records.push_back(record);
            int newOverflowPosition = writeBucketToFile(newOverflowBucket);
            currentBucket.overflowOffset = newOverflowPosition;

            // Update the current bucket with the new overflowOffset
            indexFile.seekp(currentBucketPosition);
            indexFile.write(currentBucket.serialize().c_str(), currentBucket.serialize().length());
        } else {  // Existing overflow
            int overflowPosition = currentBucket.overflowOffset;
            Bucket overflowBucket = readBucketAtPosition(overflowPosition);

            if (overflowBucket.isFull(BLOCK_SIZE)) {
                // Recursively handle further overflow in the chain
                handleOverflow(overflowBucket, record, overflowPosition);
                // Update the overflow bucket if a new overflow was added in the recursion
                indexFile.seekp(overflowPosition);
                indexFile.write(overflowBucket.serialize().c_str(), overflowBucket.serialize().length());
            } else {
                overflowBucket.records.push_back(record);
                // Update the overflow bucket directly
                indexFile.seekp(overflowPosition);
                indexFile.write(overflowBucket.serialize().c_str(), overflowBucket.serialize().length());
            }
        }

        indexFile.close();
    }

    void updateBucketInFile(const Bucket& bucket, int position) {
        fstream indexFile(fName, ios::in | ios::out | ios::binary);
        indexFile.seekp(position);
        std::string bucketData = bucket.serialize();
        indexFile.write(bucketData.c_str(), bucketData.length());
        indexFile.close();
    }

    Bucket readBucketAtPosition(int position) {
        fstream indexFile(fName, ios::in | ios::binary);
        indexFile.seekg(position);
        std::string bucketData;
        std::getline(indexFile, bucketData, '|'); // Assume each bucket ends with a '|'
        indexFile.close();
        Bucket bucket;
        bucket.deserialize(bucketData);
        return bucket;
    }



public:
    LinearHashIndex(string indexFileName) : fName(indexFileName) {
    n = 4; // Start with 4 buckets in index
    i = 2; // Need 2 bits to address 4 buckets
    numRecords = 0;
    nextFreeBlock = BLOCK_SIZE * n; // Assuming the initial buckets are written back-to-back
    blockDirectory.resize(256, -1); // Initialize with -1 indicating unused

    // Open the index file in binary mode
    ofstream indexFile(fName, ios::binary | ios::out);

    // Check if the file is successfully opened
    if (!indexFile) {
        cerr << "Cannot open index file: " << fName << endl;
        exit(1);
    }

        // Dynamically allocate the emptyBucket array
        char* emptyBucket = new char[BLOCK_SIZE];

        // Initialize the array
        memset(emptyBucket, 0, BLOCK_SIZE);


    for (int j = 0; j < n; ++j) {
        blockDirectory[j] = j * BLOCK_SIZE; // Set directory to point to the start of each bucket
        indexFile.write(emptyBucket, BLOCK_SIZE); // Write an empty bucket to the file
    }

    delete[] emptyBucket;

    indexFile.close();
}

    // Read csv file and add records to the index
    void createFromFile(const string& csvFName) {
        ifstream csvFile(csvFName);
        if (!csvFile.is_open()) {
            cerr << "Failed to open file: " << csvFName << endl;
            return;
        }

        string line;
        while (getline(csvFile, line)) {
            istringstream ss(line);
            vector<string> fields;
            string field;

            while (getline(ss, field, ',')) {  // Assuming fields are comma-separated
                fields.push_back(field);
            }

            if (fields.size() != 4) {  // Assuming each line should have 4 fields
                cerr << "Invalid line format: " << line << endl;
                continue;  // Skip this line and move to the next
            }

            try {
                Record record(fields);  // Create a record from the fields
                insertRecord(record);   // Insert the record into the index
            } catch (const exception& e) {
                cerr << "Failed to create or insert record: " << e.what() << endl;
            }
        }

        csvFile.close();
    }


    // Given an ID, find the relevant record and print it
    //Record findRecordById(int id) {
        
   // }
};
