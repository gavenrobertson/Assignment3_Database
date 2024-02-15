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

    void print() {
        cout << "\tID: " << id << "\n";
        cout << "\tNAME: " << name << "\n";
        cout << "\tBIO: " << bio << "\n";
        cout << "\tMANAGER_ID: " << manager_id << "\n";
    }
};


class LinearHashIndex {

private:
    const int BLOCK_SIZE = 4096;

    vector<int> blockDirectory; // Map the least-significant-bits of h(id) to a bucket location in EmployeeIndex (e.g., the jth bucket)
                                // can scan to correct bucket using j*BLOCK_SIZE as offset (using seek function)
								// can initialize to a size of 256 (assume that we will never have more than 256 regular (i.e., non-overflow) buckets)
    int n;  // The number of indexes in blockDirectory currently being used
    int i;	// The number of least-significant-bits of h(id) to check. Will need to increase i once n > 2^i
    int numRecords;    // Records currently in index. Used to test whether to increase n
    int nextFreeBlock; // Next place to write a bucket. Should increment it by BLOCK_SIZE whenever a bucket is written to EmployeeIndex
    string fName;      // Name of index file

    // Insert new record into index
    void insertRecord(Record record) {

        // No records written to index yet
        if (numRecords == 0) {
            // Initialize index with first blocks (start with 4)

        }

        // Add record to the index in the correct block, creating a overflow block if necessary


        // Take neccessary steps if capacity is reached:
		// increase n; increase i (if necessary); place records in the new bucket that may have been originally misplaced due to a bit flip


    }

public:
    LinearHashIndex(string indexFileName) {
        n = 4; // Start with 4 buckets in index
        i = 2; // Need 2 bits to address 4 buckets
        numRecords = 0;
        nextFreeBlock = 0;
        fName = indexFileName;

        // Create your EmployeeIndex file and write out the initial 4 buckets
        // make sure to account for the created buckets by incrementing nextFreeBlock appropriately

            ofstream indexFile(fName);
            if (!indexFile.is_open()) {
            throw runtime_error("ERROR: The index file could not be opened.");
            }

        // Do we have a bucket defined or do we define it??
        // ** All I have so far **
    }

    // Read csv file and add records to the index

    // -- Using same function from assignment 2 to read in the CSV data for now //
    // See if it still works
    void createFromFile(string csvFName) {
        ifstream csvFile(csvFName);
        if (!csvFile.is_open()) {
            throw runtime_error("Error!!! The csvFile could not be opened!");
        }

        string line;
        while (getline(csvFile, line)) {
            istringstream ss(line);
            vector<string> fields;
            string field;
            while (getline(ss, field, ',')) {
                fields.push_back(field);
            }

            Record record(fields);
            addRecordToIndex(record);
        }
    }

    // Given an ID, find the relevant record and print it
    Record findRecordById(int id) {
        cout << "Searching for Record ID: " << id << endl;
        ifstream file("EmployeeIndex");
        if (!file.is_open()) {
            throw runtime_error("Unable to open the file.");
        }

        // Implement search function for the ID parameter
    }
};
