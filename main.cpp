/*
Skeleton code for linear hash indexing
*/

#include <string>
#include <ios>
#include <fstream>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cmath>
#include "classes.h"
using namespace std;


int main(int argc, char* const argv[]) {

    // Create the index
    LinearHashIndex emp_index("EmployeeIndex.idx");  // Assuming .idx extension for clarity
    emp_index.createFromFile("Employee.csv");

    // Loop to lookup IDs until user is ready to quit
    while (true) {
        cout << "Enter an employee ID to look up, or type 'quit' to exit: ";
        string input;
        getline(cin, input);

        if (input == "quit") {
            break;  // Exit the loop if the user types 'quit'
        }

        try {
            int id = stoi(input);  // Convert input to integer
            Record foundRecord = emp_index.findRecordById(id);

            if (foundRecord.id != -1) { // Assuming -1 indicates not found
                foundRecord.print();
            } else {
                cout << "Record with ID " << id << " not found." << endl;
            }
        } catch (const invalid_argument& e) {
            // Handle case where the input cannot be converted to an integer
            cout << "Invalid ID. Please enter a numeric ID." << endl;
        } catch (const out_of_range& e) {
            // Handle case where the input integer is out of the range of int
            cout << "ID out of range. Please enter a smaller ID." << endl;
        }
    }

    return 0;
}
