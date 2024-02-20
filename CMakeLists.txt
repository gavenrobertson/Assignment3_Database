cmake_minimum_required(VERSION 3.27)
project(Assignment3_Database)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(Assignment3_Database
        classes.h
        Employee.csv
        main.cpp)
