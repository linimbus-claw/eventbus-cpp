cmake_minimum_required(VERSION 3.10)
project(EventBus CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# FindThreads for pthread
find_package(Threads REQUIRED)

# Main library (header-only)
set(HEADERS
    eventbus.hpp
)

# Test executable
add_executable(eventbus_test
    test_eventbus.cpp
    ${HEADERS}
)
target_link_libraries(eventbus_test PRIVATE Threads::Threads)

# Install target
install(FILES ${HEADERS} DESTINATION include)

# CTest
enable_testing()
add_test(NAME eventbus_test COMMAND eventbus_test)
