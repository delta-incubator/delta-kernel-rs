cmake_minimum_required(VERSION 3.12)
project(visit_expressions)

add_executable(visit_expression visit_expression.c)
target_compile_definitions(visit_expression PUBLIC DEFINE_DEFAULT_ENGINE)
target_include_directories(visit_expression PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers")
target_link_directories(visit_expression PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(visit_expression PUBLIC delta_kernel_ffi)
target_compile_options(visit_expression PUBLIC)

if(MSVC)
  target_compile_options(visit_expression PRIVATE /W4 /WX)
else()
  target_compile_options(visit_expression PRIVATE -Wall -Wextra -Wpedantic -Werror -Wno-strict-prototypes)
endif()

# Add the kernel expresion -> engine expression test
include(CTest)
set(ExprTestRunner "../../../tests/test_expression_visitor/run_test.sh")
set(ExprExpectedPath "../../../tests/test_expression_visitor/expected.txt")
add_test(NAME test_expression_visitor COMMAND ${ExprTestRunner} ${ExprExpectedPath})
add_test(NAME check_mem_leak COMMAND leaks -atExit -- ./visit_expression)
