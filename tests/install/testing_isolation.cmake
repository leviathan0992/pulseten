# The shipped library contains no white-box access functions or fault strings.
foreach(required PULSETEN_NORMAL_LIBRARY PULSETEN_TEST_SUPPORT PULSETEN_NM)
  if(NOT DEFINED ${required})
    message(FATAL_ERROR "Missing ${required}")
  endif()
endforeach()

execute_process(COMMAND "${PULSETEN_NM}" -g "${PULSETEN_NORMAL_LIBRARY}"
  OUTPUT_VARIABLE normal_symbols ERROR_VARIABLE normal_error RESULT_VARIABLE result)
if(NOT result EQUAL 0)
  message(FATAL_ERROR "Cannot inspect normal library: ${normal_error}")
endif()
if(normal_symbols MATCHES "(db|db_tables|storage|wal)_test_access|query_bind_counter|query_test_prepared_data")
  message(FATAL_ERROR "Test access leaked into the normal library")
endif()
file(STRINGS "${PULSETEN_NORMAL_LIBRARY}" fault_strings
  REGEX "injected (flush|rollback) failure")
if(fault_strings)
  message(FATAL_ERROR "Fault injection leaked into the normal library")
endif()

execute_process(COMMAND "${PULSETEN_NM}" -g "${PULSETEN_TEST_SUPPORT}"
  OUTPUT_VARIABLE support_symbols ERROR_VARIABLE support_error RESULT_VARIABLE result)
if(NOT result EQUAL 0)
  message(FATAL_ERROR "Cannot inspect test support: ${support_error}")
endif()
if(NOT support_symbols MATCHES "db_test_access")
  message(FATAL_ERROR "Expected white-box access is missing from test support")
endif()
