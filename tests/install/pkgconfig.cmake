# Exercise the installed .pc file with pkg-config, including relocated trees.
cmake_minimum_required(VERSION 3.24)
include("${PULSETEN_SOURCE_DIR}/cmake/PulseTenPkgConfig.cmake")
set(PROJECT_VERSION 0.1.0)
set(PULSETEN_PC_LIBS "")

foreach(layout IN ITEMS lib lib64 lib/arch absolute)
  set(CMAKE_INSTALL_PREFIX "${PULSETEN_TEST_DIR}/${layout}/configured")
  set(CMAKE_INSTALL_LIBDIR "${layout}")
  set(CMAKE_INSTALL_INCLUDEDIR include)
  if("${layout}" STREQUAL "absolute")
    set(CMAKE_INSTALL_LIBDIR "${CMAKE_INSTALL_PREFIX}/absolute-lib")
    set(CMAKE_INSTALL_INCLUDEDIR "${CMAKE_INSTALL_PREFIX}/absolute-include")
  endif()

  foreach(kind IN ITEMS LIBDIR INCLUDEDIR)
    if(IS_ABSOLUTE "${CMAKE_INSTALL_${kind}}")
      set(CMAKE_INSTALL_FULL_${kind} "${CMAKE_INSTALL_${kind}}")
    else()
      set(CMAKE_INSTALL_FULL_${kind}
          "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_${kind}}")
    endif()
  endforeach()

  foreach(location IN ITEMS configured relocated)
    set(prefix "${PULSETEN_TEST_DIR}/${layout}/${location}")
    if("${layout}" STREQUAL "absolute")
      # Absolute paths are deliberately fixed, even when the .pc moves.
      set(expected_libdir "${CMAKE_INSTALL_FULL_LIBDIR}")
      set(expected_includedir "${CMAKE_INSTALL_FULL_INCLUDEDIR}")
      set(pcdir "${prefix}/absolute-lib/pkgconfig")
    else()
      set(expected_libdir "${prefix}/${CMAKE_INSTALL_LIBDIR}")
      set(expected_includedir "${prefix}/${CMAKE_INSTALL_INCLUDEDIR}")
      set(pcdir "${expected_libdir}/pkgconfig")
    endif()

    file(MAKE_DIRECTORY "${pcdir}")
    pulseten_configure_pkg_config("${pcdir}/pulseten.pc")
    foreach(variable IN ITEMS prefix libdir includedir)
      execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env
          "PKG_CONFIG_PATH=${pcdir}" "PKG_CONFIG_LIBDIR=${pcdir}"
          "${PULSETEN_PKG_CONFIG_EXECUTABLE}" "--variable=${variable}" pulseten
        RESULT_VARIABLE status OUTPUT_VARIABLE actual ERROR_VARIABLE error
        OUTPUT_STRIP_TRAILING_WHITESPACE)
      if(NOT status EQUAL 0)
        message(FATAL_ERROR "pkg-config failed: ${error}")
      endif()
      get_filename_component(actual "${actual}" ABSOLUTE)
      if("${variable}" STREQUAL "prefix")
        set(expected "${prefix}")
      else()
        set(expected "${expected_${variable}}")
      endif()
      if(NOT actual STREQUAL expected)
        message(FATAL_ERROR
          "${layout}/${location}: ${variable} is ${actual}, expected ${expected}")
      endif()
    endforeach()
  endforeach()
endforeach()
