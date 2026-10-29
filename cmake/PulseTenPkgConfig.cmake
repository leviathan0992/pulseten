# Configure a relocatable pkg-config file for the chosen GNUInstallDirs.
# Absolute install directories keep their configured absolute paths.
function(pulseten_configure_pkg_config output)
  file(RELATIVE_PATH prefix_relative
       "${CMAKE_INSTALL_FULL_LIBDIR}/pkgconfig" "${CMAKE_INSTALL_PREFIX}")
  set(PULSETEN_PC_PREFIX "\${pcfiledir}/${prefix_relative}")

  foreach(kind IN ITEMS LIBDIR INCLUDEDIR)
    if(IS_ABSOLUTE "${CMAKE_INSTALL_${kind}}")
      set(PULSETEN_PC_${kind} "${CMAKE_INSTALL_FULL_${kind}}")
    else()
      file(RELATIVE_PATH directory_relative
           "${CMAKE_INSTALL_PREFIX}" "${CMAKE_INSTALL_FULL_${kind}}")
      set(PULSETEN_PC_${kind} "\${prefix}/${directory_relative}")
    endif()
  endforeach()

  configure_file("${CMAKE_CURRENT_FUNCTION_LIST_DIR}/pulseten.pc.in"
                 "${output}" @ONLY)
endfunction()
