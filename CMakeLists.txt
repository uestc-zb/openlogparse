# 指定CMake的最低版本要求
cmake_minimum_required(VERSION 3.16)

# 定义项目名称、版本、描述和主页URL
project(OpenLogReplicator
        VERSION 1.8.5
        DESCRIPTION "OpenLogReplicator, Open Source Oracle database CDC"
        HOMEPAGE_URL "https://github.com/bersler/OpenLogReplicator"
)

# 设置SCHEMA版本和构建时间戳
set(SCHEMA_VERSION "1.8.5")
string(TIMESTAMP CMAKE_BUILD_TIMESTAMP "%Y-%m-%d %H:%M" UTC)

# 设置C++标准为C++17，并要求必须支持
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS FALSE)

# 如果定义了OSX_ARCH，则设置macOS架构
if (OSX_ARCH)
    set(CMAKE_OSX_ARCHITECTURES ${OSX_ARCH})
endif ()

# 检查是否在macOS上构建
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(CMAKE_MACOSX_RPATH 1)
    set(MACOSX TRUE)
endif()

# 设置发布版本的编译标志
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -O3 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual -Wpedantic -Wmisleading-indentation  -Wnull-dereference -Wdouble-promotion -Wimplicit-fallthrough -DCTXASSERT=0")

# 设置调试版本的编译标志，包括各种sanitizers
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual -Wpedantic -Wmisleading-indentation -Wnull-dereference -Wdouble-promotion -Wimplicit-fallthrough -pedantic  -fno-omit-frame-pointer -fsanitize=address -fsanitize=shift -fsanitize=shift-exponent -fsanitize=shift-base -fsanitize=integer-divide-by-zero -fsanitize=unreachable -fsanitize=vla-bound -fsanitize=null -fsanitize=return -fsanitize=signed-integer-overflow -fsanitize=bounds -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fsanitize=nonnull-attribute -fsanitize=bool -fsanitize=enum -fsanitize=vptr -fsanitize=pointer-overflow -fsanitize=builtin -DCTXASSERT=1")

# 设置调试版本的链接标志
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

# 配置config.h文件
configure_file(config.h.in ../config.h)

# 如果定义了CPU_ARCH，则设置架构相关的编译标志
if (CPU_ARCH)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=${CPU_ARCH} -mtune=${CPU_ARCH}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -march=${CPU_ARCH} -mtune=${CPU_ARCH}")
    add_compile_definitions(CPU_ARCH=${CPU_ARCH})
endif()

# 如果定义了THREAD_INFO，则添加编译定义
if (THREAD_INFO)
    add_compile_definitions(THREAD_INFO)
endif()

# 查找并链接线程库
find_package(Threads REQUIRED)

# 添加 libssh 支持
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBSSH REQUIRED libssh)

if(LIBSSH_FOUND)
    message(STATUS "Found libssh version: ${LIBSSH_VERSION}")
    include_directories(${LIBSSH_INCLUDE_DIRS})
    add_compile_definitions(LINK_LIBRARY_LIBSSH)
else()
    message(FATAL_ERROR "libssh not found. Please install libssh-dev")
endif()

# RapidJSON支持
if (WITH_RAPIDJSON)
    include_directories(SYSTEM ${WITH_RAPIDJSON}/include)
else ()
    message(FATAL_ERROR "plase provide WITH_RAPIDJSON parameter with path to rapidjson library")
endif ()

# Oracle OCI支持（仅动态链接）
if (WITH_OCI)
    include_directories(SYSTEM ${WITH_OCI}/sdk/include)
    link_directories(${WITH_OCI})
    add_compile_definitions(LINK_LIBRARY_OCI)
endif ()

# 静态链接支持
if (WITH_STATIC)
    add_compile_definitions(LINK_STATIC)
endif ()

# Protobuf支持（动态或静态）
if (WITH_PROTOBUF)
    include_directories(SYSTEM ${WITH_PROTOBUF}/include)
    add_compile_definitions(LINK_LIBRARY_PROTOBUF)

    if (WITH_STATIC)
        add_library(static_protobuf STATIC IMPORTED)
        set_target_properties(static_protobuf PROPERTIES IMPORTED_LOCATION ${WITH_PROTOBUF}/lib/libprotobuf.a)
    else ()
        link_directories(${WITH_PROTOBUF}/lib)
    endif ()

    # ZeroMQ支持（仅动态）
    if (WITH_ZEROMQ)
        include_directories(SYSTEM ${WITH_ZEROMQ}/include)
        add_compile_definitions(LINK_LIBRARY_ZEROMQ)
        if (MACOSX)
            link_directories(${WITH_ZEROMQ}/lib)
        else ()
            link_directories(${WITH_ZEROMQ}/lib/x86_64-linux-gnu)
        endif ()
    endif ()
endif ()

# Kafka支持（动态或静态）
if (WITH_RDKAFKA)
    include_directories(SYSTEM ${WITH_RDKAFKA}/include)
    add_compile_definitions(LINK_LIBRARY_RDKAFKA)

    if (WITH_STATIC)
        find_package(OpenSSL REQUIRED)
        add_library(static_rdkafka1 STATIC IMPORTED)
        add_library(static_rdkafka2 STATIC IMPORTED)
        set_target_properties(static_rdkafka1 PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka.a)
        set_target_properties(static_rdkafka2 PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka++.a)
        add_library(static_rdkafka INTERFACE IMPORTED)
        set_property(TARGET static_rdkafka PROPERTY INTERFACE_LINK_LIBRARIES static_rdkafka1 static_rdkafka2 OpenSSL::Crypto OpenSSL::SSL)
    else ()
        link_directories(${WITH_RDKAFKA}/lib)
    endif ()
endif ()

# Prometheus支持（仅动态）
if (WITH_PROMETHEUS)
    include_directories(SYSTEM ${WITH_PROMETHEUS}/include)
    link_directories(${WITH_PROMETHEUS}/lib)
    add_compile_definitions(LINK_LIBRARY_PROMETHEUS)
endif ()

# libhv支持
if (WITH_LIBHV)
    include_directories(SYSTEM ${WITH_LIBHV}/include)
    link_directories(${WITH_LIBHV}/lib)
    add_compile_definitions(LINK_LIBRARY_LIBHV)
else ()
    message(FATAL_ERROR "plase provide WITH_LIBHV parameter with path to libhv library")
endif ()

# 添加可执行文件OpenLogReplicator
add_executable(OpenLogReplicator ${SOURCE_FILES}
        src/ReplicatorManager.cpp
        src/ReplicatorManager.h
        src/ReplicatorHttpServer.cpp
        src/ReplicatorHttpServer.h
        src/main.cpp
        src/OpenLogReplicator.cpp
        src/OpenLogReplicator.h
        src/ReplicatorHttpServer.cpp
        src/ReplicatorHttpServer.h)

# 如果启用了Protobuf，则添加StreamClient可执行文件
if (WITH_PROTOBUF)
    add_executable(StreamClient ${SOURCE_FILES})
endif ()

# 添加src和tests子目录
add_subdirectory(src)
if (WITH_TESTS)
    add_subdirectory(tests)
endif ()

# 链接线程库到OpenLogReplicator
target_link_libraries(OpenLogReplicator Threads::Threads)

# 添加 libssh 链接
if(LIBSSH_FOUND)
    target_link_libraries(OpenLogReplicator ${LIBSSH_LIBRARIES})
    target_compile_options(OpenLogReplicator PRIVATE ${LIBSSH_CFLAGS_OTHER})
endif()

# 链接Oracle OCI库
if (WITH_OCI)
    target_link_libraries(OpenLogReplicator clntshcore nnz19 clntsh)
endif ()

# 链接Kafka库
if (WITH_RDKAFKA)
    if (WITH_STATIC)
        target_link_libraries(OpenLogReplicator static_rdkafka)
    else ()
        target_link_libraries(OpenLogReplicator rdkafka++ rdkafka)
    endif ()
endif ()

# 链接Prometheus库
if (WITH_PROMETHEUS)
    target_link_libraries(OpenLogReplicator prometheus-cpp-core prometheus-cpp-pull)
endif ()

# 链接libhv库
if (WITH_LIBHV)
    target_link_libraries(OpenLogReplicator hv)
endif ()

# 链接Protobuf和ZeroMQ库
if (WITH_PROTOBUF)
    if (WITH_STATIC)
        target_link_libraries(OpenLogReplicator static_protobuf)
        target_link_libraries(StreamClient static_protobuf)
    else ()
        target_link_libraries(OpenLogReplicator protobuf)
        target_link_libraries(StreamClient protobuf)
    endif ()
    target_link_libraries(StreamClient Threads::Threads)

    if (WITH_PROMETHEUS)
        target_link_libraries(StreamClient prometheus-cpp-core prometheus-cpp-pull)
    endif ()

    if (WITH_ZEROMQ)
        target_link_libraries(OpenLogReplicator zmq)
        target_link_libraries(StreamClient zmq)
    endif ()
endif ()

# 设置包含目录
target_include_directories(OpenLogReplicator PUBLIC "${PROJECT_BINARY_DIR}")

# 安装规则
install(TARGETS OpenLogReplicator
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

# 收集依赖库列表
set(DEPENDENCY_LIBS "")

# OCI依赖安装
if(WITH_OCI)
    # 安装OCI库文件
    install(DIRECTORY ${WITH_OCI}/ DESTINATION oci
            FILES_MATCHING
            PATTERN "libclntsh*.so*"
            PATTERN "libnnz*.so*"
            PATTERN "libocci*.so*"
            PATTERN "libociei*.so*"
    )

    # 创建运行脚本
    file(WRITE ${CMAKE_BINARY_DIR}/run_OpenLogReplicator.sh
            "#!/bin/bash\n"
            "export LD_LIBRARY_PATH=\"$(dirname \"$0\")/oci:$(dirname \"$0\")/lib:${LD_LIBRARY_PATH}\"\n"
            "$(dirname \"$0\")/bin/OpenLogReplicator \"$@\"\n"
    )
    install(PROGRAMS ${CMAKE_BINARY_DIR}/run_OpenLogReplicator.sh
            DESTINATION .
            RENAME OpenLogReplicator
    )

    list(APPEND DEPENDENCY_LIBS "oci")
endif()

# Protobuf依赖安装
if(WITH_PROTOBUF)
    # 安装Protobuf库
    install(FILES ${WITH_PROTOBUF}/lib/libprotobuf.so DESTINATION lib)
    install(FILES ${WITH_PROTOBUF}/lib/libprotobuf.so.32 DESTINATION lib)
    list(APPEND DEPENDENCY_LIBS "protobuf")

    # 安装ZeroMQ库
    if(WITH_ZEROMQ)
        if(MACOSX)
            install(FILES ${WITH_ZEROMQ}/lib/libzmq.so DESTINATION lib)
        else()
            install(FILES ${WITH_ZEROMQ}/lib/x86_64-linux-gnu/libzmq.so DESTINATION lib)
        endif()
        list(APPEND DEPENDENCY_LIBS "zeromq")
    endif()
endif()

# rdkafka依赖安装
if(WITH_RDKAFKA)
    install(FILES
            ${WITH_RDKAFKA}/lib/librdkafka++.so
            ${WITH_RDKAFKA}/lib/librdkafka.so
            ${WITH_RDKAFKA}/lib/librdkafka.so.1
            DESTINATION lib
    )
    list(APPEND DEPENDENCY_LIBS "rdkafka")
endif()

# Prometheus依赖安装
if(WITH_PROMETHEUS)
    install(FILES
            ${WITH_PROMETHEUS}/lib/libprometheus-cpp-core.so
            ${WITH_PROMETHEUS}/lib/libprometheus-cpp-core.so.1.3
            ${WITH_PROMETHEUS}/lib/libprometheus-cpp-pull.so
            ${WITH_PROMETHEUS}/lib/libprometheus-cpp-pull.so.1.3
            DESTINATION lib
    )
    list(APPEND DEPENDENCY_LIBS "prometheus")
endif()

# libhv依赖安装
if(WITH_LIBHV)
    install(FILES ${WITH_LIBHV}/lib/libhv.so DESTINATION lib)
    list(APPEND DEPENDENCY_LIBS "libhv")
endif()

# 安装其他文件
#install(FILES
#        ${CMAKE_SOURCE_DIR}/LICENSE
#        ${CMAKE_SOURCE_DIR}/config.json.example
#        DESTINATION .
#)

# 包含系统库安装
include(InstallRequiredSystemLibraries)

# CPack打包配置
set(CPACK_PACKAGE_NAME "${PROJECT_NAME}")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_VENDOR "OpenLogReplicator")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Open Source Oracle database CDC")
set(CPACK_PACKAGE_HOMEPAGE_URL "${PROJECT_HOMEPAGE_URL}")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")

# 设置打包格式为TGZ
set(CPACK_GENERATOR "TGZ")
set(CPACK_SOURCE_GENERATOR "TGZ")

# 设置打包目录结构
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)  # 不包含顶层目录
set(CPACK_PACKAGE_FILE_NAME "${PROJECT_NAME}-${PROJECT_VERSION}-linux-x64")

# 设置安装后脚本
set(CPACK_POST_INSTALL_SCRIPT "${CMAKE_BINARY_DIR}/cpack_post_install.cmake")
file(WRITE ${CMAKE_BINARY_DIR}/cpack_post_install.cmake
        "# 设置可执行权限
execute_process(COMMAND chmod +x \"\${CMAKE_INSTALL_PREFIX}/OpenLogReplicator\")")

include(CPack)

# 输出打包信息
message(STATUS "Packaging with dependencies: ${DEPENDENCY_LIBS}")