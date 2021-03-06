add_executable(midi-redis dfly_main.cc)
cxx_link(midi-redis base dragonfly_lib)

add_library(dragonfly_lib command_registry.cc common_types.cc config_flags.cc 
            conn_context.cc db_slice.cc debugcmd.cc dragonfly_listener.cc
            dragonfly_connection.cc engine_shard_set.cc 
            main_service.cc memcache_parser.cc 
            redis_parser.cc resp_expr.cc reply_builder.cc)

cxx_link(dragonfly_lib uring_fiber_lib
         fibers_ext strings_lib http_server_lib tls_lib)

add_library(dfly_test_lib test_utils.cc)
cxx_link(dfly_test_lib dragonfly_lib gtest_main_ext)

cxx_test(redis_parser_test dfly_test_lib LABELS DFLY)
cxx_test(memcache_parser_test dfly_test_lib LABELS DFLY)
