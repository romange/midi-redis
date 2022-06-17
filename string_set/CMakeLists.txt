add_library(string_set_lib string_set.cc)
cxx_link(string_set_lib base redis_dict)

cxx_test(string_set_test string_set_lib)
