#ifndef rdkafka_shim_h
#define rdkafka_shim_h
#ifdef __APPLE__
#include "/usr/local/include/librdkafka/rdkafka.h"
#else
#include "/usr/include/librdkafka/rdkafka.h"
#endif
#endif
