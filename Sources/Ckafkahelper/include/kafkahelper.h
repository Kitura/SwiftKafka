//
// Created by kacper on 25/02/2021.
//

#ifndef SWIFTKAFKA_KAFKAHELPER_H
#define SWIFTKAFKA_KAFKAHELPER_H

#ifdef __APPLE__
#include "/usr/local/include/librdkafka/rdkafka.h"
#else
#include "/usr/include/librdkafka/rdkafka.h"
#endif //__APPLE__

#include <stdlib.h>

static const rd_kafka_topic_result_t * topic_result_by_idx (const rd_kafka_topic_result_t **topics, size_t cnt, size_t idx) {
    if (idx >= cnt)
        return NULL;
    return topics[idx];
}

static const rd_kafka_ConfigResource_t * ConfigResource_by_idx (const rd_kafka_ConfigResource_t **res, size_t cnt, size_t idx);
static const rd_kafka_ConfigEntry_t * ConfigEntry_by_idx (const rd_kafka_ConfigEntry_t **entries, size_t cnt, size_t idx);
#endif //SWIFTKAFKA_KAFKAHELPER_H
