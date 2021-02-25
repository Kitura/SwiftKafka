//
// Created by kacper on 25/02/2021.
//

#include "kafkahelper.h"

static const rd_kafka_topic_result_t * topic_result_by_idx (const rd_kafka_topic_result_t **topics, size_t cnt, size_t idx) {
    if (idx >= cnt)
        return NULL;
    return topics[idx];
}

static const rd_kafka_ConfigResource_t * ConfigResource_by_idx (const rd_kafka_ConfigResource_t **res, size_t cnt, size_t idx) {
    if (idx >= cnt)
        return NULL;
    return res[idx];
}

static const rd_kafka_ConfigEntry_t * ConfigEntry_by_idx (const rd_kafka_ConfigEntry_t **entries, size_t cnt, size_t idx) {
    if (idx >= cnt)
        return NULL;
    return entries[idx];
}