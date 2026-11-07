#ifndef KAFKA_CLIENT_H
#define KAFKA_CLIENT_H

#include <librdkafka/rdkafka.h>
#include <pthread.h>

typedef enum {
	KAFKA_SUCCESS = 0,
	KAFKA_ERR_INIT,
	KAFKA_ERR_PRODUCE,
	KAFKA_ERR_SUBSCRIBE
} kafka_status_t;

typedef void (*kafka_msg_callback_t)(const char *topic, const char *payload);

typedef struct {
	rd_kafka_t *producer;
	rd_kafka_t *consumer;
	pthread_t consumer_thread;
	int is_running;
	kafka_msg_callback_t msg_callback;
} kafka_context_t;

kafka_status_t init_kafka(kafka_context_t *ctx, const char *bootstrap_servers, const char *group_id);

kafka_status_t kafka_send_message(kafka_context_t *ctx, const char *topic, const char *message);

kafka_status_t start_kafka_consumer(kafka_context_t *ctx, const char *topics_str, kafka_msg_callback_t callback);

void close_kafka(kafka_context_t *ctx);

#endif
