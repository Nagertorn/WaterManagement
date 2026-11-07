#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "kafka_client.h"

kafka_status_t init_kafka(kafka_context_t *ctx, const char *bootstrap_servers, const char *group_id) {
    if (!ctx || !bootstrap_servers) return KAFKA_ERR_INIT;

    char errstr[512];
    ctx->is_running = 0;
    ctx->producer = NULL;
    ctx->consumer = NULL;

    // Productor
    rd_kafka_conf_t *prod_conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(prod_conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "[KAFKA ERROR] Producer config: %s\n", errstr);
        rd_kafka_conf_destroy(prod_conf);
        return KAFKA_ERR_INIT;
    }

    ctx->producer = rd_kafka_new(RD_KAFKA_PRODUCER, prod_conf, errstr, sizeof(errstr));
    if (!ctx->producer) {
        fprintf(stderr, "[KAFKA ERROR] Failed to create producer: %s\n", errstr);
        return KAFKA_ERR_INIT;
    }

    // Consumidor
    rd_kafka_conf_t *cons_conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(cons_conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(cons_conf, "group.id", group_id, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(cons_conf, "auto.offset.reset", "latest", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        
        fprintf(stderr, "[KAFKA ERROR] Consumer config: %s\n", errstr);
        rd_kafka_conf_destroy(cons_conf);
        close_kafka(ctx);
        return KAFKA_ERR_INIT;
    }

    ctx->consumer = rd_kafka_new(RD_KAFKA_CONSUMER, cons_conf, errstr, sizeof(errstr));
    if (!ctx->consumer) {
        fprintf(stderr, "[KAFKA ERROR] Failed to create consumer: %s\n", errstr);
        close_kafka(ctx);
        return KAFKA_ERR_INIT;
    }

    rd_kafka_poll_set_consumer(ctx->consumer);

    printf("[KAFKA] Client initialized successfully for %s\n", bootstrap_servers);
    return KAFKA_SUCCESS;
}

kafka_status_t kafka_send_message(kafka_context_t *ctx, const char *topic, const char *message) {
    if (!ctx || !ctx->producer || !topic || !message) return KAFKA_ERR_PRODUCE;

    int err = rd_kafka_producev(
        ctx->producer,
        RD_KAFKA_V_TOPIC(topic),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void*)message, strlen(message)),
        RD_KAFKA_V_END
    );

    if (err) {
        fprintf(stderr, "[KAFKA ERROR] Produce failed: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        return KAFKA_ERR_PRODUCE;
    }

    rd_kafka_poll(ctx->producer, 0);
    return KAFKA_SUCCESS;
}

static void* kafka_consumer_loop(void *arg) {
    kafka_context_t *ctx = (kafka_context_t *)arg;

    printf("[KAFKA] Consumer thread started listening...\n");

    while (ctx->is_running) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(ctx->consumer, 500);

        if (!rkmessage) continue;

        if (rkmessage->err) {
            if (rkmessage->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                fprintf(stderr, "[KAFKA ERROR] Consumer poll error: %s\n", 
                        rd_kafka_message_errstr(rkmessage));
            }
        } else {
            const char *topic_name = rd_kafka_topic_name(rkmessage->rkt);
            char payload[1024] = {0};

            if (rkmessage->len > 0) {
                snprintf(payload, sizeof(payload), "%.*s", (int)rkmessage->len, (char *)rkmessage->payload);

                if (ctx->msg_callback) {
                    ctx->msg_callback(topic_name, payload);
                }
            }
        }

        rd_kafka_message_destroy(rkmessage);
    }

    printf("[KAFKA] Consumer thread stopped.\n");
    return NULL;
}

kafka_status_t start_kafka_consumer(kafka_context_t *ctx, const char *topic, kafka_msg_callback_t callback) {
    if (!ctx || !ctx->consumer || !topic || !callback) return KAFKA_ERR_SUBSCRIBE;

    ctx->msg_callback = callback;

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);

    rd_kafka_resp_err_t err = rd_kafka_subscribe(ctx->consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    if (err) {
        fprintf(stderr, "[KAFKA ERROR] Failed to subscribe to %s: %s\n", topic, rd_kafka_err2str(err));
        return KAFKA_ERR_SUBSCRIBE;
    }

    ctx->is_running = 1;
    if (pthread_create(&ctx->consumer_thread, NULL, kafka_consumer_loop, ctx) != 0) {
        fprintf(stderr, "[KAFKA ERROR] Could not create consumer thread\n");
        ctx->is_running = 0;
        return KAFKA_ERR_SUBSCRIBE;
    }

    return KAFKA_SUCCESS;
}

void close_kafka(kafka_context_t *ctx) {
    if (!ctx) return;

    if (ctx->is_running) {
        ctx->is_running = 0;
        pthread_join(ctx->consumer_thread, NULL);
    }

    if (ctx->consumer) {
        rd_kafka_consumer_close(ctx->consumer);
        rd_kafka_destroy(ctx->consumer);
        ctx->consumer = NULL;
    }

    if (ctx->producer) {
        rd_kafka_flush(ctx->producer, 1000);
        rd_kafka_destroy(ctx->producer);
        ctx->producer = NULL;
    }

    printf("[KAFKA] Connections closed cleanly.\n");
}
