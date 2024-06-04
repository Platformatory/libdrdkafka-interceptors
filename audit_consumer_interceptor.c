#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <librdkafka/rdkafka.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <jansson.h>

struct ici {
        rd_kafka_conf_t *conf; /**< Interceptor config */
        rd_kafka_conf_t *audit_producer_conf;         /**< Interceptor-specific config */
        char *audit_topic;
};

/**
 * Generates a JSON payload for logging message production details.
 *
 * @param timestamp The timestamp of the action.
 * @param correlation_id The unique identifier for correlating logs.
 * @param topic The Kafka topic to which the message was produced.
 * @param partition The partition number of the topic.
 * @param offset The offset in the partition.
 * @return A string containing the JSON encoded payload, or NULL on error.
 */
char *generate_json_payload(const char *timestamp, const char *correlation_id,
                            const char *topic, const char *client, int partition, long offset) {
    /* Create a JSON object */
    json_t *root = json_object();
    if (!root) {
        fprintf(stderr, "Error creating JSON object.\n");
        return NULL;
    }
    /* Add data to the JSON object */
    json_object_set_new(root, "timestamp", json_string(timestamp));
    json_object_set_new(root, "correlation_id", json_string(correlation_id));
    json_object_set_new(root, "action", json_string("consume"));
    json_object_set_new(root, "topic", json_string(topic));
    json_object_set_new(root, "client", json_string(client));
    json_object_set_new(root, "partition", json_integer(partition));
    json_object_set_new(root, "offset", json_integer(offset));
    /* Encode the JSON object to a string */
    char *json_output = json_dumps(root, JSON_COMPACT);
    if (!json_output) {
        fprintf(stderr, "Error encoding JSON object.\n");
        json_decref(root);  // Clean up JSON object
        return NULL;
    }
    json_decref(root);  // Clean up JSON object
    return json_output;
}

// Function to initialize a producer specifically for audit messages
rd_kafka_t *init_audit_producer(struct ici *ici) {
    char errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_dup(ici->audit_producer_conf);

    rd_kafka_t *audit_rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!audit_rk) {
        fprintf(stderr, "Failed to create audit producer: %s\n", errstr);
    }
    return audit_rk;
}

// Consumer interceptor for on_consume
static rd_kafka_resp_err_t on_consume(rd_kafka_t *rk, rd_kafka_message_t *rkmessage, void *ic_opaque) {
    struct ici *ici = ic_opaque;
    if (!rkmessage || !rkmessage->payload) {
        printf("No message payload to process.\n");
        return RD_KAFKA_RESP_ERR_NO_ERROR;
    }

    rd_kafka_headers_t *headers = NULL;
    if (rd_kafka_message_headers(rkmessage, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR) {
        const void *correlation_id;
        size_t correlation_id_size;

        const rd_kafka_conf_t *consumer_conf = rd_kafka_conf(rk);
        char group_id[512];
        size_t group_id_size = sizeof(group_id);
        rd_kafka_conf_get(consumer_conf, "group.id", group_id, &group_id_size);
        const char *topic = rd_kafka_topic_name(rkmessage->rkt);
        if (rd_kafka_header_get(headers, 0, "correlation_id", &correlation_id, &correlation_id_size) == RD_KAFKA_RESP_ERR_NO_ERROR) {
            time_t rawtime;
            struct tm * timeinfo;

            time ( &rawtime );
            timeinfo = localtime ( &rawtime );



            rd_kafka_t *audit_rk = init_audit_producer(ici);
            if (audit_rk) {
                char *json_payload = generate_json_payload(asctime(timeinfo), correlation_id, topic, group_id, rkmessage->partition, rkmessage->offset);
                rd_kafka_producev(
                    audit_rk,
                    RD_KAFKA_V_TOPIC(ici->audit_topic),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_VALUE(json_payload, strlen(json_payload)),
                    RD_KAFKA_V_KEY(correlation_id, correlation_id_size),
                    RD_KAFKA_V_END
                );
                rd_kafka_flush(audit_rk, 10000); // Wait for messages to be delivered
                rd_kafka_destroy(audit_rk); // Clean up the audit producer
            }
        } else {
            fprintf(stderr, "AuditInterceptor: Correlation ID header missing.\n");
        }
    } else {
        fprintf(stderr, "AuditInterceptor: No headers found.\n");
    }

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static void ici_destroy(struct ici *ici) {
    if (ici->conf)
            rd_kafka_conf_destroy(ici->conf);
    if (ici->audit_producer_conf)
            free(ici->audit_producer_conf);
    if (ici->audit_topic)
            free(ici->audit_topic);
    free(ici);
}


static rd_kafka_resp_err_t on_new(rd_kafka_t *rk,
                                  const rd_kafka_conf_t *conf,
                                  void *ic_opaque,
                                  char *errstr,
                                  size_t errstr_size) {
    struct ici *ici = ic_opaque;

    rd_kafka_interceptor_add_on_consume(rk, __FILE__, on_consume, ici);
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static rd_kafka_conf_res_t on_conf_set(rd_kafka_conf_t *conf,
                                       const char *name,
                                       const char *val,
                                       char *errstr,
                                       size_t errstr_size,
                                       void *ic_opaque) {

    struct ici *ici = ic_opaque;
    int level       = 3;
    const char *prefix = "audit.";


    if (strcmp(name, "audit.topic")==0) {
        ici->audit_topic = strdup(val);
        return RD_KAFKA_CONF_OK;
    }
    

    if (strncmp(prefix, name, strlen(prefix)) == 0) {
        size_t prop_len = strlen(name)-strlen(prefix);
        char *prop = (char *)malloc((prop_len + 1) * sizeof(char));
        strncpy(prop, name+strlen(prefix), prop_len);
        rd_kafka_conf_set(ici->audit_producer_conf, prop, val, errstr, errstr_size);
        return RD_KAFKA_CONF_OK;
    }
     else {
        /* UNKNOWN makes the conf_set() call continue with
            * other interceptors and finally the librdkafka properties. */
        return RD_KAFKA_CONF_UNKNOWN;
    }

    return RD_KAFKA_CONF_UNKNOWN;
}

static rd_kafka_resp_err_t on_conf_destroy(void *ic_opaque) {
        struct ici *ici = ic_opaque;
        ici_destroy(ici);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static void conf_init0(rd_kafka_conf_t *conf) {
    struct ici *ici;

    ici = calloc(1, sizeof(*ici));
    ici->conf = rd_kafka_conf_new();
    ici->audit_producer_conf = rd_kafka_conf_new();

    // printf("ici %p with ici->conf %p", ici, ici->conf);
    
    rd_kafka_conf_interceptor_add_on_new(conf, __FILE__, on_new, ici);
    rd_kafka_conf_interceptor_add_on_conf_set(conf, __FILE__, on_conf_set, ici);
    rd_kafka_conf_interceptor_add_on_conf_destroy(conf, __FILE__,
                                                      on_conf_destroy, ici);
}

rd_kafka_resp_err_t conf_init(rd_kafka_conf_t *conf,
                              void **plug_opaquep,
                              char *errstr,
                              size_t errstr_size){

    conf_init0(conf);

    

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}