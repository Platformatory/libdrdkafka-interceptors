#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <jansson.h>
#include <uuid/uuid.h>

struct ici {
        rd_kafka_conf_t *conf; /**< Interceptor config */
        rd_kafka_conf_t *audit_producer_conf;         /**< Interceptor-specific config */
        char *audit_topic;
};

char *generate_correlation_id() {
    static long counter = 0;
    char *id = malloc(37);
    uuid_t binuuid;
    uuid_generate_random(binuuid);  // Generate a random UUID
    uuid_unparse_lower(binuuid, id);
    return id;
}

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
    json_object_set_new(root, "action", json_string("produce"));
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

static rd_kafka_resp_err_t on_send(rd_kafka_t *rk, rd_kafka_message_t *rkmessage, void *ic_opaque) {
    struct ici *ici = ic_opaque;
    if (!rkmessage || !rkmessage->payload) {
        fprintf(stderr, "No message payload to process.\n");
        return RD_KAFKA_RESP_ERR_NO_ERROR;
    }

    char *correlation_id = generate_correlation_id();
    const char *key = "your_secret_key_here";

    rd_kafka_headers_t *headers;
    rd_kafka_message_headers(rkmessage, &headers);
    if (headers) {
        headers = rd_kafka_headers_new(8);
    }

    rd_kafka_header_add(headers, "correlation_id", -1, correlation_id, -1);

    rd_kafka_message_set_headers(rkmessage, headers);
    free(correlation_id);

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t on_acknowledgement(rd_kafka_t *rk,
                                       rd_kafka_message_t *rkmessage,
                                       void *ic_opaque) {
        struct ici *ici = ic_opaque;

        const void *correlation_id;
        size_t correlation_id_size;

        const rd_kafka_conf_t *producer_conf = rd_kafka_conf(rk);
        char client_id[512];
        size_t client_id_size = sizeof(client_id);
        rd_kafka_conf_get(producer_conf, "client.id", client_id, &client_id_size);
        const char *topic = rd_kafka_topic_name(rkmessage->rkt);
        rd_kafka_headers_t *headers;
        rd_kafka_message_headers(rkmessage, &headers);
        if (!headers) {
            headers = rd_kafka_headers_new(8);
        }
   
    if (rd_kafka_header_get(headers, 0, "correlation_id", &correlation_id, &correlation_id_size) == RD_KAFKA_RESP_ERR_NO_ERROR) {
        time_t rawtime;
        struct tm * timeinfo;

        time ( &rawtime );
        timeinfo = localtime ( &rawtime );
            // Send the message to an audit topic using a separate producer instance
        rd_kafka_t *audit_rk = init_audit_producer(ici);
        if (audit_rk) {
            char *json_payload = generate_json_payload(asctime(timeinfo), correlation_id, topic, client_id, rkmessage->partition, rkmessage->offset);
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

    rd_kafka_interceptor_add_on_send(rk, __FILE__, on_send, ici);
    rd_kafka_interceptor_add_on_acknowledgement(rk, __FILE__,
                                                    on_acknowledgement, ici);
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

    // printf("conf_init: %p\n", conf);

    conf_init0(conf);

    

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

