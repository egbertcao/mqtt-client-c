
/**
 * @file
 * A simple program to that publishes the current time whenever ENTER is pressed. 
 */
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include "mqtt.h"
#include "posix_sockets.h"

typedef struct mqtt_session
{
    uint8_t sendbuf[1024]; /* sendbuf should be large enough to hold multiple whole mqtt messages */
    uint8_t recvbuf[1024]; /* recvbuf should be large enough any whole mqtt message expected to be received */
    struct mqtt_client client;
    pthread_t client_daemon;
    int sockfd;
} mqtt_session_t;

mqtt_session_t current_mqtt;

void system_mqtt_clean(int status, int sockfd, pthread_t *client_daemon)
{
    if (sockfd != -1) close(sockfd);
    if (client_daemon != NULL) pthread_cancel(*client_daemon);
    exit(status);
}

void publish_callback(void** unused, struct mqtt_response_publish *published) 
{
     /* note that published->topic_name is NOT null-terminated (here we'll change it to a c-string) */
    char* topic_name = (char*) malloc(published->topic_name_size + 1);
    memcpy(topic_name, published->topic_name, published->topic_name_size);
    topic_name[published->topic_name_size] = '\0';

    printf("Received publish('%s'): %s\n", topic_name, (const char*) published->application_message);

    free(topic_name);
}

void* client_refresher(void* client)
{
    while(1) 
    {
        mqtt_sync((struct mqtt_client*) client);
        usleep(100000U);
    }
    return NULL;
}

int system_mqtt_init(struct mqtt_client *client, pthread_t *client_daemon, int *sockfd)
{
    const char* addr;
    const char* port;
    const char* username;
    const char* password;

    if (client == NULL) {
        return MQTT_ERROR_NULLPTR;
    }

    addr = "182.61.41.198";
    port = "1883";
    username = "CaYhLkO3iI4NS5wl6hVM";
    password = NULL;
    
    /* open the non-blocking TCP socket (connecting to the broker) */
    *sockfd = open_nb_socket(addr, port);
    if (*sockfd == -1) {
        perror("Failed to open socket: ");
        system_mqtt_clean(EXIT_FAILURE, *sockfd, NULL);
    }
    
    mqtt_init(client, *sockfd, current_mqtt.sendbuf, sizeof(current_mqtt.sendbuf), current_mqtt.recvbuf, sizeof(current_mqtt.recvbuf), publish_callback);
    /* Create an anonymous session */
    const char* client_id = NULL;
    /* Ensure we have a clean session */
    uint8_t connect_flags = MQTT_CONNECT_CLEAN_SESSION;
    /* Send connection request to the broker. */
    mqtt_connect(client, client_id, NULL, NULL, 0, username, password, connect_flags, 400);
    /* check that we don't have any errors */
    if (client->error != MQTT_OK) {
        fprintf(stderr, "error: %s\n", mqtt_error_str(client->error));
        system_mqtt_clean(EXIT_FAILURE, *sockfd, NULL);
        return -1;
    }

    if(pthread_create(client_daemon, NULL, client_refresher, client)) {
        fprintf(stderr, "Failed to start client daemon.\n");
        system_mqtt_clean(EXIT_FAILURE, *sockfd, NULL);
        return -1;
    }
    sleep(3);
    return 0;
}

int main(int argc, const char *argv[]) 
{
    const char* topic = "v1/gateway/telemetry";
    const char* sub_topic = "v1/devices/me/rpc/request/+";
    int ret = system_mqtt_init(&current_mqtt.client, &current_mqtt.client_daemon, &current_mqtt.sockfd);
    if(ret != 0){
        printf("mqtt init failed.\n");
        return -1;
    }
    mqtt_subscribe(&current_mqtt.client, sub_topic, 0);

    while(1) {
        char *message = "{\"device1\":[{\"test1\":123}]}";
        ret = mqtt_publish(&current_mqtt.client, topic, message, strlen(message), MQTT_PUBLISH_QOS_0);
        printf("send result = %d\n", ret);
        if (current_mqtt.client.error != MQTT_OK) {
            fprintf(stderr, "error: %s\n", mqtt_error_str(current_mqtt.client.error));
            system_mqtt_clean(EXIT_FAILURE, current_mqtt.sockfd, &current_mqtt.client_daemon);
        }
        sleep(2);
    }   

    system_mqtt_clean(EXIT_SUCCESS, current_mqtt.sockfd, &current_mqtt.client_daemon);
}
