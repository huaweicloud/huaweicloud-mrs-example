#include <time.h>
#include "kerberos.h"


int getUserName(struct client_state *state) {
    if (state->auth_complete == 0) {
        return -1;
    }
    int result = 0;
    char *propvalue = NULL;

    result = sasl_getprop(state->sasl_conn, SASL_USERNAME, (const void **)&propvalue);
    if (result != SASL_OK || propvalue == NULL) {
        return -1;
    }
    state->username = propvalue;
    return 0;
}

void initializeClient() {
    int result = sasl_client_init(NULL);
    if (result != SASL_OK) {
        printf("init client failed");
        return;
    }
}

gss_result start(const char* full_server_realm, struct client_state *state) {
    gss_result result = {
            .code = 0
    };
    if (state->auth_started) {
        result.code = -1;
        result.message = "auth already started";
        return result;

    }
    sasl_conn_t *sasl_conn = NULL;
    int res = sasl_client_new(APP_NAME, full_server_realm, NULL, NULL, NULL, 0, &sasl_conn);
    if (res != SASL_OK) {
        result.code = -1;
        result.message = sasl_errdetail(state->sasl_conn);
        return result;
    }
    state->sasl_conn = sasl_conn;
    state->auth_started = 1;
    state->auth_complete = 0;
    char *mechUsing;
    res = sasl_client_start(state->sasl_conn, AUTH_MECHANISM, NULL, (const char **) &result.response, &result.len,
                            (const char **) &mechUsing);
    if (res == SASL_OK) {
        state->auth_complete = 1;
        sasl_dispose(&state->sasl_conn);
        result.response = "ok";
        return result;
    } else if (res != SASL_CONTINUE) {
        const char* err = sasl_errdetail(state->sasl_conn);
        sasl_dispose(&state->sasl_conn);
        result.code = -1;
        result.message = err;
        return result;
    }
    return result;
}

gss_result step(const char* challenge, size_t len, struct client_state *state) {
    gss_result result = {
            .code = 0
    };
    if (!state->auth_started) {
        result.code = -1;
        result.message = "not started";
        printf("not started");
        return result;
    }
    int res = sasl_client_step(state->sasl_conn, challenge, len, NULL, (const char **) &result.response, &result.len);
    if (res == SASL_OK) {
        state->auth_complete = 1;
        getUserName(state);
        return result;
    } else if (res != SASL_CONTINUE) {
        const char* sasl_err = sasl_errdetail(state->sasl_conn);
        result.code = -1;
        result.message = sasl_err;
        return result;
    }
    return result;
}

void close_sasl(struct client_state *state) {
    if (state->sasl_conn != NULL) {
        sasl_dispose(&state->sasl_conn);
    }
    state->auth_complete = 0;
    state->auth_started = 0;
    if (state->username != NULL) {
        free(state->username);
    }
}


