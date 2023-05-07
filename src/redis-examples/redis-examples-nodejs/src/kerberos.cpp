#include "kerberos.h"
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <fstream>
#include <random>
#include <sstream>

#include <algorithm>
#include <stdexcept>

static const char basis_64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const signed char index_64[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62,
    -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,
    1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
    23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
    39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1};
#define CHAR64(c) (((c) < 0 || (c) > 127) ? -1 : index_64[(c)])

char* base64_encode(const unsigned char* value, size_t vlen) {
    char* result = (char*)malloc((vlen * 4) / 3 + 5);
    if (result == NULL) {
        return NULL;
    }
    char* out = result;
    while (vlen >= 3) {
        *out++ = basis_64[value[0] >> 2];
        *out++ = basis_64[((value[0] << 4) & 0x30) | (value[1] >> 4)];
        *out++ = basis_64[((value[1] << 2) & 0x3C) | (value[2] >> 6)];
        *out++ = basis_64[value[2] & 0x3F];
        value += 3;
        vlen -= 3;
    }
    if (vlen > 0) {
        *out++ = basis_64[value[0] >> 2];
        unsigned char oval = (value[0] << 4) & 0x30;
        if (vlen > 1)
            oval |= value[1] >> 4;
        *out++ = basis_64[oval];
        *out++ = (vlen < 2) ? '=' : basis_64[(value[1] << 2) & 0x3C];
        *out++ = '=';
    }
    *out = '\0';

    return result;
}

unsigned char* base64_decode(const char* value, size_t* rlen) {
    *rlen = 0;
    int c1, c2, c3, c4;

    size_t vlen = strlen(value);
    unsigned char* result = (unsigned char*)malloc((vlen * 3) / 4 + 1);
    if (result == NULL) {
        return NULL;
    }
    unsigned char* out = result;

    while (1) {
        if (value[0] == 0) {
            return result;
        }
        c1 = value[0];
        if (CHAR64(c1) == -1) {
            goto base64_decode_error;
            ;
        }
        c2 = value[1];
        if (CHAR64(c2) == -1) {
            goto base64_decode_error;
            ;
        }
        c3 = value[2];
        if ((c3 != '=') && (CHAR64(c3) == -1)) {
            goto base64_decode_error;
            ;
        }
        c4 = value[3];
        if ((c4 != '=') && (CHAR64(c4) == -1)) {
            goto base64_decode_error;
            ;
        }

        value += 4;
        *out++ = (CHAR64(c1) << 2) | (CHAR64(c2) >> 4);
        *rlen += 1;

        if (c3 != '=') {
            *out++ = ((CHAR64(c2) << 4) & 0xf0) | (CHAR64(c3) >> 2);
            *rlen += 1;

            if (c4 != '=') {
                *out++ = ((CHAR64(c3) << 6) & 0xc0) | CHAR64(c4);
                *rlen += 1;
            }
        }
    }

base64_decode_error:
    *result = 0;
    *rlen = 0;

    return result;
}

client_state getClientState(std::string id) {
    std::map<std::string, client_state>::iterator it; 
    for(it = clientStateMap.begin(); it != clientStateMap.end(); it++)  {
      if (id.compare((*it).first) == 0) {
        return (*it).second;
      }
    }
    struct client_state state = {
      .sasl_conn = NULL,
      .auth_started = 0,
      .auth_complete = nullptr,
      .username = nullptr
    };
    return state;
}

void initializeClient(const Napi::CallbackInfo& info) {
  int result = sasl_client_init(NULL);
  if (result != SASL_OK) {
    throw Napi::Error::New(info.Env(), "init client failed");
  }
}

Napi::String kerberosStart(const Napi::CallbackInfo& info) {
  std::string full_server_realm = info[0].ToString();
  std::string clientId = info[1].ToString();
  Napi::Env env = info.Env();
  client_state state = getClientState(clientId);
  gss_result result = {
    .code = 0
  };
  if(state.auth_started) {
    return Napi::String::New(env, "auth already started");
  }
  sasl_conn_t *sasl_conn = NULL;
  int res = sasl_client_new(APP_NAME, full_server_realm.c_str(), NULL, NULL, NULL, 0, &sasl_conn);
  if (res != SASL_OK) {
    return Napi::String::New(env, sasl_errdetail(state.sasl_conn));
  }
  state.sasl_conn = sasl_conn;
  state.auth_started = 1;
  state.auth_complete = 0;
  char *mechUsing;
  res = sasl_client_start(state.sasl_conn, AUTH_MECHANISM, NULL, (const char**)&result.response, &result.len, (const char**)&mechUsing);
  if (res == SASL_OK) {
    state.auth_complete = 1;
    sasl_dispose(&state.sasl_conn);
    return Napi::String::New(env, "ok");
  } else if (res != SASL_CONTINUE) {
    clientStateMap.erase(clientId);
    std::string err = sasl_errdetail(state.sasl_conn);
    sasl_dispose(&state.sasl_conn);
    return Napi::String::New(env, err);
  }
  clientStateMap[clientId] = state;
  return Napi::String::New(env, base64_encode((const unsigned char*)result.response, (size_t)result.len));
}

Napi::String kerberosStep(const Napi::CallbackInfo& info) {
  std::string challenge = info[0].ToString();
  std::string clientId = info[1].ToString();
  std::string challengeLen = info[2].ToString();
  client_state state = getClientState(clientId);
  Napi::Env env = info.Env();
  size_t len;
  const char* ticketBase64 = challenge.c_str();
  const char* serverTicket = (const char*)base64_decode(ticketBase64, &len);

  if(!state.auth_started) {
    return Napi::String::New(env, "not started");
  }
  gss_result result = {
    .code = 0
  };
  int res = sasl_client_step(state.sasl_conn, serverTicket , len,  NULL, (const char**)&result.response, &result.len);
  if (res == SASL_OK) {
    state.auth_complete = 1;
    return Napi::String::New(env, base64_encode((const unsigned char*)result.response, (size_t)result.len));
  } else if (res != SASL_CONTINUE) {
    clientStateMap.erase(clientId);
    std::string err = sasl_errdetail(state.sasl_conn);
    sasl_dispose(&state.sasl_conn);
    return Napi::String::New(env, err);
  }
  return Napi::String::New(env, base64_encode((const unsigned char*)result.response, (size_t)result.len));
}

Napi::String genClientId(const Napi::CallbackInfo& info) {
  std::string ret;
  ret.resize(sz);
  Napi::Env env = info.Env();
  std::mt19937 rng(std::random_device{}());
  for (int i = 0; i < sz; ++i) {
    uint32_t x = rng() % (sizeof(CCH) - 1);
    ret[i] = CCH[x];
  }

  return Napi::String::New(env, ret);
}


static Napi::Object Init(Napi::Env env, Napi::Object exports) {
  exports["initializeClient"] = Napi::Function::New(env, initializeClient);
  exports["kerberosStart"] = Napi::Function::New(env, kerberosStart);
  exports["kerberosStep"] = Napi::Function::New(env, kerberosStep);
  exports["genClientId"] = Napi::Function::New(env, genClientId);
  return exports;
}

NODE_API_MODULE(kerberos, Init)

