/*
 * Copyright 2020 Restream
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "BuiltinAdapter.h"
#include "core/cbinding/reindexer_c.h"
#include "core/cbinding/reindexer_ctypes.h"
#include "server/cbinding/server_c.h"

reindexer_string rx_string(JNIEnv *env, jstring jstr) {
    return {
            .p = (void *) env->GetStringUTFChars(jstr, nullptr),
            .n = env->GetStringUTFLength(jstr)
    };
}

reindexer_buffer rx_buffer(JNIEnv *env, jbyteArray bytes) {
    return {
            .data = reinterpret_cast<uint8_t *>(env->GetByteArrayElements(bytes, nullptr)),
            .len = env->GetArrayLength(bytes)
    };
}

reindexer_ctx_info rx_ctx(jlong ctxId, jlong timeout) {
    return {
            .ctx_id = static_cast<uint64_t>(ctxId),
            .exec_timeout = timeout
    };
}

jobject jlong_object(JNIEnv *env, jlong jl) {
    jclass jlClass = env->FindClass("java/lang/Long");
    jmethodID valueOf = env->GetStaticMethodID(jlClass, "valueOf", "(J)Ljava/lang/Long;");
    return env->CallStaticObjectMethod(jlClass, valueOf, jl);
}

jobject j_res(JNIEnv *env, reindexer_error error) {
    jclass rsClass = env->FindClass("ru/rt/restream/reindexer/ReindexerResponse");
    jmethodID rsConstructor = env->GetMethodID(rsClass, "<init>", "(ILjava/lang/String;[Ljava/lang/Object;)V");
    jobjectArray args = env->NewObjectArray(0, env->FindClass("java/lang/Object"), nullptr);
    return env->NewObject(rsClass, rsConstructor, error.code, env->NewStringUTF(error.what), args);
}

jobject j_res(JNIEnv *env, reindexer_ret ret) {
    jclass rsClass = env->FindClass("ru/rt/restream/reindexer/ReindexerResponse");
    jmethodID rsConstructor = env->GetMethodID(rsClass, "<init>", "(ILjava/lang/String;[Ljava/lang/Object;)V");
    jstring errorMessage = nullptr;
    jobject resultsPtr = nullptr;
    jbyteArray body = nullptr;
    if (ret.err_code != 0) {
        errorMessage = env->NewStringUTF(reinterpret_cast<const char *>(ret.out.data));
    } else {
        body = env->NewByteArray(ret.out.len);
        env->SetByteArrayRegion(body, 0, ret.out.len, reinterpret_cast<const jbyte *>(ret.out.data));
        resultsPtr = jlong_object(env, ret.out.results_ptr);
    }
    jobjectArray args = env->NewObjectArray(2, env->FindClass("java/lang/Object"), nullptr);
    env->SetObjectArrayElement(args, 0, resultsPtr);
    env->SetObjectArrayElement(args, 1, body);
    return env->NewObject(rsClass, rsConstructor, ret.err_code, errorMessage, args);
}

jobject j_res(JNIEnv *env, reindexer_tx_ret ret) {
    jclass rsClass = env->FindClass("ru/rt/restream/reindexer/ReindexerResponse");
    jmethodID rsConstructor = env->GetMethodID(rsClass, "<init>", "(ILjava/lang/String;[Ljava/lang/Object;)V");
    jstring errorMessage = nullptr;
    jobject txId = nullptr;
    if (ret.err.code != 0) {
        errorMessage = env->NewStringUTF(ret.err.what);
    } else {
        txId = jlong_object(env, ret.tx_id);
    }
    jobjectArray args = env->NewObjectArray(1, env->FindClass("java/lang/Object"), nullptr);
    env->SetObjectArrayElement(args, 0, txId);
    return env->NewObject(rsClass, rsConstructor, ret.err.code, errorMessage, args);
}

JNIEXPORT jlong JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_init(JNIEnv *, jobject) {
    return init_reindexer();
}

JNIEXPORT void JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_destroy(JNIEnv *, jobject,
                                                                                            jlong rx) {
    destroy_reindexer(rx);
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_connect(JNIEnv *env, jobject,
                                                                                               jlong rx, jstring path,
                                                                                               jstring version) {
    reindexer_string dsn = rx_string(env, path);
    reindexer_string vers = rx_string(env, version);
    reindexer_error error = reindexer_connect(rx, dsn, ConnectOpts(), vers);
    env->ReleaseStringUTFChars(path, reinterpret_cast<const char *>(dsn.p));
    env->ReleaseStringUTFChars(version, reinterpret_cast<const char *>(vers.p));
    return j_res(env, error);
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_openNamespace(JNIEnv *env,
                                                                                                     jobject, jlong rx,
                                                                                                     jlong ctxId,
                                                                                                     jlong timeout,
                                                                                                     jstring namespaceName,
                                                                                                     jboolean enabled,
                                                                                                     jboolean dropOnFileFormatError,
                                                                                                     jboolean createIfMissing) {
    reindexer_string nsName = rx_string(env, namespaceName);
    StorageOpts opts = StorageOpts()
            .Enabled(enabled)
            .DropOnFileFormatError(dropOnFileFormatError)
            .CreateIfMissing(createIfMissing);
    jobject res = j_res(env, reindexer_open_namespace(rx, nsName, opts, rx_ctx(ctxId, timeout)));
    env->ReleaseStringUTFChars(namespaceName, reinterpret_cast<const char *>(nsName.p));
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_closeNamespace(JNIEnv *env,
                                                                                                      jobject,
                                                                                                      jlong rx,
                                                                                                      jlong ctxId,
                                                                                                      jlong timeout,
                                                                                                      jstring namespaceName) {
    reindexer_string nsName = rx_string(env, namespaceName);
    jobject res = j_res(env, reindexer_close_namespace(rx, nsName, rx_ctx(ctxId, timeout)));
    env->ReleaseStringUTFChars(namespaceName, reinterpret_cast<const char *>(nsName.p));
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_dropNamespace(JNIEnv *env,
                                                                                                     jobject,
                                                                                                     jlong rx,
                                                                                                     jlong ctxId,
                                                                                                     jlong timeout,
                                                                                                     jstring namespaceName) {
    reindexer_string nsName = rx_string(env, namespaceName);
    jobject res = j_res(env, reindexer_drop_namespace(rx, nsName, rx_ctx(ctxId, timeout)));
    env->ReleaseStringUTFChars(namespaceName, reinterpret_cast<const char *>(nsName.p));
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_addIndex(JNIEnv *env, jobject,
                                                                                                jlong rx, jlong ctxId,
                                                                                                jlong timeout,
                                                                                                jstring namespaceName,
                                                                                                jstring indexJson) {
    reindexer_string nsName = rx_string(env, namespaceName);
    reindexer_string indexDefJson = rx_string(env, indexJson);
    jobject res = j_res(env, reindexer_add_index(rx, nsName, indexDefJson, rx_ctx(ctxId, timeout)));
    env->ReleaseStringUTFChars(namespaceName, reinterpret_cast<const char *>(nsName.p));
    env->ReleaseStringUTFChars(indexJson, reinterpret_cast<const char *>(indexDefJson.p));
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_modifyItem(JNIEnv *env, jobject,
                                                                                                  jlong rx, jlong ctxId,
                                                                                                  jlong timeout,
                                                                                                  jbyteArray args,
                                                                                                  jbyteArray data) {
    reindexer_buffer bufferArgs = rx_buffer(env, args);
    reindexer_buffer bufferData = rx_buffer(env, data);
    jobject res = j_res(env, reindexer_modify_item_packed(rx, bufferArgs, bufferData, rx_ctx(ctxId, timeout)));
    env->ReleaseByteArrayElements(args, reinterpret_cast<jbyte *>(bufferArgs.data), 0);
    env->ReleaseByteArrayElements(data, reinterpret_cast<jbyte *>(bufferData.data), 0);
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_modifyItemTx(JNIEnv *env,
                                                                                                    jobject,
                                                                                                    jlong rx,
                                                                                                    jlong txId,
                                                                                                    jbyteArray args,
                                                                                                    jbyteArray data) {
    reindexer_buffer bufferArgs = rx_buffer(env, args);
    reindexer_buffer bufferData = rx_buffer(env, data);
    jobject res = j_res(env, reindexer_modify_item_packed_tx(rx, txId, bufferArgs, bufferData));
    env->ReleaseByteArrayElements(args, reinterpret_cast<jbyte *>(bufferArgs.data), 0);
    env->ReleaseByteArrayElements(data, reinterpret_cast<jbyte *>(bufferData.data), 0);
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_beginTx(JNIEnv *env, jobject,
                                                                                               jlong rx,
                                                                                               jstring namespaceName) {
    reindexer_string nsName = rx_string(env, namespaceName);
    jobject res = j_res(env, reindexer_start_transaction(rx, nsName));
    env->ReleaseStringUTFChars(namespaceName, reinterpret_cast<const char *>(nsName.p));
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_commitTx(JNIEnv *env, jobject,
                                                                                                jlong rx,
                                                                                                jlong txId,
                                                                                                jlong ctxId,
                                                                                                jlong timeout) {
    return j_res(env, reindexer_commit_transaction(rx, txId, rx_ctx(ctxId, timeout)));
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_rollbackTx(JNIEnv *env, jobject,
                                                                                                  jlong rx,
                                                                                                  jlong txId) {
    return j_res(env, reindexer_rollback_transaction(rx, txId));
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_selectQuery(JNIEnv *env, jobject,
                                                                                                   jlong rx,
                                                                                                   jlong ctxId,
                                                                                                   jlong timeout,
                                                                                                   jbyteArray data,
                                                                                                   jlongArray versions,
                                                                                                   jboolean asJson) {
    auto ptVersions = reinterpret_cast<int32_t *>(env->GetLongArrayElements(versions, nullptr));
    int ptVersionsCount = env->GetArrayLength(versions);
    reindexer_buffer bufferData = rx_buffer(env, data);
    jobject res = j_res(env, reindexer_select_query(rx, bufferData, asJson, ptVersions, ptVersionsCount,
                                                    rx_ctx(ctxId, timeout)));
    env->ReleaseLongArrayElements(versions, reinterpret_cast<jlong *>(ptVersions), 0);
    env->ReleaseByteArrayElements(data, reinterpret_cast<jbyte *>(bufferData.data), 0);
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_deleteQuery(JNIEnv *env, jobject,
                                                                                                   jlong rx,
                                                                                                   jlong ctxId,
                                                                                                   jlong timeout,
                                                                                                   jbyteArray data) {
    reindexer_buffer bufferData = rx_buffer(env, data);
    jobject res = j_res(env, reindexer_delete_query(rx, bufferData, rx_ctx(ctxId, timeout)));
    env->ReleaseByteArrayElements(data, reinterpret_cast<jbyte *>(bufferData.data), 0);
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_deleteQueryTx(JNIEnv *env,
                                                                                                     jobject,
                                                                                                     jlong rx,
                                                                                                     jlong txId,
                                                                                                     jbyteArray data) {
    reindexer_buffer bufferData = rx_buffer(env, data);
    jobject res = j_res(env, reindexer_delete_query_tx(rx, txId, bufferData));
    env->ReleaseByteArrayElements(data, reinterpret_cast<jbyte *>(bufferData.data), 0);
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_updateQuery(JNIEnv *env, jobject,
                                                                                                   jlong rx,
                                                                                                   jlong ctxId,
                                                                                                   jlong timeout,
                                                                                                   jbyteArray data) {
    reindexer_buffer bufferData = rx_buffer(env, data);
    jobject res = j_res(env, reindexer_update_query(rx, bufferData, rx_ctx(ctxId, timeout)));
    env->ReleaseByteArrayElements(data, reinterpret_cast<jbyte *>(bufferData.data), 0);
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_updateQueryTx(JNIEnv *env,
                                                                                                     jobject,
                                                                                                     jlong rx,
                                                                                                     jlong txId,
                                                                                                     jbyteArray data) {
    reindexer_buffer bufferData = rx_buffer(env, data);
    jobject res = j_res(env, reindexer_update_query_tx(rx, txId, bufferData));
    env->ReleaseByteArrayElements(data, reinterpret_cast<jbyte *>(bufferData.data), 0);
    return res;
}

JNIEXPORT jbyteArray JNICALL Java_ru_rt_restream_reindexer_util_NativeUtils_getBytes(JNIEnv *env, jclass,
                                                                                     jlong resultsPtr,
                                                                                     jlong cPtr,
                                                                                     jint nsId) {
    reindexer_buffer buffer = reindexer_cptr2cjson(resultsPtr, cPtr, nsId);
    jbyteArray result = env->NewByteArray(buffer.len);
    env->SetByteArrayRegion(result, 0, buffer.len, reinterpret_cast<const jbyte *>(buffer.data));
    reindexer_free_cjson(buffer);
    return result;
}

JNIEXPORT void JNICALL Java_ru_rt_restream_reindexer_util_NativeUtils_freeNativeBuffer(JNIEnv *, jclass,
                                                                                       jlong resultsPtr) {
    reindexer_free_buffer({.results_ptr = static_cast<uintptr_t>(resultsPtr)});
}

JNIEXPORT jlong JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_initServer(JNIEnv *, jobject) {
    return init_reindexer_server();
}

JNIEXPORT void JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_destroyServer(JNIEnv *, jobject,
                                                                                                  jlong svc) {
    destroy_reindexer_server(svc);
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_startServer(JNIEnv *env, jobject,
                                                                                                   jlong svc,
                                                                                                   jstring yamlConfig) {
    reindexer_string config = rx_string(env, yamlConfig);
    jobject res = j_res(env, start_reindexer_server(svc, config));
    env->ReleaseStringUTFChars(yamlConfig, reinterpret_cast<const char *>(config.p));
    return res;
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_stopServer(JNIEnv *env, jobject,
                                                                                                  jlong svc) {
    return j_res(env, stop_reindexer_server(svc));
}

JNIEXPORT jboolean JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_isServerReady(JNIEnv *, jobject,
                                                                                                      jlong svc) {
    return check_server_ready(svc);
}

JNIEXPORT jlong JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_getInstance(JNIEnv *env, jobject,
                                                                                                 jlong svc,
                                                                                                 jstring database,
                                                                                                 jstring user,
                                                                                                 jstring password) {
    uintptr_t rx = 0;
    reindexer_string dbName = rx_string(env, database);
    reindexer_string dbUser = rx_string(env, user);
    reindexer_string dbPass = rx_string(env, password);
    get_reindexer_instance(svc, dbName, dbUser, dbPass, &rx);
    env->ReleaseStringUTFChars(database, reinterpret_cast<const char *>(dbName.p));
    env->ReleaseStringUTFChars(user, reinterpret_cast<const char *>(dbUser.p));
    env->ReleaseStringUTFChars(password, reinterpret_cast<const char *>(dbPass.p));
    return rx;
}

JNIEXPORT void JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_putMeta(JNIEnv *env, jobject,
                                                                                            jlong rx,
                                                                                            jlong ctxId,
                                                                                            jlong timeout,
                                                                                            jstring namespaceName,
                                                                                            jstring key,
                                                                                            jstring data) {
    reindexer_string nsName = rx_string(env, namespaceName);
    reindexer_string metaKey = rx_string(env, key);
    reindexer_string metaData = rx_string(env, data);
    reindexer_put_meta(rx, nsName, metaKey, metaData, rx_ctx(ctxId, timeout));
    env->ReleaseStringUTFChars(namespaceName, reinterpret_cast<const char *>(nsName.p));
    env->ReleaseStringUTFChars(key, reinterpret_cast<const char *>(metaKey.p));
    env->ReleaseStringUTFChars(data, reinterpret_cast<const char *>(metaData.p));
}

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_getMeta(JNIEnv *env, jobject,
                                                                                               jlong rx,
                                                                                               jlong ctxId,
                                                                                               jlong timeout,
                                                                                               jstring namespaceName,
                                                                                               jstring key) {
    reindexer_string nsName = rx_string(env, namespaceName);
    reindexer_string metaKey = rx_string(env, key);
    jobject res = j_res(env, reindexer_get_meta(rx, nsName, metaKey, rx_ctx(ctxId, timeout)));
    env->ReleaseStringUTFChars(namespaceName, reinterpret_cast<const char *>(nsName.p));
    env->ReleaseStringUTFChars(key, reinterpret_cast<const char *>(metaKey.p));
    return res;
}
