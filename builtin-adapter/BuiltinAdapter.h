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

#include "jni.h"

#ifndef _Included_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter
#define _Included_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter
#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jlong JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_init(JNIEnv *, jobject);

JNIEXPORT void JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_destroy(JNIEnv *, jobject, jlong);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_connect(JNIEnv *, jobject, jlong,
                                                                                               jstring, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_openNamespace(JNIEnv *, jobject,
                                                                                                     jlong, jlong,
                                                                                                     jlong, jstring,
                                                                                                     jboolean, jboolean,
                                                                                                     jboolean);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_closeNamespace(JNIEnv *, jobject,
                                                                                                      jlong, jlong,
                                                                                                      jlong, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_dropNamespace(JNIEnv *, jobject,
                                                                                                     jlong, jlong,
                                                                                                     jlong, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_addIndex(JNIEnv *, jobject,
                                                                                                jlong, jlong, jlong,
                                                                                                jstring, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_updateIndex(JNIEnv *, jobject,
                                                                                                   jlong, jlong, jlong,
                                                                                                   jstring, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_dropIndex(JNIEnv *, jobject,
                                                                                                 jlong, jlong, jlong,
                                                                                                 jstring, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_modifyItem(JNIEnv *, jobject,
                                                                                                  jlong, jlong, jlong,
                                                                                                  jbyteArray,
                                                                                                  jbyteArray);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_modifyItemTx(JNIEnv *, jobject,
                                                                                                    jlong, jlong,
                                                                                                    jbyteArray,
                                                                                                    jbyteArray);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_beginTx(JNIEnv *, jobject,
                                                                                               jlong, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_commitTx(JNIEnv *, jobject,
                                                                                                jlong, jlong, jlong,
                                                                                                jlong);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_rollbackTx(JNIEnv *, jobject,
                                                                                                  jlong, jlong);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_selectQuery(JNIEnv *, jobject,
                                                                                                   jlong, jlong, jlong,
                                                                                                   jbyteArray,
                                                                                                   jlongArray,
                                                                                                   jboolean asJson);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_deleteQuery(JNIEnv *, jobject,
                                                                                                   jlong, jlong, jlong,
                                                                                                   jbyteArray);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_deleteQueryTx(JNIEnv *, jobject,
                                                                                                     jlong, jlong,
                                                                                                     jbyteArray);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_updateQuery(JNIEnv *, jobject,
                                                                                                   jlong, jlong, jlong,
                                                                                                   jbyteArray);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_updateQueryTx(JNIEnv *, jobject,
                                                                                                     jlong, jlong,
                                                                                                     jbyteArray);

JNIEXPORT jbyteArray JNICALL Java_ru_rt_restream_reindexer_util_NativeUtils_getBytes(JNIEnv *, jclass, jlong, jlong,
                                                                                     jint);

JNIEXPORT void JNICALL Java_ru_rt_restream_reindexer_util_NativeUtils_freeNativeBuffer(JNIEnv *, jclass, jlong);

JNIEXPORT jlong JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_initServer(JNIEnv *, jobject);

JNIEXPORT void JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_destroyServer(JNIEnv *, jobject,
                                                                                                  jlong);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_startServer(JNIEnv *, jobject,
                                                                                                   jlong, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_stopServer(JNIEnv *, jobject,
                                                                                                  jlong);

JNIEXPORT jboolean JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_isServerReady(JNIEnv *, jobject,
                                                                                                      jlong);

JNIEXPORT jlong JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_getInstance(JNIEnv *, jobject,
                                                                                                 jlong, jstring,
                                                                                                 jstring, jstring);

JNIEXPORT void JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_putMeta(JNIEnv *, jobject, jlong,
                                                                                               jlong, jlong, jstring,
                                                                                               jstring, jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_getMeta(JNIEnv *, jobject, jlong,
                                                                                               jlong, jlong, jstring,
                                                                                               jstring);

JNIEXPORT jobject JNICALL Java_ru_rt_restream_reindexer_binding_builtin_BuiltinAdapter_select (JNIEnv *, jobject,
                                                                                               jlong, jlong, jlong,
                                                                                               jstring, jboolean,
                                                                                               jlongArray);

#ifdef __cplusplus
}
#endif
#endif
