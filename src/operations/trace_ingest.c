#include "operations/trace_ingest.h"
#include "yyjson/yyjson.h"
#include <string.h>

cbm_operation_result_t cbm_trace_ingest_operation_execute(const char *args_json) {
    const char *json = args_json ? args_json : "{}";
    yyjson_doc *input = yyjson_read(json, strlen(json), 0);
    int trace_count = 0;
    if (input) {
        yyjson_val *root = yyjson_doc_get_root(input);
        yyjson_val *traces = yyjson_is_obj(root) ? yyjson_obj_get(root, "traces") : NULL;
        if (yyjson_is_arr(traces)) trace_count = (int)yyjson_arr_size(traces);
        yyjson_doc_free(input);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        return cbm_operation_result_copy("result allocation failed", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "accepted");
    yyjson_mut_obj_add_int(doc, root, "traces_received", trace_count);
    yyjson_mut_obj_add_str(doc, root, "note", "Runtime edge creation from traces not yet implemented");
    char *payload = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return payload ? cbm_operation_result_take(payload, false)
                   : cbm_operation_result_copy("result encoding failed", true);
}
