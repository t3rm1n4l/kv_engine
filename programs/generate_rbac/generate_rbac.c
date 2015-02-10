/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/* This is a small program that reads the input rbac file and
 * generates a c++ file that contains the file. We can't just
 * embed everything in a single constant since the language doesn't
 * guarantee that it may be that big..
 */

#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    int cmd;
    const char *input = NULL;
    const char *output = NULL;

    while ((cmd = getopt(argc, argv, "i:o:")) != -1) {
        switch (cmd) {
        case 'i' :
            input = optarg;
            break;
        case 'o':
            output = optarg;
            break;
        default:
            fprintf(stderr, "Usage: %s -i input -o output\n", argv[0]);
            exit(1);
        }
    }

    if (! (input && output)) {
        fprintf(stderr, "Usage: %s -i input -o output\n", argv[0]);
        exit(1);
    }

    FILE *inf = fopen(input, "r");
    FILE *of = fopen(output, "w");

    fprintf(of, "// This file is generated by:\n");
    fprintf(of, "// %s -i %s -o %s\n", argv[0], input, output);
    fprintf(of, "#include \"config.h\"\n");
    fprintf(of, "#include <sstream>\n");
    fprintf(of, "#include <string>\n");
    fprintf(of, "#include <cJSON.h>\n");
    fprintf(of, "#include <daemon/rbac_impl.h>\n");
    fprintf(of, "\n");
    fprintf(of, "cJSON *getDefaultRbacConfig(void) {\n");
    fprintf(of, "   std::stringstream ss;\n");

    while (!ferror(inf) && !feof(inf)) {
        char buffer[200];
        if (fgets(buffer, sizeof(buffer), inf)) {
            char *c = buffer;
            fprintf(of, "   ss << \"");
            while (*c) {
                if (*c == '"') {
                    fprintf(of, "\\\"");
                } else if (*c != '\r' && *c != '\n') {
                    fprintf(of, "%c", *c);
                }
                ++c;
            }
            fprintf(of, "\";\n");
        }
    }
    fprintf(of, "   return cJSON_Parse(ss.str().c_str());\n");
    fprintf(of, "}\n");
    fclose(of);

    return 0;
}
