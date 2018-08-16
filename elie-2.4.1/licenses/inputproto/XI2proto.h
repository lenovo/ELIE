/*
 * Copyright © 2009 Red Hat, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next
 * paragraph) shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 */

/* Conventions for this file:
 * Names:
 * structs: always typedef'd, prefixed with xXI, CamelCase
 * struct members: lower_case_with_underscores
 *        Exceptions: reqType, ReqType, repType, RepType, sequenceNumber are
 *        named as such for historical reasons.
 * request opcodes: X_XIRequestName as CamelCase
 * defines: defines used in client applications must go in XI2.h
 *          defines used only in protocol handling: XISOMENAME
 *
 * Data types: unless there is a historical name for a datatype (e.g.
 * Window), use stdint types specifying the size of the datatype.
 * historical data type names must be defined and undefined at the top and
 * end of the file.
 *
 * General:
 * spaces, not tabs.
 * structs specific to a request or reply added before the request
 *      definition. structs used in more than one request, reply or event
 *      appended to the common structs section before the definition of the
 *      first request.
 * members of structs vertically aligned on column 16 if datatypes permit.
 *      otherwise alingned on next available 8n column.
 */
